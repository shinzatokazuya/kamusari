import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import time
import re
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configura sessão com retries
def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    return session

# Normaliza nome para URL
def normalize_name(nome):
    nome = unicodedata.normalize('NFKD', str(nome)).encode('utf-8', 'ignore').decode('utf-8')
    nome = re.sub(r'\s+', '-', nome.lower()).strip('-')
    return nome

# Extrai dados do clube
def get_clube_data(clube_id, nome, cidade, estado, regiao):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    session = create_session()
    normalized_nome = normalize_name(nome)
    clube_url = f"https://www.ogol.com.br/equipe/{normalized_nome}/"
    try:
        response = session.get(clube_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extrai ID_Ogol
        jogadores_link = soup.find('a', href=re.compile(r'/jogadores'))
        id_ogol = 'N/A'
        if jogadores_link:
            match = re.search(r'equipe/[^/]+/(\d+)/jogadores', jogadores_link['href'])
            id_ogol = match.group(1) if match else 'N/A'

        # Nome completo
        nome_completo_elem = soup.find('h1')
        nome_completo = nome_completo_elem.text.strip() if nome_completo_elem else nome

        # Ano de fundação
        fundacao = 'N/A'
        fundacao_elem = soup.find('span', string=re.compile(r'Fundação', re.I))
        if fundacao_elem:
            fundacao_text = fundacao_elem.find_parent('div').text
            match = re.search(r'\d{4}', fundacao_text)
            fundacao = match.group(0) if match else 'N/A'

        # Cores
        cores = 'N/A'
        cores_elem = soup.find('span', string=re.compile(r'Cores', re.I))
        if cores_elem:
            cores_text = cores_elem.find_parent('div').text
            cores = cores_text.replace('Cores:', '').strip() if cores_text else 'N/A'

        return {
            'ID': clube_id,
            'Nome': nome,
            'Nome_Completo': nome_completo,
            'Cidade': cidade,
            'Estado': estado,
            'Região': regiao,
            'Ano_Fundação': fundacao,
            'Cores': cores,
            'ID_Ogol': id_ogol
        }
    except Exception as e:
        print(f"Erro ao acessar {clube_url}: {e}")
        with open('failed_urls.txt', 'a', encoding='utf-8') as f:
            f.write(f"Erro no clube: {clube_url} - {str(e)}\n")
        return {
            'ID': clube_id,
            'Nome': nome,
            'Nome_Completo': nome,
            'Cidade': cidade,
            'Estado': estado,
            'Região': regiao,
            'Ano_Fundação': 'N/A',
            'Cores': 'N/A',
            'ID_Ogol': 'N/A'
        }

# Função principal
def main():
    # Tenta conectar ao SQLite em diferentes caminhos
    db_paths = ['brasileirao_desde_1971.db', 'banco_de_dados/teste.db']
    conn = None
    for path in db_paths:
        try:
            conn = sqlite3.connect(path)
            clubes_df = pd.read_sql_query("SELECT ID, Nome, Cidade, Estado, Regiao FROM clubes", conn)
            print(f"Conectado com sucesso a {path}")
            break
        except sqlite3.OperationalError:
            print(f"Não encontrou {path}. Tentando próximo...")
        except Exception as e:
            print(f"Erro ao conectar a {path}: {e}")
    if conn is None:
        print("Nenhum arquivo de banco de dados encontrado. Verifique o caminho ou o nome do arquivo.")
        return
    conn.close()

    print(f"Lidos {len(clubes_df)} clubes do banco de dados")
    print(clubes_df.head())

    clubes_completos = []
    for i, row in enumerate(clubes_df.iterrows(), 1):
        clube_data = get_clube_data(row[1]['ID'], row[1]['Nome'], row[1]['Cidade'], row[1]['Estado'], row[1]['Regiao'])
        clubes_completos.append(clube_data)
        print(f"Processado clube {i}/{len(clubes_df)}: {row[1]['Nome']}")
        time.sleep(5)

    # Salva em CSV
    df = pd.DataFrame(clubes_completos)
    df.to_csv('clubes_completos.csv', index=False, encoding='utf-8')
    print(f"Dados salvos em 'clubes_completos.csv' com {len(df)} clubes")
    print(df.head())

if __name__ == "__main__":
    main()
