import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configura sessão com retries
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    return session

# Normaliza nome para URL
def normalize_name(nome):
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')
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
    # Tabela de clubes fornecida (adapte com os 557 clubes)
    clubes_data = [
        {'ID': 1, 'Nome': 'Grêmio', 'Cidade': 'Porto Alegre', 'Estado': 'RS', 'Região': 'Sul'},
        {'ID': 2, 'Nome': 'Santos', 'Cidade': 'Santos', 'Estado': 'SP', 'Região': 'Sudeste'},
        {'ID': 3, 'Nome': 'Atlético Mineiro', 'Cidade': 'Belo Horizonte', 'Estado': 'MG', 'Região': 'Sudeste'},
        # ... Adicione os outros clubes (ou leia de um CSV)
        
        {'ID': 557, 'Nome': 'Tupynambás', 'Cidade': 'Juiz de Fora', 'Estado': 'MG', 'Região': 'Sudeste'}
    ]
    clubes_df = pd.DataFrame(clubes_data)

    clubes_completos = []
    for i, row in enumerate(clubes_df.iterrows(), 1):
        clube_data = get_clube_data(row[1]['ID'], row[1]['Nome'], row[1]['Cidade'], row[1]['Estado'], row[1]['Região'])
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
