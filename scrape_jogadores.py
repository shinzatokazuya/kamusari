import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time
import re

# Função para extrair dados da página de biografia de um jogador
def get_bio_data(jogador_url, jogador_id, nome):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(jogador_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontra a seção de biografia
        bio_div = soup.find('div', id='entity_bio')
        if not bio_div:
            print(f"Seção de biografia não encontrada em {jogador_url}")
            return None

        # Nome completo
        nome_completo_elem = bio_div.find('div', class_='bio', string=re.compile(r'Nome'))
        nome_completo = nome_completo_elem.text.replace('Nome', '').strip() if nome_completo_elem else 'N/A'

        # Data de nascimento
        nascimento_elem = bio_div.find('div', class_='bio_half', string=re.compile(r'Data de Nascimento'))
        data_nascimento = nascimento_elem.find('span').next_sibling.strip() if nascimento_elem else 'N/A'

        # Data de falecimento
        situacao_elem = bio_div.find('div', class_='bio', string=re.compile(r'Situação'))
        data_falecimento = 'N/A'
        if situacao_elem and 'Falecido' in situacao_elem.text:
            match = re.search(r'Falecido - (\d{4}-\d{2}-\d{2})', situacao_elem.text)
            data_falecimento = match.group(1) if match else 'N/A'

        # Nacionalidade
        nacionalidade_elem = bio_div.find('div', class_='bio_half', string=re.compile(r'Nacionalidade'))
        nacionalidade = nacionalidade_elem.find('div', class_='text').text.strip() if nacionalidade_elem else 'N/A'

        return {
            'ID_Jogador': jogador_id,
            'Nome Completo': nome_completo,
            'Data de Nascimento': data_nascimento,
            'Data de Falecimento': data_falecimento,
            'Nacionalidade': nacionalidade
        }
    except Exception as e:
        print(f"Erro ao acessar {jogador_url}: {e}")
        return None

# Função para extrair dados de uma página de jogadores
def scrape_pagina(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        jogadores = []
        # Encontra a tabela de jogadores
        tabela = soup.find('table', class_='zztable stats zz-table')
        if not tabela:
            print(f"Tabela não encontrada em {url}")
            return jogadores

        for linha in tabela.find_all('tr')[1:]:  # Pula o cabeçalho
            colunas = linha.find_all('td')
            if len(colunas) < 10:
                continue  # Ignora linhas inválidas (ex.: total ou anúncios)

            # Extrai o nome normalmente utilizado
            nome_elem = colunas[2].find('a', href=re.compile(r'/jogador/'))
            nome = nome_elem.text.strip() if nome_elem else 'N/A'

            # Extrai o ID do jogador do link de detalhes
            link_detalhes = colunas[9].find('a', href=re.compile(r'xray\.php'))
            jogador_id = 'N/A'
            if link_detalhes:
                match = re.search(r'jogador_id=(\d+)', link_detalhes['href'])
                jogador_id = match.group(1) if match else 'N/A'

            # Monta a URL da página de biografia
            jogador_url = f"https://www.ogol.com.br/jogador/{nome.lower().replace(' ', '-')}/{jogador_id}"

            # Extrai dados da biografia
            bio_data = get_bio_data(jogador_url, jogador_id, nome)
            if bio_data:
                jogadores.append(bio_data)

        return jogadores
    except Exception as e:
        print(f"Erro ao acessar página {url}: {e}")
        return []

# Função principal
def main():
    # URL base para a lista de jogadores do Santos
    base_url = "https://www.ogol.com.br/equipe/santos/jogadores?pais=0&epoca_stats_id=0&pos=0&o=&active=99&page="
    max_paginas = 21  # Ajustado para 1003 jogadores (~50 por página)
    urls = [f"{base_url}{i}" for i in range(1, max_paginas + 1)]

    jogadores = []
    # Scraping paralelo com ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as executor:  # Reduzido para evitar bloqueios
        resultados = executor.map(scrape_pagina, urls)
        for resultado in resultados:
            jogadores.extend(resultado)
            time.sleep(2)  # Delay para evitar bloqueios

    # Salva em CSV
    df = pd.DataFrame(jogadores)
    df.to_csv('jogadores_santos_bio.csv', index=False, encoding='utf-8')
    print(f"Dados salvos em 'jogadores_santos_bio.csv' com {len(df)} jogadores")
    print(df.head())

if __name__ == "__main__":
    main()
