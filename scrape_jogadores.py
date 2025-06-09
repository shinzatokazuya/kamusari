import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time
import re

# Função para extrair dados de uma página de jogadores
def scrape_pagina(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        jogadores = []
        # Encontra a tabela ou lista de jogadores (ajuste o seletor conforme o HTML do Ogol)
        tabela = soup.find('table', class_='zztable') or soup.find('div', class_='players_list')
        if not tabela:
            return jogadores

        for linha in tabela.find_all('tr')[1:]:  # Pula o cabeçalho
            colunas = linha.find_all('td')
            if len(colunas) >= 2:
                link_jogador = colunas[1].find('a', href=True)
                if not link_jogador:
                    continue
                jogador_url = 'https://www.ogol.com.br' + link_jogador['href']
                jogador_id = re.search(r'id=(\d+)', jogador_url).group(1) if re.search(r'id=(\d+)', jogador_url) else 'N/A'

                # Acessa a página individual do jogador
                try:
                    jogador_response = requests.get(jogador_url, headers=headers, timeout=10)
                    jogador_soup = BeautifulSoup(jogador_response.text, 'html.parser')

                    # Extrai dados (ajuste os seletores conforme o HTML real do Ogol)
                    nome_completo = jogador_soup.find('h1', class_='player_name') or jogador_soup.find('div', class_='name')
                    apelido = jogador_soup.find('span', class_='nickname') or nome_completo
                    data_nascimento = jogador_soup.find('span', class_='birth_date') or jogador_soup.find('div', class_='info_birth')
                    data_falecimento = jogador_soup.find('span', class_='death_date') or jogador_soup.find('div', class_='info_death')

                    jogadores.append({
                        'ID_Jogador': jogador_id,
                        'Nome Completo': nome_completo.text.strip() if nome_completo else 'N/A',
                        'Apelido': apelido.text.strip() if apelido else nome_completo.text.strip() if nome_completo else 'N/A',
                        'Data de Nascimento': data_nascimento.text.strip() if data_nascimento else 'N/A',
                        'Data de Falecimento': data_falecimento.text.strip() if data_falecimento else 'N/A'
                    })
                except Exception as e:
                    print(f"Erro ao acessar jogador {jogador_url}: {e}")
                    continue

        return jogadores
    except Exception as e:
        print(f"Erro ao acessar página {url}: {e}")
        return []

# Função principal
def main():
    # URL base para a lista de jogadores de um clube (exemplo: Flamengo)
    base_url = "https://www.ogol.com.br/club/players.php?id=614&page="  # ID 614 é do Flamengo
    max_paginas = 100  # Ajuste conforme necessário (ex.: 100 páginas)
    urls = [f"{base_url}{i}" for i in range(1, max_paginas + 1)]

    jogadores = []
    # Scraping paralelo com ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:  # Ajuste max_workers para evitar bloqueios
        resultados = executor.map(scrape_pagina, urls)
        for resultado in resultados:
            jogadores.extend(resultado)
            time.sleep(1)  # Delay para evitar bloqueios

    # Salva em CSV
    df = pd.DataFrame(jogadores)
    df.to_csv('jogadores_flamengo.csv', index=False, encoding='utf-8')
    print(f"Dados salvos em 'jogadores_flamengo.csv' com {len(df)} jogadores")
    print(df.head())

if __name__ == "__main__":
    main()
