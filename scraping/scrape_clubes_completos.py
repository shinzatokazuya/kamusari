import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def get_club_infobox(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        infobox = soup.find('table', class_='infobox vcard vevent')
        if not infobox:
            print(f"Infobox não encontrada em {url}")
            return None

        print(f"Infobox encontrada em {url}. Analisando linhas...")
        data = {}
        for row in infobox.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:  # Verifica se há pelo menos dois <td>
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                print(f"Rótulo: {label}, Valor: {value}")  # Depuração
                if label == 'Nome':
                    data['Nome'] = value
                elif label == 'Alcunhas':
                    alcunhas = value.split()  # Divide por espaços
                    data['Primeira_Alcunha'] = alcunhas[0].strip() if alcunhas else 'N/A'
                elif label == 'Mascote':
                    data['Mascote'] = value
                elif label == 'Fundação':
                    fundacao = value.split(';', ',')[0].strip()
                    data['Fundação'] = fundacao
                elif label == 'Estádio':
                    estadios = value.split('<br>')
                    data['Estádio'] = estadios[0].strip() if estadios else 'N/A'
                elif label == 'Capacidade':
                    capacidades = value.split('<br>')
                    capacidade_texto = capacidades[0].strip()
                    # Extrai apenas os números, mantendo pontos
                    data['Capacidade'] = re.sub(r'[^\d.]', '', capacidade_texto) if capacidade_texto else 'N/A'
                elif label == 'Localização':
                    data['Localização'] = value

        if not data:
            print(f"Nenhum campo extraído de {url}")
        return data
    except Exception as e:
        print(f"Erro ao processar {url}: {e}")
        return None

def get_club_links():
    url = 'https://pt.wikipedia.org/wiki/Participações_dos_clubes_no_Campeonato_Brasileiro_de_Futebol'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        tabela = soup.find('table', class_='wikitable')
        if not tabela:
            print("Tabela não encontrada")
            return []

        links = []
        for row in tabela.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) > 1:
                link_tag = cells[1].find('a', href=True)
                if link_tag and not link_tag['href'].startswith('#'):
                    full_url = 'https://pt.wikipedia.org' + link_tag['href']
                    links.append(full_url)

        return list(dict.fromkeys(links))
    except Exception as e:
        print(f"Erro ao extrair links: {e}")
        return []

def main():
    club_urls = get_club_links()
    print(f"Encontrados {len(club_urls)} links de clubes")

    club_data_list = []
    stadium_data_list = []

    # Limitar para teste (remova ou ajuste conforme necessário)
    club_urls = club_urls[:5]  # Processa apenas os 5 primeiros

    for url in club_urls:
        print(f"Processando: {url}")
        data = get_club_infobox(url)
        if data:
            club_data = {
                'Nome': data.get('Nome', 'N/A'),
                'Primeira_Alcunha': data.get('Primeira_Alcunha', 'N/A'),
                'Mascote': data.get('Mascote', 'N/A'),
                'Fundação': data.get('Fundação', 'N/A')
            }
            stadium_data = {
                'Estádio': data.get('Estádio', 'N/A'),
                'Capacidade': data.get('Capacidade', 'N/A'),
                'Localização': data.get('Localização', 'N/A')
            }
            club_data_list.append(club_data)
            stadium_data_list.append(stadium_data)
            print(f"Dados coletados para {url}: {stadium_data}")  # Foco nos dados do estádio
        time.sleep(2)

    if club_data_list:
        clubes_df = pd.DataFrame(club_data_list)
        clubes_df.to_csv('clubes_infobox_all.csv', index=False, encoding='utf-8')
        print("Dados dos clubes salvos em 'clubes_infobox_all.csv'")
    else:
        print("Nenhum dado de clube foi coletado para salvar.")

    if stadium_data_list:
        estadios_df = pd.DataFrame(stadium_data_list)
        estadios_df.to_csv('estadios_infobox_all.csv', index=False, encoding='utf-8')
        print("Dados dos estádios salvos em 'estadios_infobox_all.csv'")
    else:
        print("Nenhum dado de estádio foi coletado para salvar.")

if __name__ == "__main__":
    main()
