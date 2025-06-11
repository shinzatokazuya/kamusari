import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

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

        data = {}
        for row in infobox.find_all('tr'):
            label = row.find('th')
            value = row.find('td')
            if label and value:
                label_text = label.get_text(strip=True)
                value_text = value.get_text(strip=True)
                if label_text == 'Nome':
                    data['Nome'] = value_text
                elif label_text == 'Alcunhas':
                    alcunhas = value_text.split('<br>')
                    data['Primeira_Alcunha'] = alcunhas[0].strip() if alcunhas else 'N/A'
                elif label_text == 'Mascote':
                    data['Mascote'] = value_text
                elif label_text == 'Fundação':
                    fundacao = value_text.split(';')[0].strip()
                    data['Fundação'] = fundacao
                elif label_text == 'Estádio':
                    data['Estádio'] = value_text
                elif label_text == 'Capacidade':
                    capacidades = value_text.split('<br>')
                    data['Capacidade'] = capacidades[0].split('(')[0].strip() if capacidades else 'N/A'
                elif label_text == 'Localização':
                    data['Localização'] = value_text

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
            if len(cells) > 1:  # Verifica se há pelo menos 2 células
                link_tag = cells[1].find('a', href=True)
                if link_tag and not link_tag['href'].startswith('#'):  # Ignora links internos
                    full_url = 'https://pt.wikipedia.org' + link_tag['href']
                    links.append(full_url)

        return list(dict.fromkeys(links))  # Remove duplicatas
    except Exception as e:
        print(f"Erro ao extrair links: {e}")
        return []

def main():
    # Obter todos os links da segunda coluna
    club_urls = get_club_links()
    print(f"Encontrados {len(club_urls)} links de clubes")

    club_data_list = []
    stadium_data_list = []

    # Limitar para teste (remova ou ajuste conforme necessário)
    club_urls = club_urls[:5]  # Processa apenas os 5 primeiros para depuração

    # Processar cada URL
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
            print(f"Dados coletados para {url}: {club_data}")  # Depuração
        time.sleep(2)  # Delay de 2 segundos para evitar bloqueio

    # Verificar se há dados antes de salvar
    if club_data_list:
        # Gerar CSV para clubes
        clubes_df = pd.DataFrame(club_data_list)
        clubes_df.to_csv('clubes_infobox_all.csv', index=False, encoding='utf-8')
        print("Dados dos clubes salvos em 'clubes_infobox_all.csv'")
    else:
        print("Nenhum dado de clube foi coletado para salvar.")

    if stadium_data_list:
        # Gerar CSV para estádios
        estadios_df = pd.DataFrame(stadium_data_list)
        estadios_df.to_csv('estadios_infobox_all.csv', index=False, encoding='utf-8')
        print("Dados dos estádios salvos em 'estadios_infobox_all.csv'")
    else:
        print("Nenhum dado de estádio foi coletado para salvar.")

if __name__ == "__main__":
    main()
