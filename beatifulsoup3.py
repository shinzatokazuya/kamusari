import requests
from bs4 import BeautifulSoup
import time
import sqlite3

# 1. Definir a URL do site que você quer analisar
url = 'https://pt.wikipedia.org/wiki/Participações_dos_clubes_no_Campeonato_Brasileiro_de_Futebol'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# 2. Conectar ao banco de dados SQLite3 (ou criar se não existir)
conn = sqlite3.connect('teste.db')
cursor = conn.cursor()

# 3. Criar a tabela se ela não existir
cursor.execute('''
    CREATE TABLE IF NOT EXISTS clubes (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        clube TEXT,
        cidade TEXT,
        UF TEXT
    )
''')
conn.commit()

try:
    # 4. Fazer a requisição HTTP e analisar o HTML
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    # 5. Encontrar a tabela diretamente pela classe
    tabela_desejada = soup.find('table', class_='wikitable')

    if tabela_desejada:
        # 6. Extrair e inserir os dados da tabela
        for linha in tabela_desejada.find_all('tr'):
            celulas = linha.find_all('td')
            if len(celulas) >= 3:  # Verifica se a linha tem pelo menos 3 células
                uf = celulas[0].text.strip()
                clube = celulas[1].text.strip()
                cidade = celulas[2].text.strip()

                # Inserir os dados no banco de dados
                cursor.execute('''
                    INSERT INTO clubes (clube, cidade, UF)
                    VALUES (?, ?, ?)
                ''', (clube, cidade, uf))
                conn.commit()
                print(f"Clube: {clube}, Cidade: {cidade}, UF: {uf} inseridos no banco de dados.")
            else:
                print("Linha incompleta encontrada, pulando...")
    else:
        print("Tabela desejada não encontrada.")

except requests.exceptions.RequestException as e:
    print(f"Erro ao acessar a URL: {e}")
except AttributeError as e:
    print(f"Erro ao encontrar o elemento: {e}")
except sqlite3.Error as e:
    print(f"Erro ao acessar o banco de dados: {e}")

finally:
    # 8. Fechar a conexão com o banco de dados
    if conn:  # Verifica se a conexão foi aberta
        conn.close()

print("Fim do processo.")
