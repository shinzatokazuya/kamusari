import requests
from bs4 import BeautifulSoup
import time
import sqlite3

# 1. Definir a URL do site que você quer analisar
url = 'https://www.ogol.com.br/edicao/campeonato-brasileiro-serie-a-2004/457'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# 2. Conectar ao banco de dados SQLite3 (ou criar se não existir)
conn = sqlite3.connect('brasileirao_2004.db')
cursor = conn.cursor()

# 3. Criar a tabela se ela não existir (MODIFICADO para 12 colunas)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS classificacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        posicao INTEGER,
        time TEXT,
        pontos INTEGER,
        jogos_disputados INTEGER,
        vitorias INTEGER,
        empates INTEGER,
        derrotas INTEGER,
        gols_marcados INTEGER,
        gols_sofridos INTEGER,
        saldo_de_gols INTEGER,
        aproveitamento TEXT,
        nome_do_time TEXT
    )
''')
conn.commit()

try:
    # 4. Fazer a requisição HTTP e analisar o HTML
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    # 5. Encontrar o elemento 'container'
    container = soup.find('div', id='page_content')

    if container:
        # 6. Navegar até a tabela desejada
        tabela_desejada = container.find('table').find('table')

        if tabela_desejada:
            # 7. Extrair e inserir os dados da tabela (MODIFICADO para 12 colunas)
            for linha in tabela_desejada.find_all('tr', class_='zztable stats zz-datatable dataTable no-footer'):
                celulas = linha.find_all('td')
                if len(celulas) >= 11:  # Verifica se a linha tem pelo menos 11 células
                    posicao = int(celulas[0].text.strip())
                    nome_do_time = celulas[1].text.strip()
                    pontos = int(celulas[2].text.strip())
                    jogos_disputados = int(celulas[3].text.strip())
                    vitorias = int(celulas[4].text.strip())
                    empates = int(celulas[5].text.strip())
                    derrotas = int(celulas[6].text.strip())
                    gols_marcados = int(celulas[7].text.strip())
                    gols_sofridos = int(celulas[8].text.strip())
                    saldo_de_gols = int(celulas[9].text.strip())
                    aproveitamento = celulas[10].text.strip()

                    # Inserir os dados no banco de dados
                    cursor.execute('''
                        INSERT INTO classificacao (posicao, nome_do_time, pontos, jogos_disputados, vitorias, empates, derrotas,
                                                gols_marcados, gols_sofridos, saldo_de_gols, aproveitamento, time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (posicao, nome_do_time, pontos, jogos_disputados, vitorias, empates, derrotas,
                          gols_marcados, gols_sofridos, saldo_de_gols, aproveitamento, nome_do_time))
                    conn.commit()
                    print(f"Dados de {nome_do_time} inseridos no banco de dados.")
                else:
                    print("Linha incompleta encontrada, pulando...")
        else:
            print("Tabela desejada não encontrada.")
    else:
        print("Container não encontrado.")

except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL: {e}")
except AttributeError as e:
        print(f"Erro ao encontrar o elemento: {e}")
except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")

finally:
    # 8. Fechar a conexão com o banco de dados
    if conn:
        conn.close()

print("Fim do processo.")
