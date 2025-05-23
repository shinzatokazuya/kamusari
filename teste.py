import requests
from bs4 import BeautifulSoup
import sqlite3
import re

# 1. Definir a URL do site que você quer analisar
url = 'https://www.ogol.com.br/edicao/brasileiro-1979/3866/calendario?equipa=0&estado=1&filtro=&op=calendario&page=11'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# 2. Conectar ao banco de dados SQLite3 (ou criar se não existir)
conn = sqlite3.connect('teste.db')
cursor = conn.cursor()

# 3. Criar a tabela se ela não existir
cursor.execute('''
    CREATE TABLE IF NOT EXISTS partidas (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        hora TEXT,
        id_mandante INTEGER,
        mandante_placar INTEGER,
        visitante_placar INTEGER,
        id_visitante INTEGER,
        fase TEXT,
        FOREIGN KEY (id_mandante) REFERENCES clubes(ID),
        FOREIGN KEY (id_visitante) REFERENCES clubes(ID)
    )
''')
conn.commit()

def buscar_id_clube(cursor, nome_clube):
    """Busca o ID de um clube no banco de dados pelo nome."""
    cursor.execute("SELECT ID FROM clubes WHERE clube LIKE ?", ("%" + nome_clube + "%",))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

try:
    # 4. Fazer a requisição HTTP e analisar o HTML
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    # 5. Encontrar a tabela diretamente pela classe (ou outro seletor)
    tabela_desejada = soup.find('table', class_='zztable stats')  # Use a classe correta aqui

    if tabela_desejada:
        for linha in tabela_desejada.find_all('tr'):
            celulas = linha.find_all('td')
            if len(celulas) >= 6:
                data = celulas[1].text.strip()
                hora = celulas[2].text.strip()
                mandante = celulas[3].text.strip()
                placar = celulas[5].text.strip()
                visitante = celulas[7].text.strip()
                fase = celulas[8].text.strip()

                id_mandante = buscar_id_clube(cursor, mandante)
                id_visitante = buscar_id_clube(cursor, visitante)

                if id_mandante and id_visitante:
                    placar_raw = celulas[5].text.strip().upper()

                    if "WO" in placar_raw or "W.O." in placar_raw:
                        # Define regra: mandante vence por 3x0
                        mandante_placar, visitante_placar = 3, 0
                    else:
                        if '-' not in placar:
                            print(f"Placar inválido: {placar}, pulando partida")
                            continue
                        try:
                            placar_limpo = re.search(r'(\d+)\s*-\s*(\d+)', placar)
                            if placar_limpo:
                                mandante_placar = int(placar_limpo.group(1))
                                visitante_placar = int(placar_limpo.group(2))
                            else:
                                print(f"Placar mal formatado: {placar}, pulando partida")
                                continue

                        except ValueError:
                            print(f"Erro ao converter placar: {placar}, pulando partida")
                            continue

                    # Inserir os dados no banco de dados
                    cursor.execute('''
                        INSERT INTO partidas (data, hora, id_mandante, mandante_placar, visitante_placar, id_visitante, fase)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (data, hora, id_mandante, mandante_placar, visitante_placar, id_visitante, fase))
                    conn.commit()
                    print(
                        f"Partida: {mandante} {mandante_placar} x {visitante_placar} {visitante} inserida no banco de dados.")
                else:
                    print(
                        f"Clube mandante ou visitante não encontrado. Mandante: {mandante}, Visitante: {visitante}, pulando partida"
                    )
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
    if conn:
        conn.close()

print("Fim do processo.")

