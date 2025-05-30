import json
import sqlite3

# Carregar JSON do arquivo
with open('clubes.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Conectar ao banco SQLite (ou criar)
conn = sqlite3.connect('clubes.db')
cur = conn.cursor()

# Criar as tabelas (se ainda não criadas)
cur.execute('''
CREATE TABLE IF NOT EXISTS clubes (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT,
  cidade TEXT,
  estado TEXT,
  regiao TEXT,
  nome_completo TEXT,
  fundacao TEXT,
  cores TEXT,
  escudo TEXT
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS estadios (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  clube_id INTEGER,
  nome TEXT,
  cidade TEXT,
  FOREIGN KEY (clube_id) REFERENCES clubes(id)
)
''')

# Inserir clubes
for clube in data['clubes']:
    # transformar lista de cores em JSON string
    cores_json = json.dumps(clube['cores'], ensure_ascii=False)

    cur.execute('''
        INSERT OR REPLACE INTO clubes (nome, cidade, estado, regiao, nome_completo, fundacao, cores, escudo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (clube['id'], clube['nome'], clube['cidade'], clube['estado'], clube['regiao'],
          clube['nome_completo'], clube['ano_fundacao'], cores_json, clube['escudo']))

# Inserir estádios
for estadio in data['estadios']:
    cur.execute('''
        INSERT INTO estadios (clube_id, nome, cidade) VALUES (?, ?, ?)
    ''', (estadio['clube_id'], estadio['nome'], estadio['cidade']))

conn.commit()
conn.close()

print("Importação concluída!")
