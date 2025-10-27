import sqlite3
import os

# Caminhos dos bancos
DB_ANTIGO = "teste_bd_1971.db"
DB_NOVO = "estruturado_bd_1971.db"
SCHEMA_NOVO = "tabelas.txt"

def criar_banco_novo():
    """Cria o novo banco e aplica o schema"""
    if os.path.exists(DB_NOVO):
        os.remove(DB_NOVO)
        print("🧹 Banco novo antigo removido para recriação.")

    with open(SCHEMA_NOVO, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = sqlite3.connect(DB_NOVO)
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()
    print("✅ Novo banco criado com sucesso.")

def migrar_dados():
    """Migra os dados do banco antigo para o novo"""
    conn_novo = sqlite3.connect(DB_NOVO)
    conn_antigo = sqlite3.connect(DB_ANTIGO)

    conn_novo.execute("PRAGMA foreign_keys = ON;")
    conn_antigo.execute("PRAGMA foreign_keys = ON;")

    cur_novo = conn_novo.cursor()
    cur_antigo = conn_antigo.cursor()

    # --- LOCAIS ---
    print("➡️ Migrando locais...")
    cur_antigo.execute("SELECT cidade, UF, regiao, pais FROM locais;")
    locais = cur_antigo.fetchall()
    cur_novo.executemany("INSERT INTO locais (cidade, UF, regiao, pais) VALUES (?, ?, ?, ?)", locais)

    # --- ESTÁDIOS ---
    print("➡️ Migrando estadios...")
    cur_antigo.execute("SELECT estadio, capacidade, local_id FROM estadios;")
    estadios = cur_antigo.fetchall()
    cur_novo.executemany("INSERT INTO estadios (estadio, capacidade, local_id) VALUES (?, ?, ?)", estadios)

    # --- CLUBES ---
    print("➡️ Migrando clubes...")
    cur_antigo.execute("SELECT clube, local_id FROM clubes;")
    clubes = cur_antigo.fetchall()
    cur_novo.executemany("INSERT INTO clubes (clube, local_id) VALUES (?, ?)", clubes)

    # --- CAMPEONATOS ---
    print("➡️ Migrando campeonatos...")
    cur_antigo.execute("SELECT campeonato, pais, entidade FROM campeonatos;")
    campeonatos = cur_antigo.fetchall()
    cur_novo.executemany("INSERT INTO campeonatos (campeonato, pais, entidade) VALUES (?, ?, ?)", campeonatos)

    # --- EDIÇÕES ---
    print("➡️ Migrando edicoes...")
    cur_antigo.execute("SELECT campeonato_id, ano FROM edicoes;")
    edicoes = cur_antigo.fetchall()
    cur_novo.executemany("INSERT INTO edicoes (campeonato_id, ano) VALUES (?, ?)", edicoes)

    # --- PARTIDAS ---
    print("➡️ Migrando partidas...")
    cur_antigo.execute("""
        SELECT edicao_id, estadio_id, data, hora, fase, mandante_id, visitante_id,
               mandante_placar, visitante_placar, mandante_penalti, visitante_penalti, prorrogacao
        FROM partidas;
    """)
    partidas = cur_antigo.fetchall()
    cur_novo.executemany("""
        INSERT INTO partidas (
            edicao_id, estadio_id, data, hora, fase, mandante_id, visitante_id,
            mandante_placar, visitante_placar, mandante_penalti, visitante_penalti, prorrogacao
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, partidas)
