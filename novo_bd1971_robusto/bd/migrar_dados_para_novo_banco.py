# criar_banco.py
import sqlite3
import os
from pathlib import Path


# Ajuste estes caminhos conforme sua organização de pastas
SCHEMA_PATH = Path("tabelas/tabelas.txt")   # arquivo SQL com CREATE TABLE... (seu schema). :contentReference[oaicite:1]{index=1}
DB_PATH = Path("bd/estruturadoV2_bd_1971.db") # caminho do novo banco SQLite

# Caminhos dos bancos
DB_NOVO = "bd/estruturado_bd_1971.db"
SCHEMA_NOVO = "tabelas/tabelas.txt"


def criar_banco(schema_path=SCHEMA_PATH, db_path=DB_PATH, recreate=False):
    # cria diretório pai se necessário
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if recreate and db_path.exists():
        print(f"Removendo banco existente: {db_path}")
        db_path.unlink()

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema não encontrado: {schema_path}")

    if db_path.exists():
        print(f"Banco já existe em {db_path}. Use recreate=True para recriar.")
        return

    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        # executa o schema inteiro (várias CREATE TABLE)
        conn.executescript(schema_sql)
        conn.commit()
        # ativa foreign keys por segurança nas operações seguintes
        conn.execute("PRAGMA foreign_keys = ON;")
        print(f"Banco criado com sucesso em: {db_path}")
    finally:
        conn.close()

def checar_integridade(db_path=DB_PATH):
    if not db_path.exists():
        print("Banco não encontrado para checagem.")
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA foreign_key_check;")
        issues = cur.fetchall()
        if issues:
            print("⚠️ Problemas de integridade (foreign key) encontrados:")
            for i in issues[:20]:
                print(i)
        else:
            print("✅ Nenhuma inconsistência de foreign key detectada.")
    finally:
        conn.close()

if __name__ == "__main__":
    # Se quiser recriar sempre, troque para True
    criar_banco(recreate=False)
    checar_integridade()
