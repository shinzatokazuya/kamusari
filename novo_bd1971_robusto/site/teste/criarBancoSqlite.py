"""
ingestao.py — Leitura dos CSVs e população do banco SQLite
Execução: python ingestao.py

Pré-requisitos:
    pip install pandas

Coloque este arquivo na mesma pasta que os CSVs.
O banco será criado em ../bd/brasileirao.db (ajuste DB_PATH se quiser).
"""

import sqlite3
import pandas as pd
import os
import re
from pathlib import Path

# ── Configuração ────────────────────────────────────────────────────────────────
CSV_DIR  = Path(__file__).parent.parent.parent / "output_csvs"            # pasta onde estão os CSVs
DB_PATH  = CSV_DIR  / "site" / "teste" / "brasileirao.db"     # ajuste se quiser outro local
# ────────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# 1. UTILITÁRIOS
# ══════════════════════════════════════════════════════════════════════════════

def normalizar_data(serie: pd.Series) -> pd.Series:
    """
    Recebe uma série com datas em vários formatos (DD/MM/YYYY, YYYY-MM-DD,
    textos livres) e devolve strings ISO 8601 (YYYY-MM-DD) ou None.
    O SQLite armazena DATE como TEXT; a exibição DD/MM/YYYY fica no front-end.
    """
    def _converter(valor):
        if pd.isna(valor) or str(valor).strip() in ("", "nan", "None"):
            return None
        s = str(valor).strip()
        # Já está em ISO
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            return s
        # DD/MM/YYYY ou DD-MM-YYYY
        m = re.match(r'^(\d{2})[/\-](\d{2})[/\-](\d{4})$', s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # Qualquer outra coisa: tenta pd.to_datetime
        try:
            return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            return None

    return serie.apply(_converter)


def safe_int(val):
    try:
        v = float(val)
        return None if pd.isna(v) else int(v)
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        v = float(val)
        return None if pd.isna(v) else round(v, 4)
    except (ValueError, TypeError):
        return None


def csv(nome: str) -> pd.DataFrame:
    path = CSV_DIR / nome
    if not path.exists():
        print(f"  ⚠  {nome} não encontrado, pulando.")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.where(df != "", other=None)   # string vazia → None → NULL no SQLite
    print(f"  ✓  {nome}: {len(df)} linhas, colunas: {list(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ── Tabelas base ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS locais (
    id        INTEGER PRIMARY KEY,
    cidade    TEXT,
    uf        TEXT,
    estado    TEXT,
    regiao    TEXT,
    pais      TEXT
);

CREATE TABLE IF NOT EXISTS campeonatos (
    id          INTEGER PRIMARY KEY,
    campeonato  TEXT NOT NULL,
    pais        TEXT NOT NULL,
    entidade    TEXT,
    tipo        TEXT,
    criado_em   TEXT
);

CREATE TABLE IF NOT EXISTS jogadores (
    id            INTEGER PRIMARY KEY,
    nome          TEXT NOT NULL,
    apelido       TEXT,
    nascimento    TEXT,   -- YYYY-MM-DD
    falecimento   TEXT,   -- YYYY-MM-DD
    nacionalidade TEXT,
    naturalidade  TEXT,
    altura        INTEGER,
    peso          INTEGER,
    posicao       TEXT,
    pe_preferido  TEXT,
    aposentado    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS treinadores (
    id            INTEGER PRIMARY KEY,
    nome          TEXT NOT NULL,
    apelido       TEXT,
    nascimento    TEXT,
    falecimento   TEXT,
    nacionalidade TEXT,
    naturalidade  TEXT,
    aposentado    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS arbitros (
    id            INTEGER PRIMARY KEY,
    nome          TEXT NOT NULL,
    apelido       TEXT,
    nascimento    TEXT,
    falecimento   TEXT,
    nacionalidade TEXT,
    naturalidade  TEXT,
    aposentado    INTEGER
);

-- ── Tabelas intermediárias ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS estadios (
    id          INTEGER PRIMARY KEY,
    estadio     TEXT NOT NULL,
    capacidade  INTEGER,
    local_id    INTEGER REFERENCES locais(id),
    inauguracao TEXT,
    ativo       INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS clubes (
    id        INTEGER PRIMARY KEY,
    clube     TEXT NOT NULL,
    apelido   TEXT,
    local_id  INTEGER REFERENCES locais(id),
    fundacao  TEXT,
    ativo     INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS edicoes (
    id            INTEGER PRIMARY KEY,
    campeonato_id INTEGER NOT NULL REFERENCES campeonatos(id),
    ano           TEXT NOT NULL,
    data_inicio   TEXT,
    data_fim      TEXT,
    campeao_id    INTEGER REFERENCES clubes(id),
    vice_id       INTEGER REFERENCES clubes(id),
    criado_em     TEXT
);

-- ── Partidas ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS partidas (
    id                INTEGER PRIMARY KEY,
    edicao_id         INTEGER REFERENCES edicoes(id),
    campeonato_id     INTEGER REFERENCES campeonatos(id),
    data              TEXT,
    hora              TEXT,
    fase              TEXT,
    rodada            INTEGER,
    estadio_id        INTEGER REFERENCES estadios(id),
    mandante_id       INTEGER NOT NULL REFERENCES clubes(id),
    visitante_id      INTEGER NOT NULL REFERENCES clubes(id),
    mandante_placar   INTEGER,
    visitante_placar  INTEGER,
    mandante_penalti  INTEGER,
    visitante_penalti INTEGER,
    prorrogacao       INTEGER DEFAULT 0,
    publico           INTEGER
);

-- ── Vínculos com a partida ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jogadores_em_partida (
    partida_id      INTEGER NOT NULL REFERENCES partidas(id),
    jogador_id      INTEGER NOT NULL REFERENCES jogadores(id),
    clube_id        INTEGER NOT NULL REFERENCES clubes(id),
    titular         INTEGER NOT NULL DEFAULT 1,
    posicao_jogada  TEXT,
    numero_camisa   INTEGER,
    PRIMARY KEY (partida_id, jogador_id)
);

CREATE TABLE IF NOT EXISTS treinadores_em_partida (
    partida_id   INTEGER NOT NULL REFERENCES partidas(id),
    treinador_id INTEGER,                    -- NULL = treinador desconhecido
    clube_id     INTEGER NOT NULL REFERENCES clubes(id),
    tipo         TEXT DEFAULT 'Titular'
    -- SEM PRIMARY KEY composta porque treinador_id pode ser NULL
);

CREATE TABLE IF NOT EXISTS arbitros_em_partida (
    partida_id INTEGER NOT NULL REFERENCES partidas(id),
    arbitro_id INTEGER REFERENCES arbitros(id)
    -- arbitro_id pode ser NULL (partida sem árbitro registrado)
);

CREATE TABLE IF NOT EXISTS eventos_partida (
    id          INTEGER PRIMARY KEY,
    partida_id  INTEGER NOT NULL REFERENCES partidas(id),
    jogador_id  INTEGER NOT NULL REFERENCES jogadores(id),
    clube_id    INTEGER NOT NULL REFERENCES clubes(id),
    tipo_evento TEXT NOT NULL,
    tipo_gol    TEXT,
    minuto      TEXT
);

-- ── Índices úteis para as queries do app ───────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_partidas_edicao  ON partidas(edicao_id);
CREATE INDEX IF NOT EXISTS idx_partidas_data    ON partidas(data);
CREATE INDEX IF NOT EXISTS idx_jep_jogador      ON jogadores_em_partida(jogador_id);
CREATE INDEX IF NOT EXISTS idx_eventos_partida  ON eventos_partida(partida_id);
CREATE INDEX IF NOT EXISTS idx_eventos_jogador  ON eventos_partida(jogador_id);
CREATE INDEX IF NOT EXISTS idx_tep_clube        ON treinadores_em_partida(clube_id);
CREATE INDEX IF NOT EXISTS idx_aep_partida      ON arbitros_em_partida(partida_id);
"""


# ══════════════════════════════════════════════════════════════════════════════
# 3. INGESTÃO POR TABELA
# ══════════════════════════════════════════════════════════════════════════════

def inserir(conn: sqlite3.Connection, tabela: str, df: pd.DataFrame, colunas: list[str]):
    """INSERT OR IGNORE genérico para evitar duplicatas em re-execuções."""
    if df.empty:
        return
    placeholders = ", ".join("?" * len(colunas))
    sql = f"INSERT OR IGNORE INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
    rows = [tuple(row) for row in df[colunas].itertuples(index=False, name=None)]
    conn.executemany(sql, rows)
    print(f"    → {tabela}: {conn.total_changes} inserções acumuladas")


def carregar_locais(conn, df):
    if df.empty:
        return
    cols = ["id", "cidade", "uf", "estado", "regiao", "pais"]
    df2 = df.rename(columns=str.lower).copy()
    df2["id"] = df2["id"].apply(safe_int)
    inserir(conn, "locais", df2, cols)


def carregar_campeonatos(conn, df):
    if df.empty:
        return
    df2 = df.rename(columns={"ID": "id"}).copy()
    df2["id"] = df2["id"].apply(safe_int)
    inserir(conn, "campeonatos", df2, ["id", "campeonato", "pais", "entidade", "tipo"])


def carregar_jogadores(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]          = df2["id"].apply(safe_int)
    df2["nascimento"]  = normalizar_data(df2["nascimento"])
    df2["falecimento"] = normalizar_data(df2["falecimento"])
    df2["altura"]      = df2["altura"].apply(safe_int)
    df2["peso"]        = df2["peso"].apply(safe_int)
    df2["aposentado"]  = df2["aposentado"].apply(safe_int)
    cols = ["id", "nome", "apelido", "nascimento", "falecimento",
            "nacionalidade", "naturalidade", "altura", "peso",
            "posicao", "pe_preferido", "aposentado"]
    inserir(conn, "jogadores", df2, cols)


def carregar_treinadores(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]          = df2["id"].apply(safe_int)
    df2["nascimento"]  = normalizar_data(df2["nascimento"])
    df2["falecimento"] = normalizar_data(df2["falecimento"])
    df2["aposentado"]  = df2["aposentado"].apply(safe_int)
    cols = ["id", "nome", "apelido", "nascimento", "falecimento",
            "nacionalidade", "naturalidade", "aposentado"]
    inserir(conn, "treinadores", df2, cols)


def carregar_arbitros(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]          = df2["id"].apply(safe_int)
    df2["nascimento"]  = normalizar_data(df2["nascimento"])
    df2["falecimento"] = df2["falecimento"].apply(safe_float)   # pode ser float no CSV
    df2["aposentado"]  = df2["aposentado"].apply(safe_int)
    cols = ["id", "nome", "apelido", "nascimento", "falecimento",
            "nacionalidade", "naturalidade", "aposentado"]
    inserir(conn, "arbitros", df2, cols)


def carregar_estadios(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]         = df2["id"].apply(safe_int)
    df2["capacidade"] = df2["capacidade"].apply(safe_int)
    df2["local_id"]   = df2["local_id"].apply(safe_int)
    df2["inauguracao"]= df2["inauguracao"].apply(safe_int)   # ano inteiro no CSV
    df2["ativo"]      = df2["ativo"].apply(safe_int)
    cols = ["id", "estadio", "capacidade", "local_id", "inauguracao", "ativo"]
    inserir(conn, "estadios", df2, cols)


def carregar_clubes(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]       = df2["id"].apply(safe_int)
    df2["local_id"] = df2["local_id"].apply(safe_int)
    df2["ativo"]    = df2["ativo"].apply(safe_int)
    cols = ["id", "clube", "apelido", "local_id", "fundacao", "ativo"]
    inserir(conn, "clubes", df2, cols)


def carregar_edicoes(conn, df):
    if df.empty:
        return
    df2 = df.rename(columns={"ID": "id"}).copy()
    df2["id"]            = df2["id"].apply(safe_int)
    df2["campeonato_id"] = df2["campeonato_id"].apply(safe_int)
    df2["data_inicio"]   = normalizar_data(df2["data_inicio"])
    df2["data_fim"]      = normalizar_data(df2["data_fim"])
    df2["campeao_id"]    = df2["campeao_id"].apply(safe_int)
    df2["vice_id"]       = df2["vice_id"].apply(safe_int)
    cols = ["id", "campeonato_id", "ano", "data_inicio", "data_fim",
            "campeao_id", "vice_id"]
    inserir(conn, "edicoes", df2, cols)


def carregar_partidas(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    for col in ["id", "edicao_id", "campeonato_id", "rodada", "estadio_id",
                "mandante_id", "visitante_id", "mandante_placar",
                "visitante_placar", "mandante_penalti", "visitante_penalti",
                "prorrogacao", "publico"]:
        df2[col] = df2[col].apply(safe_int)
    df2["data"] = normalizar_data(df2["data"])
    cols = ["id", "edicao_id", "campeonato_id", "data", "hora", "fase",
            "rodada", "estadio_id", "mandante_id", "visitante_id",
            "mandante_placar", "visitante_placar", "mandante_penalti",
            "visitante_penalti", "prorrogacao", "publico"]
    inserir(conn, "partidas", df2, cols)


def carregar_jogadores_em_partida(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    for col in ["partida_id", "jogador_id", "clube_id", "titular", "numero_camisa"]:
        df2[col] = df2[col].apply(safe_int)
    # Desduplicar: a PK é (partida_id, jogador_id)
    df2 = df2.drop_duplicates(subset=["partida_id", "jogador_id"])
    cols = ["partida_id", "jogador_id", "clube_id", "titular",
            "posicao_jogada", "numero_camisa"]
    inserir(conn, "jogadores_em_partida", df2, cols)


def carregar_treinadores_em_partida(conn, df):
    """
    treinador_id pode ser NULL → não usamos PRIMARY KEY composta.
    Inserimos com INSERT OR IGNORE baseado nos três campos juntos.
    """
    if df.empty:
        return
    df2 = df.copy()
    df2["partida_id"]   = df2["partida_id"].apply(safe_int)
    df2["treinador_id"] = df2["treinador_id"].apply(safe_int)   # pode ficar None
    df2["clube_id"]     = df2["clube_id"].apply(safe_int)
    # Remove duplicatas exatas
    df2 = df2.drop_duplicates(subset=["partida_id", "treinador_id", "clube_id"])
    sql = """
        INSERT INTO treinadores_em_partida (partida_id, treinador_id, clube_id, tipo)
        VALUES (?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """
    rows = [
        (row.partida_id, row.treinador_id, row.clube_id, row.tipo)
        for row in df2.itertuples(index=False)
    ]
    conn.executemany(sql, rows)


def carregar_arbitros_em_partida(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["partida_id"] = df2["partida_id"].apply(safe_int)
    df2["arbitro_id"] = df2["arbitro_id"].apply(safe_int)   # pode ficar None
    df2 = df2.drop_duplicates(subset=["partida_id", "arbitro_id"])
    sql = "INSERT INTO arbitros_em_partida (partida_id, arbitro_id) VALUES (?, ?)"
    rows = [(r.partida_id, r.arbitro_id) for r in df2.itertuples(index=False)]
    conn.executemany(sql, rows)


def carregar_eventos_partida(conn, df):
    if df.empty:
        print("  ⚠  eventos_partida.csv vazio, pulando.")
        return
    df2 = df.copy()
    for col in ["id", "partida_id", "jogador_id", "clube_id"]:
        df2[col] = df2[col].apply(safe_int)
    cols = ["id", "partida_id", "jogador_id", "clube_id",
            "tipo_evento", "tipo_gol", "minuto"]
    inserir(conn, "eventos_partida", df2, cols)


# ══════════════════════════════════════════════════════════════════════════════
# 4. MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n📂 CSV_DIR : {CSV_DIR}")
    print(f"🗄  DB_PATH : {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=OFF")   # desliga FK durante carga
    conn.executescript(SCHEMA)
    conn.commit()
    print("✅ Schema criado/validado\n")

    # Ordem de inserção respeita dependências entre tabelas
    etapas = [
        ("locais.csv",                carregar_locais),
        ("campeonatos.csv",           carregar_campeonatos),
        ("jogadores.csv",             carregar_jogadores),
        ("treinadores.csv",           carregar_treinadores),
        ("arbitros.csv",              carregar_arbitros),
        ("estadios.csv",              carregar_estadios),
        ("clubes.csv",                carregar_clubes),
        ("edicoes.csv",               carregar_edicoes),
        ("partidas.csv",              carregar_partidas),
        ("jogadores_em_partida.csv",  carregar_jogadores_em_partida),
        ("treinadores_em_partida.csv",carregar_treinadores_em_partida),
        ("arbitros_em_partida.csv",   carregar_arbitros_em_partida),
        ("eventos_partida.csv",       carregar_eventos_partida),
    ]

    for nome_csv, func in etapas:
        print(f"── {nome_csv}")
        df = csv(nome_csv)
        func(conn, df)
        conn.commit()
        print()

    # Liga FK e valida integridade
    conn.execute("PRAGMA foreign_keys=ON")
    resultado = conn.execute("PRAGMA integrity_check").fetchone()
    print(f"\n🔍 Integrity check: {resultado[0]}")

    # Estatísticas finais
    tabelas = [
        "locais", "campeonatos", "jogadores", "treinadores", "arbitros",
        "estadios", "clubes", "edicoes", "partidas",
        "jogadores_em_partida", "treinadores_em_partida",
        "arbitros_em_partida", "eventos_partida"
    ]
    print("\n📊 Registros por tabela:")
    for t in tabelas:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   {t:<30} {n:>8,}")

    conn.close()
    print(f"\n✅ Banco criado em: {DB_PATH}\n")


if __name__ == "__main__":
    main()
