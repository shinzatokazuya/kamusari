"""
ingestao.py - Leitura dos CSVs e populacao do banco SQLite
Execucao: python ingestao.py
Pre-requisitos: pip install pandas
"""

import sqlite3
import pandas as pd
import re
from pathlib import Path

CSV_DIR = Path(__file__).parent.parent.parent / "output_csvs"
DB_PATH = CSV_DIR / "brasileirao.db"


# ==============================================================================
# 1. UTILITARIOS
# ==============================================================================

def normalizar_data(serie):
    def _converter(valor):
        if pd.isna(valor) or str(valor).strip() in ("", "nan", "None"):
            return None
        s = str(valor).strip()
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            return s
        m = re.match(r'^(\d{2})[/\-](\d{2})[/\-](\d{4})$', s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
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


def ler_csv(nome):
    path = CSV_DIR / nome
    if not path.exists():
        print(f"  aviso: {nome} nao encontrado, pulando.")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.where(df != "", other=None)
    print(f"  ok  {nome}: {len(df)} linhas")
    return df


def ids_validos_de(conn, tabela, coluna="id"):
    """Retorna conjunto de IDs existentes em uma tabela do banco."""
    rows = conn.execute(f"SELECT {coluna} FROM {tabela}").fetchall()
    return {r[0] for r in rows if r[0] is not None}


def sanitizar_fk(df, col, ids_validos, nome_tabela=""):
    """
    Converte para None qualquer valor em `col` que nao exista em `ids_validos`.
    Retorna o df modificado e imprime quantos foram descartados.
    """
    if col not in df.columns:
        return df
    df = df.copy()
    antes = df[col].notna().sum()

    def _fix(val):
        if val is None or pd.isna(val) if not isinstance(val, str) else val in ("", "None", "nan"):
            return None
        try:
            v = int(float(val))
            return v if v in ids_validos else None
        except (ValueError, TypeError):
            return None

    df[col] = df[col].apply(_fix)
    depois = df[col].notna().sum()
    perdidos = int(antes - depois)
    if perdidos:
        print(f"  aviso: {perdidos} valor(es) em '{col}' sem referencia em {nome_tabela} -> NULL")
    return df


def sanitizar_fk_int(df, col, ids_validos, nome_tabela=""):
    """
    Versao para colunas que ja foram convertidas para int/None via safe_int.
    """
    if col not in df.columns:
        return df
    df = df.copy()
    antes = df[col].notna().sum()
    df[col] = df[col].apply(lambda v: v if (v is None or v in ids_validos) else None)
    depois = df[col].notna().sum()
    perdidos = int(antes - depois)
    if perdidos:
        print(f"  aviso: {perdidos} valor(es) em '{col}' sem referencia em {nome_tabela} -> NULL")
    return df


# ==============================================================================
# 2. SCHEMA
# ==============================================================================

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS locais (
    id      INTEGER PRIMARY KEY,
    cidade  TEXT,
    uf      TEXT,
    estado  TEXT,
    regiao  TEXT,
    pais    TEXT
);

CREATE TABLE IF NOT EXISTS campeonatos (
    id         INTEGER PRIMARY KEY,
    campeonato TEXT NOT NULL,
    pais       TEXT NOT NULL,
    entidade   TEXT,
    tipo       TEXT,
    criado_em  TEXT
);

CREATE TABLE IF NOT EXISTS jogadores (
    id            INTEGER PRIMARY KEY,
    nome          TEXT NOT NULL,
    apelido       TEXT,
    nascimento    TEXT,
    falecimento   TEXT,
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

CREATE TABLE IF NOT EXISTS estadios (
    id          INTEGER PRIMARY KEY,
    estadio     TEXT NOT NULL,
    capacidade  INTEGER,
    local_id    INTEGER REFERENCES locais(id),
    inauguracao TEXT,
    ativo       INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS clubes (
    id       INTEGER PRIMARY KEY,
    clube    TEXT NOT NULL,
    apelido  TEXT,
    local_id INTEGER REFERENCES locais(id),
    fundacao TEXT,
    ativo    INTEGER DEFAULT 1
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

CREATE TABLE IF NOT EXISTS jogadores_em_partida (
    partida_id     INTEGER NOT NULL REFERENCES partidas(id),
    jogador_id     INTEGER NOT NULL REFERENCES jogadores(id),
    clube_id       INTEGER NOT NULL REFERENCES clubes(id),
    titular        INTEGER NOT NULL DEFAULT 1,
    posicao_jogada TEXT,
    numero_camisa  INTEGER,
    PRIMARY KEY (partida_id, jogador_id)
);

CREATE TABLE IF NOT EXISTS treinadores_em_partida (
    partida_id   INTEGER NOT NULL REFERENCES partidas(id),
    treinador_id INTEGER,
    clube_id     INTEGER NOT NULL REFERENCES clubes(id),
    tipo         TEXT DEFAULT 'Titular'
);

CREATE TABLE IF NOT EXISTS arbitros_em_partida (
    partida_id INTEGER NOT NULL REFERENCES partidas(id),
    arbitro_id INTEGER REFERENCES arbitros(id)
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

CREATE INDEX IF NOT EXISTS idx_partidas_edicao ON partidas(edicao_id);
CREATE INDEX IF NOT EXISTS idx_partidas_data   ON partidas(data);
CREATE INDEX IF NOT EXISTS idx_jep_jogador     ON jogadores_em_partida(jogador_id);
CREATE INDEX IF NOT EXISTS idx_eventos_partida ON eventos_partida(partida_id);
CREATE INDEX IF NOT EXISTS idx_eventos_jogador ON eventos_partida(jogador_id);
CREATE INDEX IF NOT EXISTS idx_tep_clube       ON treinadores_em_partida(clube_id);
CREATE INDEX IF NOT EXISTS idx_aep_partida     ON arbitros_em_partida(partida_id);
"""


# ==============================================================================
# 3. INGESTAO POR TABELA
# ==============================================================================

def inserir(conn, tabela, df, colunas):
    if df.empty:
        return
    placeholders = ", ".join("?" * len(colunas))
    sql = f"INSERT OR IGNORE INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
    rows = [tuple(row) for row in df[colunas].itertuples(index=False, name=None)]
    conn.executemany(sql, rows)
    n = conn.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0]
    print(f"    -> {tabela}: {n} registros totais")


def carregar_locais(conn, df):
    if df.empty:
        return
    df2 = df.rename(columns=str.lower).copy()
    df2["id"] = df2["id"].apply(safe_int)
    inserir(conn, "locais", df2, ["id", "cidade", "uf", "estado", "regiao", "pais"])


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
    df2["falecimento"] = df2["falecimento"].apply(safe_float)
    df2["aposentado"]  = df2["aposentado"].apply(safe_int)
    cols = ["id", "nome", "apelido", "nascimento", "falecimento",
            "nacionalidade", "naturalidade", "aposentado"]
    inserir(conn, "arbitros", df2, cols)


def carregar_estadios(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]          = df2["id"].apply(safe_int)
    df2["capacidade"]  = df2["capacidade"].apply(safe_int)
    df2["local_id"]    = df2["local_id"].apply(safe_int)
    df2["inauguracao"] = df2["inauguracao"].apply(safe_int)
    df2["ativo"]       = df2["ativo"].apply(safe_int)
    # Sanitiza FK local_id
    ids_loc = ids_validos_de(conn, "locais")
    df2 = sanitizar_fk_int(df2, "local_id", ids_loc, "locais")
    cols = ["id", "estadio", "capacidade", "local_id", "inauguracao", "ativo"]
    inserir(conn, "estadios", df2, cols)


def carregar_clubes(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["id"]       = df2["id"].apply(safe_int)
    df2["local_id"] = df2["local_id"].apply(safe_int)
    df2["ativo"]    = df2["ativo"].apply(safe_int)
    ids_loc = ids_validos_de(conn, "locais")
    df2 = sanitizar_fk_int(df2, "local_id", ids_loc, "locais")
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
    ids_camp  = ids_validos_de(conn, "campeonatos")
    ids_clube = ids_validos_de(conn, "clubes")
    df2 = sanitizar_fk_int(df2, "campeonato_id", ids_camp,  "campeonatos")
    df2 = sanitizar_fk_int(df2, "campeao_id",    ids_clube, "clubes")
    df2 = sanitizar_fk_int(df2, "vice_id",        ids_clube, "clubes")
    cols = ["id", "campeonato_id", "ano", "data_inicio", "data_fim", "campeao_id", "vice_id"]
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
    ids_edicao  = ids_validos_de(conn, "edicoes")
    ids_camp    = ids_validos_de(conn, "campeonatos")
    ids_estadio = ids_validos_de(conn, "estadios")
    ids_clube   = ids_validos_de(conn, "clubes")
    df2 = sanitizar_fk_int(df2, "edicao_id",     ids_edicao,  "edicoes")
    df2 = sanitizar_fk_int(df2, "campeonato_id", ids_camp,    "campeonatos")
    df2 = sanitizar_fk_int(df2, "estadio_id",    ids_estadio, "estadios")
    df2 = sanitizar_fk_int(df2, "mandante_id",   ids_clube,   "clubes")
    df2 = sanitizar_fk_int(df2, "visitante_id",  ids_clube,   "clubes")
    # Remove partidas sem mandante ou visitante (NOT NULL no schema)
    antes = len(df2)
    df2 = df2.dropna(subset=["mandante_id", "visitante_id"])
    if len(df2) < antes:
        print(f"  aviso: {antes - len(df2)} partida(s) removidas por mandante/visitante invalido")
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
    ids_partida  = ids_validos_de(conn, "partidas")
    ids_jogador  = ids_validos_de(conn, "jogadores")
    ids_clube    = ids_validos_de(conn, "clubes")
    df2 = sanitizar_fk_int(df2, "partida_id", ids_partida, "partidas")
    df2 = sanitizar_fk_int(df2, "jogador_id", ids_jogador, "jogadores")
    df2 = sanitizar_fk_int(df2, "clube_id",   ids_clube,   "clubes")
    # Remove linhas com FKs NOT NULL invalidas
    antes = len(df2)
    df2 = df2.dropna(subset=["partida_id", "jogador_id", "clube_id"])
    if len(df2) < antes:
        print(f"  aviso: {antes - len(df2)} linha(s) em jogadores_em_partida removidas por FK invalida")
    df2 = df2.drop_duplicates(subset=["partida_id", "jogador_id"])
    cols = ["partida_id", "jogador_id", "clube_id", "titular", "posicao_jogada", "numero_camisa"]
    inserir(conn, "jogadores_em_partida", df2, cols)


def carregar_treinadores_em_partida(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["partida_id"]   = df2["partida_id"].apply(safe_int)
    df2["treinador_id"] = df2["treinador_id"].apply(safe_int)
    df2["clube_id"]     = df2["clube_id"].apply(safe_int)
    ids_partida   = ids_validos_de(conn, "partidas")
    ids_treinador = ids_validos_de(conn, "treinadores")
    ids_clube     = ids_validos_de(conn, "clubes")
    df2 = sanitizar_fk_int(df2, "partida_id",   ids_partida,   "partidas")
    df2 = sanitizar_fk_int(df2, "treinador_id", ids_treinador, "treinadores")  # pode virar NULL, ok
    df2 = sanitizar_fk_int(df2, "clube_id",     ids_clube,     "clubes")
    antes = len(df2)
    df2 = df2.dropna(subset=["partida_id", "clube_id"])
    if len(df2) < antes:
        print(f"  aviso: {antes - len(df2)} linha(s) em treinadores_em_partida removidas")
    df2 = df2.drop_duplicates(subset=["partida_id", "treinador_id", "clube_id"])
    sql = "INSERT OR IGNORE INTO treinadores_em_partida (partida_id, treinador_id, clube_id, tipo) VALUES (?, ?, ?, ?)"
    rows = [(r.partida_id, r.treinador_id, r.clube_id, r.tipo) for r in df2.itertuples(index=False)]
    conn.executemany(sql, rows)
    n = conn.execute("SELECT COUNT(*) FROM treinadores_em_partida").fetchone()[0]
    print(f"    -> treinadores_em_partida: {n} registros totais")


def carregar_arbitros_em_partida(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["partida_id"] = df2["partida_id"].apply(safe_int)
    df2["arbitro_id"] = df2["arbitro_id"].apply(safe_int)
    ids_partida = ids_validos_de(conn, "partidas")
    ids_arbitro = ids_validos_de(conn, "arbitros")
    df2 = sanitizar_fk_int(df2, "partida_id", ids_partida, "partidas")
    df2 = sanitizar_fk_int(df2, "arbitro_id", ids_arbitro, "arbitros")
    antes = len(df2)
    df2 = df2.dropna(subset=["partida_id"])
    if len(df2) < antes:
        print(f"  aviso: {antes - len(df2)} linha(s) em arbitros_em_partida removidas")
    df2 = df2.drop_duplicates(subset=["partida_id", "arbitro_id"])
    sql = "INSERT OR IGNORE INTO arbitros_em_partida (partida_id, arbitro_id) VALUES (?, ?)"
    rows = [(r.partida_id, r.arbitro_id) for r in df2.itertuples(index=False)]
    conn.executemany(sql, rows)
    n = conn.execute("SELECT COUNT(*) FROM arbitros_em_partida").fetchone()[0]
    print(f"    -> arbitros_em_partida: {n} registros totais")


def carregar_eventos_partida(conn, df):
    if df.empty:
        print("  aviso: eventos_partida.csv vazio, pulando.")
        return
    df2 = df.copy()
    for col in ["id", "partida_id", "jogador_id", "clube_id"]:
        df2[col] = df2[col].apply(safe_int)
    ids_partida = ids_validos_de(conn, "partidas")
    ids_jogador = ids_validos_de(conn, "jogadores")
    ids_clube   = ids_validos_de(conn, "clubes")
    df2 = sanitizar_fk_int(df2, "partida_id", ids_partida, "partidas")
    df2 = sanitizar_fk_int(df2, "jogador_id", ids_jogador, "jogadores")
    df2 = sanitizar_fk_int(df2, "clube_id",   ids_clube,   "clubes")
    antes = len(df2)
    df2 = df2.dropna(subset=["partida_id", "jogador_id", "clube_id"])
    if len(df2) < antes:
        print(f"  aviso: {antes - len(df2)} evento(s) removidos por FK invalida")
    cols = ["id", "partida_id", "jogador_id", "clube_id", "tipo_evento", "tipo_gol", "minuto"]
    inserir(conn, "eventos_partida", df2, cols)


# ==============================================================================
# 4. MAIN
# ==============================================================================

def main():
    print(f"\n CSV_DIR : {CSV_DIR}")
    print(f" DB_PATH : {DB_PATH}\n")

    # Apaga banco anterior se existir (re-execucao limpa)
    if DB_PATH.exists():
        DB_PATH.unlink()
        print("  banco anterior removido\n")

    conn = sqlite3.connect(DB_PATH)
    # FK OFF apenas durante criacao do schema, depois liga para todos os INSERTs
    conn.executescript("PRAGMA foreign_keys=OFF;\n" + SCHEMA)
    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()
    print("✅ Schema criado\n")

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
        print(f"-- {nome_csv}")
        df = ler_csv(nome_csv)
        func(conn, df)
        conn.commit()
        print()

    resultado = conn.execute("PRAGMA integrity_check").fetchone()
    print(f"Integrity check: {resultado[0]}")

    tabelas = [
        "locais", "campeonatos", "jogadores", "treinadores", "arbitros",
        "estadios", "clubes", "edicoes", "partidas",
        "jogadores_em_partida", "treinadores_em_partida",
        "arbitros_em_partida", "eventos_partida"
    ]
    print("\nRegistros por tabela:")
    for t in tabelas:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   {t:<30} {n:>8,}")

    conn.close()
    print(f"\n✅ Banco criado em: {DB_PATH}\n")


if __name__ == "__main__":
    main()
