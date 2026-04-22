"""
ingestao.py - Leitura dos CSVs e populacao do banco SQLite
Execucao: python ingestao.py

Pre-requisitos:
    pip install pandas

Coloque este arquivo na mesma pasta que os CSVs.
"""

import sqlite3
import pandas as pd
import os
import re
from pathlib import Path

CSV_DIR = Path(__file__).parent.parent.parent / "output_csvs"
DB_PATH = CSV_DIR / "brasileirao.db"


# ==============================================================================
# 1. UTILITARIOS
# ==============================================================================

def normalizar_data(serie: pd.Series) -> pd.Series:
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


def csv(nome: str) -> pd.DataFrame:
    path = CSV_DIR / nome
    if not path.exists():
        print(f"  ⚠  {nome} nao encontrado, pulando.")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.where(df != "", other=None)
    print(f"  ✓  {nome}: {len(df)} linhas, colunas: {list(df.columns)}")
    return df


def nullar_local_id_orfao(df: pd.DataFrame, ids_locais: set) -> pd.DataFrame:
    """
    Substitui local_id que nao existe em locais por None (NULL no SQLite).
    Evita FK violation sem precisar desligar FK globalmente.
    """
    if "local_id" not in df.columns:
        return df
    df = df.copy()

    def _fix(val):
        if val is None:
            return None
        try:
            v = int(float(val))
            return v if v in ids_locais else None
        except (ValueError, TypeError):
            return None

    antes = df["local_id"].notna().sum()
    df["local_id"] = df["local_id"].apply(_fix)
    depois = df["local_id"].notna().sum()
    if antes != depois:
        print(f"  ⚠  {int(antes - depois)} local_id(s) orfaos convertidos para NULL")
    return df


# ==============================================================================
# 2. SCHEMA
# ==============================================================================

SCHEMA = """
PRAGMA journal_mode=WAL;

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
    print(f"    -> {tabela}: {conn.total_changes} insercoes acumuladas")


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
    df2["id"]         = df2["id"].apply(safe_int)
    df2["capacidade"] = df2["capacidade"].apply(safe_int)
    df2["local_id"]   = df2["local_id"].apply(safe_int)
    df2["inauguracao"] = df2["inauguracao"].apply(safe_int)
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
    df2 = df2.drop_duplicates(subset=["partida_id", "treinador_id", "clube_id"])
    sql = "INSERT OR IGNORE INTO treinadores_em_partida (partida_id, treinador_id, clube_id, tipo) VALUES (?, ?, ?, ?)"
    rows = [(r.partida_id, r.treinador_id, r.clube_id, r.tipo) for r in df2.itertuples(index=False)]
    conn.executemany(sql, rows)


def carregar_arbitros_em_partida(conn, df):
    if df.empty:
        return
    df2 = df.copy()
    df2["partida_id"] = df2["partida_id"].apply(safe_int)
    df2["arbitro_id"] = df2["arbitro_id"].apply(safe_int)
    df2 = df2.drop_duplicates(subset=["partida_id", "arbitro_id"])
    sql = "INSERT OR IGNORE INTO arbitros_em_partida (partida_id, arbitro_id) VALUES (?, ?)"
    rows = [(r.partida_id, r.arbitro_id) for r in df2.itertuples(index=False)]
    conn.executemany(sql, rows)


def carregar_eventos_partida(conn, df):
    if df.empty:
        print("  ⚠  eventos_partida.csv vazio, pulando.")
        return
    df2 = df.copy()
    for col in ["id", "partida_id", "jogador_id", "clube_id"]:
        df2[col] = df2[col].apply(safe_int)
    cols = ["id", "partida_id", "jogador_id", "clube_id", "tipo_evento", "tipo_gol", "minuto"]
    inserir(conn, "eventos_partida", df2, cols)


# ==============================================================================
# 4. MAIN
# ==============================================================================

def main():
    print(f"\n CSV_DIR : {CSV_DIR}")
    print(f" DB_PATH : {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)

    # CORRECAO: PRAGMA foreign_keys=OFF precisa estar dentro do executescript.
    # conn.execute() isolado nao tem efeito quando executescript() abre sua
    # propria transacao logo depois. Apos criar o schema, ligamos FK=ON
    # explicitamente e usamos nullar_local_id_orfao() para garantir integridade
    # sem depender do PRAGMA estar desligado durante os INSERTs.
    conn.executescript("PRAGMA foreign_keys=OFF;\n" + SCHEMA)
    conn.commit()

    # FK ON para todos os INSERTs a seguir
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()
    print("✅ Schema criado/validado\n")

    # Le locais primeiro para montar conjunto de IDs validos
    df_locais = csv("locais.csv")
    ids_locais = set()
    if not df_locais.empty:
        ids_locais = set(
            df_locais.rename(columns=str.lower)["id"]
            .dropna()
            .apply(safe_int)
            .dropna()
            .astype(int)
        )

    # (nome_csv, funcao, precisa_sanitizar_local_id)
    etapas = [
        ("locais.csv",                carregar_locais,                False),
        ("campeonatos.csv",           carregar_campeonatos,           False),
        ("jogadores.csv",             carregar_jogadores,             False),
        ("treinadores.csv",           carregar_treinadores,           False),
        ("arbitros.csv",              carregar_arbitros,              False),
        ("estadios.csv",              carregar_estadios,              True),
        ("clubes.csv",                carregar_clubes,                True),
        ("edicoes.csv",               carregar_edicoes,               False),
        ("partidas.csv",              carregar_partidas,              False),
        ("jogadores_em_partida.csv",  carregar_jogadores_em_partida,  False),
        ("treinadores_em_partida.csv",carregar_treinadores_em_partida,False),
        ("arbitros_em_partida.csv",   carregar_arbitros_em_partida,   False),
        ("eventos_partida.csv",       carregar_eventos_partida,       False),
    ]

    for nome_csv, func, sanitizar in etapas:
        print(f"-- {nome_csv}")
        df = df_locais if nome_csv == "locais.csv" else csv(nome_csv)

        if sanitizar and not df.empty:
            df = nullar_local_id_orfao(df, ids_locais)

        func(conn, df)
        conn.commit()
        print()

    resultado = conn.execute("PRAGMA integrity_check").fetchone()
    print(f"\n Integrity check: {resultado[0]}")

    tabelas = [
        "locais", "campeonatos", "jogadores", "treinadores", "arbitros",
        "estadios", "clubes", "edicoes", "partidas",
        "jogadores_em_partida", "treinadores_em_partida",
        "arbitros_em_partida", "eventos_partida"
    ]
    print("\n Registros por tabela:")
    for t in tabelas:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   {t:<30} {n:>8,}")

    conn.close()
    print(f"\n✅ Banco criado em: {DB_PATH}\n")


if __name__ == "__main__":
    main()
