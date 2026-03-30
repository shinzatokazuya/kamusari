"""
validador_cbf_csv.py
====================
Valida os dados extraídos do OGol comparando com as súmulas oficiais da CBF.
Trabalha inteiramente com arquivos CSV — sem SQLite.

FLUXO GERAL:
  1. Carrega todos os CSVs em memória (pandas DataFrames)
  2. Para cada súmula PDF em ./sumulas/, extrai dados com pdfplumber
  3. Localiza a partida correspondente nos CSVs por data + mandante + visitante
  4. Compara: placar, escalação, árbitro principal
  5. Gera relatório JSON + resumo no terminal

ESTRUTURA DE ARQUIVOS ESPERADA:
  ./sumulas/               ← PDFs das súmulas da CBF
  ./partidas.csv
  ./clubes.csv
  ./edicoes.csv
  ./jogadores.csv
  ./jogadores_em_partida.csv
  ./arbitros.csv
  ./arbitros_em_partida.csv
  ./relatorios_validacao/  ← criada automaticamente

DEPENDÊNCIAS:
  pip install pdfplumber pandas
"""

import re
import json
import unicodedata
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pdfplumber


# ──────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO — ajuste os caminhos se necessário
# ──────────────────────────────────────────────────────────────────

PASTA_SUMULAS = Path("sumulas/2013")
PASTA_CSV = Path("../novo_bd1971_robusto/output_csvs")          # pasta onde estão os CSVs
PASTA_RELATORIO = Path("relatorios_validacao")
PASTA_RELATORIO.mkdir(exist_ok=True)


# ──────────────────────────────────────────────────────────────────
# CARREGAMENTO DOS CSVs
# ──────────────────────────────────────────────────────────────────

def carregar_csvs() -> dict:
    """
    Carrega todos os CSVs relevantes em um dicionário de DataFrames.

    Usamos low_memory=False para evitar avisos de tipo em colunas mistas.
    Os IDs são mantidos como inteiros onde possível para facilitar os JOINs
    que faremos manualmente com merge().
    """
    print("📂 Carregando CSVs...")
    dados = {}

    arquivos = {
        "partidas":                 "partidas.csv",
        "clubes":                   "clubes.csv",
        "edicoes":                  "edicoes.csv",
        "jogadores":                "jogadores.csv",
        "jogadores_em_partida":     "jogadores_em_partida.csv",
        "arbitros":                 "arbitros.csv",
        "arbitros_em_partida":      "arbitros_em_partida.csv",
        "treinadores":              "treinadores.csv",
        "treinadores_em_partida":   "treinadores_em_partida.csv",
    }

    for chave, nome_arquivo in arquivos.items():
        caminho = PASTA_CSV / nome_arquivo
        if not caminho.exists():
            print(f"   ⚠️  {nome_arquivo} não encontrado — pulando")
            dados[chave] = pd.DataFrame()
            continue
        dados[chave] = pd.read_csv(caminho, low_memory=False)
        print(f"   ✓ {nome_arquivo}: {len(dados[chave])} linhas")

    # Padroniza nome da coluna de ID nas tabelas que usam "ID" maiúsculo
    # (edicoes e algumas outras usam "ID", as demais usam "id")
    for chave in ["edicoes", "clubes", "jogadores", "arbitros", "treinadores"]:
        df = dados[chave]
        if "ID" in df.columns and "id" not in df.columns:
            dados[chave] = df.rename(columns={"ID": "id"})

    # Constrói dois índices rápidos que vamos usar com frequência:
    # clube_por_id: {1: "Esporte Clube Bahia", 2: "Santos Futebol Clube", ...}
    # jogador_por_id: {1: {"nome": "...", "apelido": "..."}, ...}
    dados["clube_por_id"] = (
        dados["clubes"].set_index("id")["clube"].to_dict()
        if not dados["clubes"].empty else {}
    )

    dados["jogador_por_id"] = (
        dados["jogadores"].set_index(
            "id")[["nome", "apelido"]].to_dict("index")
        if not dados["jogadores"].empty else {}
    )

    dados["arbitro_por_id"] = (
        dados["arbitros"].set_index("id")[["nome", "apelido"]].to_dict("index")
        if not dados["arbitros"].empty else {}
    )

    # Mapa edicao_id -> ano (para filtrar só os anos que têm súmulas)
    if not dados["edicoes"].empty:
        dados["edicao_para_ano"] = (
            dados["edicoes"].set_index("id")["ano"].to_dict()
        )
    else:
        dados["edicao_para_ano"] = {}

    print(f"   ✓ Índices criados\n")
    return dados


# ──────────────────────────────────────────────────────────────────
# UTILITÁRIOS DE NORMALIZAÇÃO
# ──────────────────────────────────────────────────────────────────

def normalizar(texto) -> str:
    """
    Remove acentos, converte para minúsculas e colapsa espaços.

    Isso é essencial porque o OGol e a CBF frequentemente grafam
    o mesmo nome de forma diferente. Exemplos reais:
      CBF: "FLUMINENSE FOOTBALL CLUB"  →  normalizado: "fluminense football club"
      OGol: "Fluminense"               →  normalizado: "fluminense"
    A comparação ainda pode falhar (um tem "football club", o outro não),
    mas reduz drasticamente os falsos negativos por acentuação ou capitalização.
    """
    if not texto or (isinstance(texto, float)):
        return ""
    texto = str(texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto).strip().lower()


def normalizar_data_cbf(data_str: str) -> Optional[str]:
    """
    Converte data no formato DD/MM/YYYY (padrão súmula CBF)
    para YYYY-MM-DD (padrão nos seus CSVs).
    Retorna None se não conseguir parsear.
    """
    if not data_str:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", data_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return None


def contem_nome(nome_haystack: str, nome_agulha: str) -> bool:
    """
    Verifica se nome_agulha está contido em nome_haystack ou vice-versa,
    após normalização. Mínimo de 4 caracteres para evitar falsos positivos.

    Exemplo:
      contem_nome("atletico mineiro", "atletico") → True
      contem_nome("santos", "santos futebol clube") → True
      contem_nome("bahia", "parana") → False
    """
    a = normalizar(nome_haystack)
    b = normalizar(nome_agulha)
    if len(a) < 4 or len(b) < 4:
        return False
    return a in b or b in a


# ──────────────────────────────────────────────────────────────────
# EXTRAÇÃO DA SÚMULA PDF
# ──────────────────────────────────────────────────────────────────

def extrair_sumula(pdf_path: Path) -> dict:
    """
    Extrai dados estruturados de uma súmula PDF da CBF.

    A súmula tem duas fontes de informação:
    - Texto corrido: data, estádio, árbitros, gols, cartões
    - Tabela estruturada: relação de jogadores (mais confiável)

    Retornamos tudo num dicionário para facilitar a comparação.
    """
    with pdfplumber.open(str(pdf_path)) as pdf:
        texto_completo = "\n".join(
            page.extract_text() or "" for page in pdf.pages
        )
        texto_completo = re.sub(
            r"[ \t]+", " ", texto_completo.replace("\r", "\n"))

        # Jogadores via tabela (mais estruturado do que texto livre)
        jogadores, mandante_tabela, visitante_tabela = _extrair_jogadores_tabela(
            pdf)

    info = _extrair_info_geral(
        texto_completo, mandante_tabela, visitante_tabela)

    return {
        "arquivo":          pdf_path.name,
        "info":             info,
        "jogadores":        jogadores,
        "gols":             _extrair_gols(texto_completo),
        "cartoes_amarelos": _extrair_cartoes(texto_completo, "Cartões Amarelos", "Cartões Vermelhos"),
        "cartoes_vermelhos": _extrair_cartoes(texto_completo, "Cartões Vermelhos", "Ocorrências"),
        "arbitros":         _extrair_arbitros(texto_completo),
    }


def _extrair_info_geral(texto: str, mandante_hint=None, visitante_hint=None) -> dict:
    info = {}

    # Campeonato e rodada
    m = re.search(r"Campeonato:\s*(.+?)\s+Rodada:\s*([0-9]+)", texto)
    if m:
        info["campeonato"] = m.group(1).strip()
        info["rodada"] = m.group(2).strip()

    # Times via linha "Jogo: Time X Time"
    for m in re.finditer(r"Jogo:\s*(.+)", texto):
        cand = m.group(1).strip()
        if " X " in cand:
            mand, vis = cand.split(" X ", 1)
            info.setdefault("mandante", mand.strip())
            info.setdefault("visitante", vis.strip())
            break

    # A tabela de jogadores é mais confiável para o nome dos times
    if mandante_hint:
        info["mandante"] = mandante_hint
    if visitante_hint:
        info["visitante"] = visitante_hint

    # Data, horário e estádio
    m = re.search(
        r"Data:\s*([0-9/]+)\s+Horário:\s*([0-9:]+)\s+Estádio:\s*(.+)", texto
    )
    if m:
        info["data"] = m.group(1).strip()
        info["horario"] = m.group(2).strip()
        info["estadio"] = m.group(3).strip()

    # Placar — nem sempre está explícito; às vezes só contamos os gols depois
    m = re.search(r"Resultado[:\s]+(\d+)\s*[xX]\s*(\d+)", texto)
    if m:
        info["gols_mandante"] = int(m.group(1))
        info["gols_visitante"] = int(m.group(2))

    return info


def _extrair_jogadores_tabela(pdf) -> tuple:
    """
    Usa pdfplumber.extract_tables() para pegar a tabela
    "Relação de Jogadores" que já vem separada em 2 colunas (mandante | visitante).

    Retorna: (lista_jogadores, nome_mandante, nome_visitante)
    """
    for page in pdf.pages:
        for tbl in page.extract_tables():
            if not tbl or not tbl[0] or not tbl[0][0]:
                continue
            if "Relação de Jogadores" not in str(tbl[0][0]):
                continue

            header = tbl[1] if len(tbl) > 1 else []
            mandante = header[0] if header else None
            visitante = header[6] if len(header) > 6 else None

            jogadores = []
            for row in tbl[3:]:   # primeiras 3 linhas são cabeçalhos
                if row[0] and isinstance(row[0], str) and row[0].startswith("T ="):
                    break   # linha de legenda — fim dos jogadores

                row = (list(row) + [None] * 12)[:12]
                esq, dir_ = row[:6], row[6:12]

                if esq[0]:
                    jogadores.append({
                        "time":         mandante,
                        "numero":       str(esq[0] or "").strip(),
                        "apelido":      str(esq[1] or "").strip(),
                        "nome_completo": str(esq[2] or "").strip(),
                        "titular":      "T" in str(esq[3] or ""),
                    })
                if dir_[0]:
                    jogadores.append({
                        "time":         visitante,
                        "numero":       str(dir_[0] or "").strip(),
                        "apelido":      str(dir_[1] or "").strip(),
                        "nome_completo": str(dir_[2] or "").strip(),
                        "titular":      "T" in str(dir_[3] or ""),
                    })

            return jogadores, mandante, visitante

    return [], None, None


def _extrair_arbitros(texto: str) -> list:
    """
    Extrai árbitro principal e assistentes da súmula.
    A seção de arbitragem na súmula da CBF tem formato variado,
    então usamos vários padrões regex como fallback.
    """
    arbitros = []
    padroes = [
        (r"Árbitro[:\s]+([^\n]+)",           "Principal"),
        (r"1[oº]\s*Assistente[:\s]+([^\n]+)", "Assistente 1"),
        (r"2[oº]\s*Assistente[:\s]+([^\n]+)", "Assistente 2"),
        (r"4[oº]\s*Árbitro[:\s]+([^\n]+)",    "4º Árbitro"),
    ]
    for padrao, funcao in padroes:
        m = re.search(padrao, texto, re.IGNORECASE)
        if m:
            nome = m.group(1).strip()
            # Remove sufixo de UF tipo "(SP)" que aparece em alguns PDFs
            nome = re.sub(r"\s*\([A-Z]{2}\)\s*$", "", nome).strip()
            if nome:
                arbitros.append({"nome": nome, "funcao": funcao})
    return arbitros


def _extrair_gols(texto: str) -> list:
    if "Gols" not in texto:
        return []
    bloco = texto.split("Gols", 1)[1]
    if "Cartões Amarelos" in bloco:
        bloco = bloco.split("Cartões Amarelos", 1)[0]
    gols = []
    for linha in bloco.splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("Tempo") or linha.startswith("NR ="):
            continue
        toks = linha.split()
        if len(toks) >= 5:
            gols.append({
                "minuto":  toks[0],
                "tipo":    toks[3] if len(toks) > 3 else "",
                "jogador": " ".join(toks[4:-1]),
                "time":    toks[-1],
            })
    return gols


def _extrair_cartoes(texto: str, secao_inicio: str, secao_fim: str) -> list:
    if secao_inicio not in texto:
        return []
    bloco = texto.split(secao_inicio, 1)[1]
    if secao_fim in bloco:
        bloco = bloco.split(secao_fim, 1)[0]
    cartoes = []
    for linha in bloco.splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("Tempo") or linha.startswith("Motivo"):
            continue
        toks = linha.split()
        if len(toks) >= 4:
            cartoes.append({
                "minuto":  toks[0],
                "jogador": " ".join(toks[3:-1]),
                "time":    toks[-1],
            })
    return cartoes


# ──────────────────────────────────────────────────────────────────
# BUSCA NOS CSVs
# ──────────────────────────────────────────────────────────────────

def encontrar_partida_nos_csvs(
    dados: dict,
    data_iso: str,
    mandante_cbf: str,
    visitante_cbf: str
) -> Optional[dict]:
    """
    Busca nos CSVs a partida que corresponde aos dados da súmula.

    Estratégia em três passos (do mais restrito ao mais tolerante):

    Passo 1 — match exato por data + nome exato dos clubes.
    Passo 2 — match por data + contenção de nome (um nome contém o outro),
               útil para variações como "Atletico Mineiro" vs "Clube Atlético Mineiro".
    Passo 3 — se houver múltiplos resultados, retorna None e registra ambiguidade.

    Fazemos join manual entre partidas e clubes usando os IDs, exatamente
    como um JOIN SQL faria — mas em pandas.
    """
    df_partidas = dados["partidas"].copy()
    clube_por_id = dados["clube_por_id"]

    # Adiciona colunas com os nomes dos clubes (equivale ao JOIN com clubes.csv)
    df_partidas["mandante_nome"] = df_partidas["mandante_id"].map(clube_por_id)
    df_partidas["visitante_nome"] = df_partidas["visitante_id"].map(
        clube_por_id)

    # Filtra pela data primeiro (reduz muito o espaço de busca)
    df_data = df_partidas[df_partidas["data"] == data_iso]
    if df_data.empty:
        return None

    # ── Passo 1: nome exato (case-insensitive) ──
    mask_exato = (
        df_data["mandante_nome"].str.lower(
        ).str.strip() == mandante_cbf.lower().strip()
    ) & (
        df_data["visitante_nome"].str.lower(
        ).str.strip() == visitante_cbf.lower().strip()
    )
    resultado = df_data[mask_exato]

    # ── Passo 2: contenção de nome ──
    if len(resultado) != 1:
        mask_contem = df_data.apply(
            lambda row: (
                contem_nome(str(row["mandante_nome"]),  mandante_cbf) and
                contem_nome(str(row["visitante_nome"]), visitante_cbf)
            ),
            axis=1
        )
        resultado = df_data[mask_contem]

    if len(resultado) == 1:
        row = resultado.iloc[0]
        return {
            "partida_id":       int(row["id"]),
            "data":             row["data"],
            "mandante":         row["mandante_nome"],
            "visitante":        row["visitante_nome"],
            "mandante_id":      int(row["mandante_id"]),
            "visitante_id":     int(row["visitante_id"]),
            "mandante_placar":  row["mandante_placar"],
            "visitante_placar": row["visitante_placar"],
        }

    return None   # não encontrou ou há ambiguidade


def buscar_jogadores_da_partida(dados: dict, partida_id: int) -> list:
    """
    Retorna os jogadores registrados para uma partida nos CSVs,
    já com nome/apelido resolvidos via o índice jogador_por_id.
    """
    df = dados["jogadores_em_partida"]
    df_partida = df[df["partida_id"] == partida_id]

    resultado = []
    for _, row in df_partida.iterrows():
        jid = int(row["jogador_id"]) if not pd.isna(
            row["jogador_id"]) else None
        info_jogador = dados["jogador_por_id"].get(jid, {}) if jid else {}
        resultado.append({
            "jogador_id":   jid,
            "nome":         info_jogador.get("nome", ""),
            "apelido":      info_jogador.get("apelido", ""),
            "clube_id":     int(row["clube_id"]) if not pd.isna(row["clube_id"]) else None,
            "titular":      int(row["titular"]) if not pd.isna(row["titular"]) else 0,
            "numero_camisa": row.get("numero_camisa"),
        })
    return resultado


def buscar_arbitro_da_partida(dados: dict, partida_id: int) -> list:
    """
    Retorna os árbitros registrados para uma partida nos CSVs.
    """
    df = dados["arbitros_em_partida"]
    df_partida = df[df["partida_id"] == partida_id]

    resultado = []
    for _, row in df_partida.iterrows():
        aid = int(row["arbitro_id"]) if not pd.isna(
            row.get("arbitro_id", float("nan"))) else None
        if aid:
            info = dados["arbitro_por_id"].get(aid, {})
            resultado.append({
                "arbitro_id": aid,
                "nome":       info.get("nome", ""),
                "apelido":    info.get("apelido", ""),
            })
    return resultado


# ──────────────────────────────────────────────────────────────────
# COMPARAÇÕES
# ──────────────────────────────────────────────────────────────────

def comparar_placar(sumula: dict, partida_csv: dict) -> list:
    divergencias = []
    gols_m = sumula["info"].get("gols_mandante")
    gols_v = sumula["info"].get("gols_visitante")
    if gols_m is None or gols_v is None:
        return []   # placar não encontrado na súmula — não dá pra comparar

    placar_csv_m = partida_csv.get("mandante_placar")
    placar_csv_v = partida_csv.get("visitante_placar")

    try:
        if int(gols_m) != int(placar_csv_m) or int(gols_v) != int(placar_csv_v):
            divergencias.append({
                "tipo":  "PLACAR_DIVERGENTE",
                "cbf":   f"{gols_m} x {gols_v}",
                "banco": f"{placar_csv_m} x {placar_csv_v}",
            })
    except (TypeError, ValueError):
        pass   # placar nulo no CSV (jogo WO ou sem dados)

    return divergencias


def comparar_jogadores(sumula: dict, jogadores_csv: list) -> tuple:
    """
    Para cada jogador da súmula, tenta encontrar uma correspondência no CSV.

    A comparação é feita por apelido normalizado. Se o CSV tiver o apelido
    "Ronaldinho" e a súmula tiver "Ronaldo de Assis Moreira", a função
    contem_nome() vai capturar a correspondência porque "ronaldinho" contém
    "ronaldin" e há sobreposição. Mas casos como "Felipe" (CBF) vs "Luiz Felipe"
    (OGol) precisam de inspeção manual — por isso geramos o relatório detalhado.

    Retorna: (qtd_coincidentes, lista_divergencias)
    """
    # Indexa os jogadores do CSV por apelido e nome normalizados
    apelidos_csv = {normalizar(j["apelido"])
                    for j in jogadores_csv if j["apelido"]}
    nomes_csv = {normalizar(j["nome"]) for j in jogadores_csv if j["nome"]}

    coincidentes = 0
    divergencias = []

    for j_cbf in sumula.get("jogadores", []):
        apelido_cbf = normalizar(j_cbf.get("apelido", ""))
        nome_cbf = normalizar(j_cbf.get("nome_completo", ""))

        # Considera coincidente se:
        # (a) apelido exato bate, ou
        # (b) nome completo exato bate, ou
        # (c) há contenção entre apelidos (captura variações de nome)
        encontrado = (
            (apelido_cbf and apelido_cbf in apelidos_csv) or
            (nome_cbf and nome_cbf in nomes_csv) or
            any(contem_nome(a, apelido_cbf)
                for a in apelidos_csv if len(a) >= 4)
        )

        if encontrado:
            coincidentes += 1
        else:
            divergencias.append({
                "tipo":         "JOGADOR_CBF_NAO_ENCONTRADO_NO_CSV",
                "apelido_cbf":  j_cbf.get("apelido", ""),
                "nome_cbf":     j_cbf.get("nome_completo", ""),
                "time_cbf":     j_cbf.get("time", ""),
                "titular":      j_cbf.get("titular", False),
            })

    # Verifica o inverso: jogadores no CSV que não aparecem na súmula
    # (possível indicativo de erro de scraping — alguém foi adicionado errado)
    for j_csv in jogadores_csv:
        apelido_b = normalizar(j_csv.get("apelido", ""))
        nome_b = normalizar(j_csv.get("nome", ""))

        encontrado_na_cbf = any(
            normalizar(j.get("apelido", "")) == apelido_b or
            normalizar(j.get("nome_completo", "")) == nome_b
            for j in sumula.get("jogadores", [])
        )

        if not encontrado_na_cbf:
            divergencias.append({
                "tipo":          "JOGADOR_CSV_NAO_ESTA_NA_SUMULA",
                "nome_csv":      j_csv.get("nome", ""),
                "apelido_csv":   j_csv.get("apelido", ""),
            })

    return coincidentes, divergencias


def comparar_arbitro(sumula: dict, arbitros_csv: list) -> list:
    """
    Compara o árbitro principal da súmula com os árbitros no CSV.
    No seu banco, o OGol registra apenas um árbitro por partida.
    """
    principal = next(
        (a["nome"]
         for a in sumula.get("arbitros", []) if a["funcao"] == "Principal"),
        None
    )
    if not principal:
        return []

    # Considera válido se qualquer árbitro do CSV bate com o principal da súmula
    encontrado = any(
        contem_nome(a.get("nome", ""),   principal) or
        contem_nome(a.get("apelido", ""), principal)
        for a in arbitros_csv
    )

    if not encontrado:
        return [{
            "tipo":           "ARBITRO_DIVERGENTE",
            "arbitro_cbf":    principal,
            "arbitros_csv":   [a.get("nome") for a in arbitros_csv],
        }]
    return []


# ──────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ──────────────────────────────────────────────────────────────────

@dataclass
class ResultadoValidacao:
    arquivo_sumula:         str
    partida_id:             Optional[int] = None
    partida_encontrada:     bool = False
    divergencias_placar:    list = field(default_factory=list)
    divergencias_escalacao: list = field(default_factory=list)
    divergencias_arbitro:   list = field(default_factory=list)
    jogadores_cbf:          int = 0
    jogadores_csv:          int = 0
    jogadores_coincidentes: int = 0
    erro:                   str = ""

    @property
    def score_coincidencia(self) -> float:
        if self.jogadores_cbf == 0:
            return 0.0
        return self.jogadores_coincidentes / self.jogadores_cbf * 100


def validar_todas_sumulas(dados: dict) -> list:
    pdfs = sorted(PASTA_SUMULAS.glob("*.pdf"))
    if not pdfs:
        print(f"Nenhum PDF encontrado em {PASTA_SUMULAS}/")
        return []

    print(f"\n{'='*60}")
    print(f"Validando {len(pdfs)} súmulas contra os CSVs...")
    print(f"{'='*60}\n")

    resultados = []

    for pdf_path in pdfs:
        print(f"📄 {pdf_path.name}")
        resultado = ResultadoValidacao(arquivo_sumula=pdf_path.name)

        try:
            sumula = extrair_sumula(pdf_path)

            data_iso = normalizar_data_cbf(sumula["info"].get("data", ""))
            mandante = sumula["info"].get("mandante", "")
            visitante = sumula["info"].get("visitante", "")

            if not data_iso or not mandante or not visitante:
                resultado.erro = "Dados insuficientes na súmula (data ou nomes dos times)"
                print(f"   ⚠️  {resultado.erro}")
                resultados.append(resultado)
                continue

            partida = encontrar_partida_nos_csvs(
                dados, data_iso, mandante, visitante)

            if not partida:
                resultado.erro = f"Partida não encontrada: {mandante} x {visitante} em {data_iso}"
                print(f"   ❌ {resultado.erro}")
                resultados.append(resultado)
                continue

            resultado.partida_encontrada = True
            resultado.partida_id = partida["partida_id"]
            print(f"   ✅ Partida ID {resultado.partida_id}: "
                  f"{partida['mandante']} {partida['mandante_placar']} x "
                  f"{partida['visitante_placar']} {partida['visitante']}")

            # Placar
            resultado.divergencias_placar = comparar_placar(sumula, partida)

            # Jogadores
            jogadores_csv = buscar_jogadores_da_partida(
                dados, resultado.partida_id)
            resultado.jogadores_cbf = len(sumula.get("jogadores", []))
            resultado.jogadores_csv = len(jogadores_csv)
            coincidentes, diverg_jog = comparar_jogadores(
                sumula, jogadores_csv)
            resultado.jogadores_coincidentes = coincidentes
            resultado.divergencias_escalacao = diverg_jog

            # Árbitro
            arbitros_csv = buscar_arbitro_da_partida(
                dados, resultado.partida_id)
            resultado.divergencias_arbitro = comparar_arbitro(
                sumula, arbitros_csv)

            # Resumo desta partida
            print(f"   📊 Jogadores → CBF: {resultado.jogadores_cbf} | "
                  f"CSV: {resultado.jogadores_csv} | "
                  f"Coincidentes: {coincidentes} "
                  f"({resultado.score_coincidencia:.0f}%)")

            if resultado.divergencias_placar:
                for d in resultado.divergencias_placar:
                    print(f"   🚨 PLACAR: CBF={d['cbf']} vs CSV={d['banco']}")

            if resultado.divergencias_arbitro:
                for d in resultado.divergencias_arbitro:
                    print(
                        f"   🚨 ÁRBITRO: CBF={d['arbitro_cbf']} vs CSV={d['arbitros_csv']}")

        except Exception as e:
            resultado.erro = str(e)
            print(f"   💥 Erro: {e}")

        resultados.append(resultado)

    return resultados


def gerar_relatorio(resultados: list):
    total = len(resultados)
    encontradas = sum(1 for r in resultados if r.partida_encontrada)
    scores = [r.score_coincidencia for r in resultados if r.partida_encontrada]
    media = sum(scores) / len(scores) if scores else 0.0

    com_problema = [
        r for r in resultados
        if r.partida_encontrada and (
            r.divergencias_placar or
            r.divergencias_arbitro or
            r.score_coincidencia < 70
        )
    ]

    print(f"\n{'='*60}")
    print("📋 RELATÓRIO FINAL DE VALIDAÇÃO")
    print(f"{'='*60}")
    print(f"Súmulas processadas:           {total}")
    print(
        f"Partidas encontradas nos CSVs: {encontradas} ({encontradas/max(total, 1)*100:.0f}%)")
    print(f"Score médio de coincidência:   {media:.1f}%")
    print(f"Partidas com algum problema:   {len(com_problema)}")
    print(f"{'='*60}\n")

    # JSON detalhado
    relatorio = []
    for r in resultados:
        relatorio.append({
            "arquivo":                    r.arquivo_sumula,
            "partida_id":                 r.partida_id,
            "partida_encontrada":         r.partida_encontrada,
            "score_coincidencia_pct":     round(r.score_coincidencia, 1),
            "jogadores_cbf":              r.jogadores_cbf,
            "jogadores_csv":              r.jogadores_csv,
            "jogadores_coincidentes":     r.jogadores_coincidentes,
            "divergencias_placar":        r.divergencias_placar,
            "divergencias_arbitro":       r.divergencias_arbitro,
            "divergencias_escalacao":     r.divergencias_escalacao,
            "erro":                       r.erro,
        })

    caminho = PASTA_RELATORIO / "validacao_cbf.json"
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)
    print(f"Relatório detalhado salvo em: {caminho}")

    if com_problema:
        print("\n⚠️  PARTIDAS COM DIVERGÊNCIAS SIGNIFICATIVAS:")
        for r in com_problema:
            print(f"   • {r.arquivo_sumula} (ID {r.partida_id}) "
                  f"— score escalação: {r.score_coincidencia:.0f}%")


if __name__ == "__main__":
    dados = carregar_csvs()
    resultados = validar_todas_sumulas(dados)
    gerar_relatorio(resultados)
