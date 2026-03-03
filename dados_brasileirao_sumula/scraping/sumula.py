import re
import json
from pathlib import Path

import pdfplumber


# -------------------------------------------------
# Utilitários
# -------------------------------------------------

def limpar_texto(texto: str) -> str:
    texto = texto.replace("\r", "\n")
    texto = re.sub(r"[ \t]+", " ", texto)
    return texto


# -------------------------------------------------
# Extração de informações gerais
# -------------------------------------------------

def extrair_info_geral(texto: str, mandante_hint=None, visitante_hint=None) -> dict:
    info = {}

    # Campeonato / Rodada
    m_campeonato = re.search(
        r"Campeonato:\s*(.+?)\s+Rodada:\s*([0-9]+)", texto
    )
    if m_campeonato:
        info["campeonato"] = m_campeonato.group(1).strip()
        info["rodada"] = m_campeonato.group(2).strip()

    # Descrição do jogo (linha com "Time X Time")
    jogo_descricao = None
    for m in re.finditer(r"Jogo:\s*(.+)", texto):
        candidato = m.group(1).strip()
        if " X " in candidato:
            jogo_descricao = candidato
            break

    if jogo_descricao:
        info["jogo_descricao"] = jogo_descricao
        if " X " in jogo_descricao:
            mand, vis = jogo_descricao.split(" X ", 1)
            info.setdefault("mandante", mand.strip())
            info.setdefault("visitante", vis.strip())
    else:
        # fallback
        m_jogo = re.search(r"Jogo:\s*(.+?)\n", texto)
        if m_jogo:
            info["jogo_descricao"] = m_jogo.group(1).strip()

    # Sobrepor com nomes vindos da tabela (se existirem)
    if mandante_hint:
        info["mandante"] = mandante_hint
    if visitante_hint:
        info["visitante"] = visitante_hint

    # Data / Horário / Estádio
    m_data = re.search(
        r"Data:\s*([0-9/]+)\s+Horário:\s*([0-9:]+)\s+Estádio:\s*(.+)",
        texto,
    )
    if m_data:
        info["data"] = m_data.group(1).strip()
        info["horario"] = m_data.group(2).strip()
        info["estadio"] = m_data.group(3).strip()

    # Número do jogo (primeiro "Jogo: 371" do cabeçalho)
    m_num = re.search(r"Jogo:\s*([0-9]+)", texto)
    if m_num:
        info["numero_jogo"] = m_num.group(1).strip()

    return info


# -------------------------------------------------
# Extração de jogadores (usando TABELA do PDF)
# -------------------------------------------------

def extrair_jogadores(pdf):
    """
    Usa pdfplumber.extract_tables() para pegar a tabela
    "Relação de Jogadores", que já vem separada em 2 times.
    """
    for page in pdf.pages:
        tables = page.extract_tables()
        for tbl in tables:
            if not tbl or not tbl[0] or not tbl[0][0]:
                continue
            if "Relação de Jogadores" in str(tbl[0][0]):
                rows = tbl
                if len(rows) < 3:
                    continue

                header_times = rows[1]
                mandante = header_times[0]
                visitante = header_times[6] if len(header_times) > 6 else None

                jogadores = []

                # Linhas de dados começam em rows[3]
                for row in rows[3:]:
                    # Linha de legenda ("T = Titular | R = Reserva | ...")
                    if row[0] and isinstance(row[0], str) and row[0].startswith("T ="):
                        break

                    # Garante 12 colunas (6 por time)
                    row = (row + [None] * 12)[:12]
                    left = row[:6]
                    right = row[6:12]

                    # Lado esquerdo = mandante
                    if left[0]:
                        jogadores.append({
                            "time": mandante,
                            "numero": left[0],
                            "apelido": left[1],
                            "nome_completo": left[2],
                            "tr": left[3],   # T/R, T(g), etc.
                            "pa": left[4],   # P/A
                            "cbf": left[5],
                        })

                    # Lado direito = visitante
                    if right[0]:
                        jogadores.append({
                            "time": visitante,
                            "numero": right[0],
                            "apelido": right[1],
                            "nome_completo": right[2],
                            "tr": right[3],
                            "pa": right[4],
                            "cbf": right[5],
                        })

                return jogadores, mandante, visitante

    return [], None, None


# -------------------------------------------------
# Extração de eventos (gols, cartões, substituições)
# -------------------------------------------------

def extrair_gols(texto: str):
    if "Gols" not in texto:
        return []

    bloco = texto.split("Gols", 1)[1]
    if "Cartões Amarelos" in bloco:
        bloco = bloco.split("Cartões Amarelos", 1)[0]

    linhas = [l.strip() for l in bloco.splitlines() if l.strip()]
    gols = []

    for l in linhas:
        if l.startswith("Tempo") or l.startswith("NR ="):
            continue

        toks = l.split()
        if len(toks) < 6:
            continue

        tempo = toks[0]
        tempo_parte = toks[1]
        numero = toks[2]
        tipo = toks[3]
        time = toks[-1]          # último token é "Equipe/UF"
        jogador = " ".join(toks[4:-1])

        gols.append({
            "tempo": tempo,
            "tempo_parte": tempo_parte,
            "numero": numero,
            "tipo": tipo,        # NR, PN, etc.
            "jogador": jogador,
            "time": time,
        })

    return gols


def extrair_cartoes_amarelos(texto: str):
    if "Cartões Amarelos" not in texto:
        return []

    bloco = texto.split("Cartões Amarelos", 1)[1]
    if "Cartões Vermelhos" in bloco:
        bloco = bloco.split("Cartões Vermelhos", 1)[0]

    linhas = [l.strip() for l in bloco.splitlines() if l.strip()]
    amarelos = []

    for l in linhas:
        if l.startswith("Tempo") or l.startswith("Motivo:"):
            continue

        toks = l.split()
        if len(toks) < 6:
            continue

        tempo = toks[0]
        tempo_parte = toks[1]
        numero = toks[2]
        time = toks[-1]
        jogador = " ".join(toks[3:-1])

        amarelos.append({
            "tempo": tempo,
            "tempo_parte": tempo_parte,
            "numero": numero,
            "jogador": jogador,
            "time": time,
        })

    return amarelos


def extrair_cartoes_vermelhos(texto: str):
    if "Cartões Vermelhos" not in texto:
        return []

    bloco = texto.split("Cartões Vermelhos", 1)[1]
    if "Ocorrências / Observações" in bloco:
        bloco = bloco.split("Ocorrências / Observações", 1)[0]

    linhas = [l.strip() for l in bloco.splitlines() if l.strip()]
    vermelhos = []

    for l in linhas:
        if (
            l.startswith("Tempo")
            or l.startswith("Cartão Vermelho")
            or l.startswith("Motivo:")
        ):
            continue

        if " - " not in l:
            continue

        parte_jogador, time = l.rsplit(" - ", 1)
        toks = parte_jogador.split()
        if len(toks) < 4:
            continue

        tempo = toks[0]
        tempo_parte = toks[1]
        numero = toks[2]
        jogador = " ".join(toks[3:])

        vermelhos.append({
            "tempo": tempo,
            "tempo_parte": tempo_parte,
            "numero": numero,
            "jogador": jogador,
            "time": time,
        })

    return vermelhos


def extrair_substituicoes(texto: str):
    if "Substituições" not in texto:
        return []

    bloco = texto.split("Substituições", 1)[1]
    if "Confederação Brasileira de Futebol" in bloco:
        bloco = bloco.split("Confederação Brasileira de Futebol", 1)[0]

    linhas = [l.strip() for l in bloco.splitlines() if l.strip()]
    subs = []

    for l in linhas:
        if l.startswith("Tempo"):
            continue

        toks = l.split()
        if len(toks) < 7:
            continue

        # Formatos:
        # "- INT Equipe 10 - Nome 19 - Nome"
        # "22:00 2T Equipe 11 - Nome 21 - Nome"
        if toks[0] in ("-", "+"):
            tempo = toks[0]
            tempo_parte = toks[1] if len(toks) > 1 else ""
            idx_eq = 2
        else:
            tempo = toks[0]
            tempo_parte = toks[1]
            idx_eq = 2

        time = toks[idx_eq]
        resto = toks[idx_eq + 1:]

        if "-" not in resto:
            continue

        # Procura a segunda ocorrência "nº -"
        idx_num2 = None
        for i in range(len(resto)):
            if resto[i].isdigit() and i + 1 < len(resto) and resto[i + 1] == "-":
                if idx_num2 is None:
                    idx_num2 = i  # primeira ocorrência
                elif i != 0:
                    idx_num2 = i  # segunda, que é a saída
                    break

        if idx_num2 is None:
            continue

        numero_entrou = resto[0]
        jogador_entrou = " ".join(resto[2:idx_num2])
        numero_saiu = resto[idx_num2]
        jogador_saiu = " ".join(resto[idx_num2 + 2:])

        subs.append({
            "tempo": tempo,
            "tempo_parte": tempo_parte,
            "time": time,
            "numero_entrou": numero_entrou,
            "jogador_entrou": jogador_entrou,
            "numero_saiu": numero_saiu,
            "jogador_saiu": jogador_saiu,
        })

    return subs


# -------------------------------------------------
# Pipeline de uma súmula
# -------------------------------------------------

def parse_sumula(pdf_path: Path) -> dict:
    with pdfplumber.open(str(pdf_path)) as pdf:
        texto = "\n".join(page.extract_text() or "" for page in pdf.pages)
        jogadores, mand, vis = extrair_jogadores(pdf)

    texto = limpar_texto(texto)
    info = extrair_info_geral(texto, mand, vis)

    dados = {
        "arquivo": pdf_path.name,
        "info": info,
        "jogadores": jogadores,
        "gols": extrair_gols(texto),
        "cartoes_amarelos": extrair_cartoes_amarelos(texto),
        "cartoes_vermelhos": extrair_cartoes_vermelhos(texto),
        "substituicoes": extrair_substituicoes(texto),
    }

    return dados


# -------------------------------------------------
# Main: percorre pasta sumulas/ e gera JSONs
# -------------------------------------------------

def main():
    pasta_sumulas = Path("sumulas")
    pasta_saida = Path("saida")
    pasta_saida.mkdir(exist_ok=True)

    pdfs = sorted(pasta_sumulas.glob("*.pdf"))

    if not pdfs:
        print("Nenhuma súmula encontrada na pasta 'sumulas'.")
        return

    for pdf in pdfs:
        print(f"Processando {pdf.name}...")
        dados = parse_sumula(pdf)

        num_jogo = dados["info"].get("numero_jogo")
        if num_jogo:
            nome_saida = f"jogo_{num_jogo}.json"
        else:
            nome_saida = pdf.stem + ".json"

        caminho_saida = pasta_saida / nome_saida
        with open(caminho_saida, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)

    print("Concluído. JSONs gerados na pasta 'saida'.")


if __name__ == "__main__":
    main()
import json
import csv
from pathlib import Path


def carregar_jsons(pasta_json: Path):
    arquivos = sorted(pasta_json.glob("*.json"))
    dados = []

    for arq in arquivos:
        with open(arq, "r", encoding="utf-8") as f:
            dados.append(json.load(f))
    return dados


def escrever_csv(caminho, cabecalho, linhas):
    caminho.parent.mkdir(exist_ok=True)
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cabecalho)
        writer.writeheader()
        writer.writerows(linhas)


def main():
    pasta_json = Path("saida")
    pasta_csv = Path("csv")
    pasta_csv.mkdir(exist_ok=True)

    print("Carregando JSONs...")
    sumulas = carregar_jsons(pasta_json)

    jogos = []
    jogadores = []
    gols = []
    amarelos = []
    vermelhos = []
    subs = []

    for s in sumulas:
        info = s["info"]
        id_jogo = info.get("numero_jogo") or s["arquivo"]

        # -----------------------
        # JOGOS
        # -----------------------
        jogos.append({
            "id_jogo": id_jogo,
            "arquivo": s["arquivo"],
            "campeonato": info.get("campeonato"),
            "rodada": info.get("rodada"),
            "mandante": info.get("mandante"),
            "visitante": info.get("visitante"),
            "data": info.get("data"),
            "horario": info.get("horario"),
            "estadio": info.get("estadio"),
        })

        # -----------------------
        # JOGADORES
        # -----------------------
        for j in s["jogadores"]:
            jogadores.append({
                "id_jogo": id_jogo,
                "time": j.get("time"),
                "numero": j.get("numero"),
                "apelido": j.get("apelido"),
                "nome_completo": j.get("nome_completo"),
                "tr": j.get("tr"),
                "pa": j.get("pa"),
                "cbf": j.get("cbf"),
            })

        # -----------------------
        # GOLS
        # -----------------------
        for g in s["gols"]:
            g["id_jogo"] = id_jogo
            gols.append(g)

        # -----------------------
        # CARTÕES AMARELOS
        # -----------------------
        for a in s["cartoes_amarelos"]:
            a["id_jogo"] = id_jogo
            amarelos.append(a)

        # -----------------------
        # CARTÕES VERMELHOS
        # -----------------------
        for v in s["cartoes_vermelhos"]:
            v["id_jogo"] = id_jogo
            vermelhos.append(v)

        # -----------------------
        # SUBSTITUIÇÕES
        # -----------------------
        for sub in s["substituicoes"]:
            sub["id_jogo"] = id_jogo
            subs.append(sub)

    print("Gerando CSVs...")

    # Exportar tudo para CSV
    escrever_csv(
        pasta_csv / "jogos.csv",
        ["id_jogo", "arquivo", "campeonato", "rodada", "mandante", "visitante", "data", "horario", "estadio"],
        jogos,
    )

    escrever_csv(
        pasta_csv / "jogadores.csv",
        ["id_jogo", "time", "numero", "apelido", "nome_completo", "tr", "pa", "cbf"],
        jogadores,
    )

    escrever_csv(
        pasta_csv / "gols.csv",
        ["id_jogo", "tempo", "tempo_parte", "numero", "tipo", "jogador", "time"],
        gols,
    )

    escrever_csv(
        pasta_csv / "cartoes_amarelos.csv",
        ["id_jogo", "tempo", "tempo_parte", "numero", "jogador", "time"],
        amarelos,
    )

    escrever_csv(
        pasta_csv / "cartoes_vermelhos.csv",
        ["id_jogo", "tempo", "tempo_parte", "numero", "jogador", "time"],
        vermelhos,
    )

    escrever_csv(
        pasta_csv / "substituicoes.csv",
        ["id_jogo", "tempo", "tempo_parte", "time", "numero_entrou", "jogador_entrou", "numero_saiu", "jogador_saiu"],
        subs,
    )

    print("\n✔ Conversão concluída!")
    print("CSV gerados em: pasta 'csv/'")


if __name__ == "__main__":
    main()
