import os
from pathlib import Path

import requests


def baixar_sumula(ano: int, codigo: int, sufixo: str = "se", pasta_destino: Path = Path("sumulas\2013")):
    """
    Baixa uma única súmula dado o ano e o código (ex.: 142376),
    gerando a URL no formato:
    https://conteudo.cbf.com.br/sumulas/{ano}/{codigo}{sufixo}.pdf
    """
    pasta_destino.mkdir(parents=True, exist_ok=True)

    nome_arquivo = f"{codigo}{sufixo}.pdf"
    url = f"https://conteudo.cbf.com.br/sumulas/{ano}/{nome_arquivo}"
    caminho_arquivo = pasta_destino / nome_arquivo

    print(f"Tentando baixar {url} ...")

    try:
        resp = requests.get(url, timeout=15)
    except Exception as e:
        print(f"  Erro na requisição: {e}")
        return False

    if resp.status_code == 200 and resp.content.startswith(b"%PDF"):
        with open(caminho_arquivo, "wb") as f:
            f.write(resp.content)
        print(f"  OK! Salvo em: {caminho_arquivo}")
        return True
    else:
        print(f"  Não encontrado ({resp.status_code})")
        return False


def baixar_intervalo(ano: int, inicio: int, fim: int, sufixo: str = "se"):
    """
    Baixa todas as súmulas do intervalo [inicio, fim],
    ex.: inicio=142371, fim=142400.
    """
    pasta_destino = Path("sumulas")
    pasta_destino.mkdir(exist_ok=True)

    for codigo in range(inicio, fim + 1):
        baixar_sumula(ano, codigo, sufixo, pasta_destino)


if __name__ == "__main__":
    # >>> AJUSTE AQUI OS PARÂMETROS QUE VOCÊ QUISER <<<

    ANO = 2013
    INICIO = 1421     # primeiro código que você quer testar
    FIM = 1422       # último código do intervalo
    SUFIXO = "se"        # em Série A está vindo "se" (pode existir "sb" etc., se precisar)

    baixar_intervalo(ANO, INICIO, FIM, SUFIXO)
