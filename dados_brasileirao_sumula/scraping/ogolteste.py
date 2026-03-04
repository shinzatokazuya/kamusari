import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

BASE_URL = "https://www.ogol.com.br/edicao/copa-libertadores-2025/193713/estatisticas"
MAX_PAGES = 13   # <<< limite aqui

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def extrair_tabela(html: str) -> pd.DataFrame | None:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return None

    df = pd.read_html(StringIO(str(table)), decimal=",", thousands=".")[0]
    if df.shape[0] == 0:
        return None
    return df


def baixar_pagina(page: int) -> pd.DataFrame | None:
    params = {
        "v": "jt1",
        "ord": "d",
    }

    # página 1 não precisa do parâmetro
    if page > 1:
        params["page"] = page  # ajuste para 'pag' se necessário

    print(f"Baixando página {page}...")
    resp = requests.get(BASE_URL, headers=headers, params=params)
    resp.raise_for_status()

    df = extrair_tabela(resp.text)
    if df is None:
        print(f"  -> Nenhuma tabela na página {page}.")
    else:
        print(f"  -> {df.shape[0]} linhas")

    return df


def main():
    todos = []

    for page in range(1, MAX_PAGES + 1):
        df = baixar_pagina(page)

        if df is None:
            print("  -> Página vazia. Encerrando antes do limite.")
            break

        todos.append(df)
        time.sleep(1)

    if not todos:
        print("Nenhuma tabela capturada.")
        return

    df_final = pd.concat(todos, ignore_index=True)
    df_final.columns = [c.strip() for c in df_final.columns]

    df_final.to_csv("ogol_libertadores_2025_paginas_1_13.csv",
                    index=False,
                    encoding="utf-8-sig")

    print("\nArquivo gerado: ogol_libertadores_2025_paginas_1_13.csv")
    print(f"Total de linhas: {df_final.shape[0]}")


if __name__ == "__main__":
    main()
