import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin

class OGolScraperModular:
    def __init__(self, url_lista):
        self.url_lista = url_lista
        self.base_url = "https://www.ogol.com.br"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        self.delay = 2

    # ==========================
    # FUNÇÕES BASE
    # ==========================
    def _get_soup(self, url):
        """Faz requisição e retorna BeautifulSoup"""
        time.sleep(self.delay)
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    def _extrair_link(self, celula):
        """Extrai texto e href de uma célula"""
        tag = celula.find("a")
        texto = tag.get_text(strip=True) if tag else celula.get_text(strip=True)
        link = urljoin(self.base_url, tag["href"]) if tag and "href" in tag.attrs else None
        return texto, link

    # ==========================
    # EXTRAÇÃO PRINCIPAL
    # ==========================
    def ler_lista_partidas(self):
        """Lê a tabela principal e retorna uma lista de partidas com os links"""
        print(f"🔍 Lendo tabela de partidas: {self.url_lista}")
        soup = self._get_soup(self.url_lista)

        tabela = soup.find("table", class_="zztable stats")
        if not tabela:
            print("❌ Tabela não encontrada.")
            return []

        partidas = []
        for linha in tabela.find_all("tr"):
            celulas = linha.find_all("td")
            if len(celulas) < 6:
                continue

            data = celulas[1].get_text(strip=True)
            hora = celulas[2].get_text(strip=True)
            mandante, link_mandante = self._extrair_link(celulas[3])
            placar, link_partida = self._extrair_link(celulas[5])
            visitante, link_visitante = self._extrair_link(celulas[7])
            fase = celulas[8].get_text(strip=True) if len(celulas) > 8 else ""

            partidas.append({
                "data": data,
                "hora": hora,
                "mandante": mandante,
                "visitante": visitante,
                "placar": placar,
                "fase": fase,
                "link_mandante": link_mandante,
                "link_partida": link_partida,
                "link_visitante": link_visitante
            })

        print(f"✅ {len(partidas)} partidas encontradas.")
        return partidas

    # ==========================
    # FUNÇÕES MODULARES DE LINK
    # ==========================
    def ler_link_mandante(self, url_mandante):
        """Lê o link do mandante e extrai informações específicas"""
        print(f"🏠 Lendo mandante: {url_mandante}")
        soup = self._get_soup(url_mandante)

        div_principal = soup.find("div", class_="zz-tpl-rb")
        dados = {}

        if div_principal:
            print("   ➤ Div principal encontrada. Agora procurando divs internas...")

            # Exemplo: buscar todas as divs internas (filhas)
            divs_internas = div_principal.find_all("div", class_="rbbox nofooter")
            for i, div in enumerate(divs_internas, start=1):
                texto = div.get_text(strip=True)
                if texto:
                    dados[f"mandante_div_{i}"] = texto

        else:
            print("   ⚠ Nenhuma div principal encontrada no mandante.")

        return dados

    def ler_link_partida(self, url_partida):
        """Lê o link da partida (placar) e extrai múltiplas divs e suas filhas"""
        print(f"⚽ Lendo detalhes da partida: {url_partida}")
        soup = self._get_soup(url_partida)

        dados = {}

        # Exemplo 1: buscar várias divs com uma classe específica
        divs_info = soup.find_all("div", class_="info")
        for idx, div in enumerate(divs_info, start=1):
            texto_div = div.get_text(strip=True)
            dados[f"partida_info_{idx}"] = texto_div

            # Exemplo 2: dentro dessa div, buscar outras divs filhas específicas
            divs_filhas = div.find_all("div", recursive=True)
            for j, filha in enumerate(divs_filhas, start=1):
                texto_filha = filha.get_text(strip=True)
                if texto_filha:
                    dados[f"partida_div_{idx}_filha_{j}"] = texto_filha

        if not dados:
            print("   ⚠ Nenhuma div encontrada no link da partida.")
        else:
            print(f"   ➤ {len(dados)} itens de div extraídos do link da partida.")

        return dados

    def ler_link_visitante(self, url_visitante):
        """Lê o link do visitante e extrai informações específicas"""
        print(f"🛫 Lendo visitante: {url_visitante}")
        soup = self._get_soup(url_visitante)

        div_principal = soup.find("div", class_="zz-tpl-rb")
        dados = {}

        if div_principal:
            print("   ➤ Div principal encontrada no visitante. Buscando internas...")
            for i, div in enumerate(div_principal.find_all("div", recursive=True), start=1):
                texto = div.get_text(strip=True)
                if texto:
                    dados[f"visitante_div_{i}"] = texto
        else:
            print("   ⚠ Nenhuma div principal encontrada no visitante.")

        return dados

    # ==========================
    # EXECUÇÃO GERAL
    # ==========================
    def executar(self):
        partidas = self.ler_lista_partidas()
        resultados = []

        for p in partidas:
            resultado = p.copy()

            if p["link_mandante"]:
                resultado.update(self.ler_link_mandante(p["link_mandante"]))
            if p["link_partida"]:
                resultado.update(self.ler_link_partida(p["link_partida"]))
            if p["link_visitante"]:
                resultado.update(self.ler_link_visitante(p["link_visitante"]))

            resultados.append(resultado)

        self.salvar_csv(resultados)

    def salvar_csv(self, dados):
        if not dados:
            return
        campos = sorted({k for d in dados for k in d})
        with open("resultado_links_detalhado.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(dados)
        print("💾 CSV salvo: resultado_links_detalhado.csv")


# ==========================
# USO
# ==========================
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperModular(url)
    scraper.executar()
