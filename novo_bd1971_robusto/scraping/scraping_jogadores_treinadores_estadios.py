import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin

class OGolScraperModular:
    def __init__(self, url_lista):
        self.url_lista = url_lista
        self.base_url = "https://www.ogol.com.br"
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.delay = 2
        self.times_lidos = set()  # evitar repetição
        self.estadios_lidos = set()
        self.arbitros_lidos = set()

    def _get_soup(self, url):
        time.sleep(self.delay)
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    def _extrair_link(self, celula):
        tag = celula.find("a")
        texto = tag.get_text(strip=True) if tag else celula.get_text(strip=True)
        link = urljoin(self.base_url, tag["href"]) if tag and "href" in tag.attrs else None
        return texto, link

    # =====================================================
    # LISTA DE PARTIDAS
    # =====================================================
    def ler_lista_partidas(self):
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
        return partidas

    # =====================================================
    # MANDANTE / VISITANTE
    # =====================================================
    def ler_link_time(self, url_time, tipo):
        """Lê link do time (mandante ou visitante)"""
        if not url_time:
            return None
        if url_time in self.times_lidos:
            print(f"⏭ {tipo} já lido anteriormente, pulando.")
            return None
        self.times_lidos.add(url_time)

        print(f"🏟️ Lendo {tipo}: {url_time}")
        soup = self._get_soup(url_time)

        div_especifica = soup.find("div", id="entity_bio")
        if not div_especifica:
            print(f"⚠ Div 'entity_bio' não encontrada em {tipo}.")
            return None

        dados = {}
        spans = div_especifica.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            if "Nome" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["nome_completo"] = valor
            if "Apelidos" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["apelido"] = valor
            if "Ano de Fundação" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["fundacao"] = valor
            if "Cidade" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["cidade"] = valor
            if "Estado" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["estado"] = valor
        return dados

    # =====================================================
    # PARTIDA + LINKS INTERNOS (ESTÁDIO, ÁRBITRO)
    # =====================================================
    def ler_link_partida(self, url_partida):
        print(f"⚽ Lendo partida: {url_partida}")
        soup = self._get_soup(url_partida)
        div_especifica = soup.find("div", class_="header")
        if not div_especifica:
            print("⚠ Div 'header' não encontrada.")
            return None

        dados = {}
        for a_tag in div_especifica.find_all("a"):
            texto = a_tag.get_text(strip=True)
            link = urljoin(self.base_url, a_tag["href"]) if "href" in a_tag.attrs else None

            if "Estádio" in texto or "Estádio" in a_tag.parent.get_text():
                dados["estadio_nome"] = texto
                dados["link_estadio"] = link
                if link and link not in self.estadios_lidos:
                    self.estadios_lidos.add(link)
                    est = self.ler_link_estadio(link)
                    if est:
                        dados.update(est)

            elif "Árbitro" in texto or "Árbitro" in a_tag.parent.get_text():
                dados["arbitro_nome"] = texto
                dados["link_arbitro"] = link
                if link and link not in self.arbitros_lidos:
                    self.arbitros_lidos.add(link)
                    arb = self.ler_link_arbitro(link)
                    if arb:
                        dados.update(arb)

        return dados

    # =====================================================
    # ESTÁDIO / ÁRBITRO (EXTRAÇÃO ESPECÍFICA)
    # =====================================================
    def ler_link_estadio(self, url_estadio):
        print(f"🏟️ Lendo estádio: {url_estadio}")
        soup = self._get_soup(url_estadio)
        div = soup.find("div", id="entity_bio")
        if not div:
            return None

        dados = {"origem": "estadio"}
        spans = div.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            if "Nome" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["nome_completo_estadio"] = valor
            if "Cidade" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["cidade_estadio"] = valor
            if "Capacidade" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["capacidade"] = valor
        return dados

    def ler_link_arbitro(self, url_arbitro):
        print(f"👨‍⚖️ Lendo árbitro: {url_arbitro}")
        soup = self._get_soup(url_arbitro)
        div = soup.find("div", id="entity_bio")
        if not div:
            return None

        dados = {"origem": "arbitro"}
        spans = div.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            if "Nome" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["nome_completo_arbitro"] = valor
            if "Nacionalidade" in texto:
                valor = span.find_next("span").get_text(strip=True)
                dados["nacionalidade_arbitro"] = valor
        return dados

    # =====================================================
    # SALVAR CSVs
    # =====================================================
    def salvar_csv(self, nome, dados_lista):
        if not dados_lista:
            return
        campos = sorted({k for d in dados_lista for k in d})
        with open(nome, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(dados_lista)
        print(f"💾 CSV salvo: {nome}")

    # =====================================================
    # EXECUÇÃO
    # =====================================================
    def executar(self):
        partidas = self.ler_lista_partidas()
        dados_partidas, dados_times = [], []

        for p in partidas:
            print(f"\n=== Processando {p['mandante']} x {p['visitante']} ===")

            if p["link_partida"]:
                info_partida = self.ler_link_partida(p["link_partida"])
                if info_partida:
                    dados_partidas.append({"partida": p["placar"], **info_partida})

            for tipo, link in [("mandante", p["link_mandante"]), ("visitante", p["link_visitante"])]:
                info_time = self.ler_link_time(link, tipo)
                if info_time:
                    dados_times.append({"time": p[tipo], "tipo": tipo, **info_time})

        self.salvar_csv("novo_bd1971_robusto/csv_extraidos/partidas.csv", dados_partidas)
        self.salvar_csv("novo_bd1971_robusto/csv_extraidos/times.csv", dados_times)


# =====================================================
# EXECUTAR
# =====================================================
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperModular(url)
    scraper.executar()
