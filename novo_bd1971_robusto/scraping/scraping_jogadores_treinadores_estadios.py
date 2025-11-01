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

        # caches para evitar repetições
        self.clubes_lidos = set()
        self.estadios_lidos = set()

        # coletores de dados
        self.lista_clubes = []
        self.lista_estadios = []

    # =====================================================
    # BASE
    # =====================================================
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
    # LISTA PRINCIPAL
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
    # CLUBES
    # =====================================================
    def ler_link_clube(self, url_clube, tipo):
        """Lê o link do clube e extrai os campos da div.bio"""
        if not url_clube or url_clube in self.clubes_lidos:
            return
        self.clubes_lidos.add(url_clube)

        print(f"🏟️ Lendo {tipo}: {url_clube}")
        soup = self._get_soup(url_clube)

        div_bio = soup.find("div", class_="bio")
        div_bio2 = soup.find("div", class_="bio_half")
        if not div_bio or not div_bio2:
            print("⚠ Div 'bio' não encontrada para o clube.")
            return

        dados = {"tipo": tipo}
        spans = div_bio.find_all("span")
        pans = div_bio2.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            # Captura o conteúdo de texto que vem logo após o <span>
            valor = span.next_sibling.strip() if span.next_sibling else None

            if "Nome" in texto:
                dados["nome"] = valor
        for pan in pans:
            texto = pan.get_text(strip=True)
            valor = pan.next_sibling.strip() if pan.next_sibling else None

            if "Apelido" in texto:
                dados["apelido"] = valor
            elif "Fundado" in texto or "Ano de Fundação" in texto:
                dados["fundacao"] = valor
            elif "Cidade" in texto:
                dados["cidade"] = valor
            elif "Estado" in texto:
                dados["estado"] = valor
            elif "País" in texto:
                dados["pais"] = valor

        # Adiciona à lista de clubes
        self.lista_clubes.append(dados)
        print(f"   ➤ Clube '{dados.get('nome')}' adicionado.")

    # =====================================================
    # PARTIDAS — PEGAR LINKS DE ESTÁDIO E JOGADORES
    # =====================================================
    def ler_link_partida(self, url_partida):
        """Acessa o link da partida e encontra as duas divs de interesse"""
        print(f"⚽ Lendo partida: {url_partida}")
        soup = self._get_soup(url_partida)

        # Div 1 — Links de estádio e árbitro
        div_estadio_arbitro = soup.find("div", class_="header")
        if div_estadio_arbitro:
            for a_tag in div_estadio_arbitro.find_all("a", href=True):
                link = urljoin(self.base_url, a_tag["href"])
                texto = a_tag.get_text(strip=True)

                # Aqui só guardamos os links, não extraímos nada ainda
                if "estadio" in link.lower() or "estádio" in texto.lower():
                    self.ler_link_estadio(link)

        # Div 2 — Links de jogadores (ainda não usados nesta fase)
        div_jogadores = soup.find("div", class_="zz-tpl-main")
        if div_jogadores:
            print("   ➤ Div de jogadores encontrada (guardando para futura extração).")

    # =====================================================
    # ESTÁDIOS
    # =====================================================
    def ler_link_estadio(self, url_estadio):
        """Acessa o link do estádio e extrai dados da div.bio"""
        if not url_estadio or url_estadio in self.estadios_lidos:
            return
        self.estadios_lidos.add(url_estadio)

        print(f"🏟️ Lendo estádio: {url_estadio}")
        soup = self._get_soup(url_estadio)

        div_bio = soup.find("div", class_="bio")
        div_bio2 = soup.find("div", class_="bio_half")
        if not div_bio or not div_bio2:
            print("⚠ Div 'bio' não encontrada no estádio.")
            return

        dados = {}
        spans = div_bio.find_all("span")
        pans = div_bio2.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            valor = span.next_sibling.strip() if span.next_sibling else None

            if "Nome" in texto:
                dados["nome"] = valor

        for pan in pans:
            texto = pan.get_text(strip=True)
            valor = pan.next_sibling.strip() if pan.next_sibling else None

            if "País" in texto:
                dados["pais"] = valor
            elif "Cidade" in texto:
                dados["cidade"] = valor
            elif "Fundado" in texto:
                dados["fundacao"] = valor
            elif "Capacidade" in texto:
                dados["capacidade"] = valor

        self.lista_estadios.append(dados)
        print(f"   ➤ Estádio '{dados.get('nome')}' adicionado.")

    # =====================================================
    # EXPORTAÇÃO
    # =====================================================
    def salvar_csv(self, nome, dados_lista, campos):
        if not dados_lista:
            print(f"⚠ Nenhum dado para salvar em {nome}.")
            return
        with open(nome, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(dados_lista)
        print(f"💾 CSV salvo: {nome}")

    # =====================================================
    # EXECUÇÃO PRINCIPAL
    # =====================================================
    def executar(self):
        partidas = self.ler_lista_partidas()

        for p in partidas:
            print(f"\n=== Processando {p['mandante']} x {p['visitante']} ===")

            # clubes
            self.ler_link_clube(p["link_mandante"], "mandante")
            self.ler_link_clube(p["link_visitante"], "visitante")

            # partida (para pegar links de estádio)
            if p["link_partida"]:
                self.ler_link_partida(p["link_partida"])

        # salvar csvs
        self.salvar_csv(
            "novo_bd1971_robusto/csv_extraidos/clubes.csv",
            self.lista_clubes,
            ["tipo", "nome", "apelido", "fundacao", "cidade", "estado", "pais"]
        )
        self.salvar_csv(
            "novo_bd1971_robusto/csv_extraidos/estadios.csv",
            self.lista_estadios,
            ["nome", "pais", "cidade", "fundacao", "capacidade"]
        )


# =====================================================
# EXECUTAR
# =====================================================
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperModular(url)
    scraper.executar()
