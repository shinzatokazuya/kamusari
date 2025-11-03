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
        self.jogadores_lidos = set()

        # coletores de dados
        self.lista_clubes = []
        self.lista_estadios = []
        self.lista_jogadores = []

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

    def _valor_depois_do_span(self, span):
        """
        Retorna o primeiro valor textual útil que aparece depois do <span>
        Pode ser:
        - texto direto como sibling,
        - texto dentro de uma tag (a, div.text, etc),
        - ou None se não encontrado.
        """
        # percorre todos os siblings que aparecem depois do span
        for sib in span.next_siblings:
            # se for string (NavigableString)
            if isinstance(sib, str):
                txt = sib.strip()
                if txt:
                    return txt
                else:
                    continue
            # se for Tag (BeautifulSoup)
            try:
                txt = sib.get_text(strip=True)
            except Exception:
                txt = None
            if txt:
                return txt
        return None

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
        """Lê o link do clube e extrai todos os campos da div#entity_bio"""
        if not url_clube or url_clube in self.clubes_lidos:
            return
        self.clubes_lidos.add(url_clube)

        print(f"🏟️ Lendo {tipo}: {url_clube}")
        soup = self._get_soup(url_clube)

        container = soup.find("div", id="entity_bio")
        if not container:
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        # pega todas as divs com classe bio OU bio_half
        divs_info = container.find_all("div", class_=["bio", "bio_half"])
        dados = {"tipo": tipo}

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            # tenta pegar o valor da forma correta

            # guarda apenas campos de interesse
            if "Nome" in campo:
                dados["nome"] = valor
            elif "Apelido" in campo or "Apelidos" in campo:
                dados["apelido"] = valor
            elif "Fundado" in campo or "Ano de Fundação" in campo:
                dados["fundacao"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor
            elif "Estado" in campo:
                dados["estado"] = valor
            elif "País" in campo:
                dados["pais"] = valor

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
        div_jogadores = soup.find("div", class_="box_container")
        if div_jogadores:
            print("   ➤ Div de jogadores encontrada.")
            for a_tag in div_jogadores.find_all("a", href=True):
                link = urljoin(self.base_url, a_tag["href"])
                texto = a_tag.get_text(strip=True)

                # Aqui só guardamos os links, não extraímos nada ainda
                if texto != "":
                    self.ler_link_jogadores(link)

    # =====================================================
    # ESTÁDIOS
    # =====================================================
    def ler_link_estadio(self, url_estadio):
        """Acessa o link do estádio e extrai dados da div#entity_bio"""
        if not url_estadio or url_estadio in self.estadios_lidos:
            return
        self.estadios_lidos.add(url_estadio)

        print(f"🏟️ Lendo estádio: {url_estadio}")
        soup = self._get_soup(url_estadio)

        container = soup.find("div", id="entity_bio")
        if not container:
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        # pega todas as divs com classe bio OU bio_half
        divs_info = container.find_all("div", class_=["bio", "bio_half"])
        dados = {}

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            # tenta pegar o valor da forma correta

            if "Nome" in campo:
                dados["nome"] = valor
            elif "País" in campo:
                dados["pais"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor
            elif "Ano de Inauguração" in campo:
                dados["fundacao"] = valor
            elif "Lotação" in campo:
                dados["capacidade"] = valor

        self.lista_estadios.append(dados)
        print(f"   ➤ Estádio '{dados.get('nome')}' adicionado.")

    def ler_link_jogadores(self, url_jogadores):
        """ Acessa o link dos jogadores e extrai dados da div#entity_bio """
        if not url_jogadores or url_jogadores in self.jogadores_lidos:
            return
        self.jogadores_lidos.add(url_jogadores)

        print(f"🏟️ Lendo jogador: {url_jogadores}")
        soup = self._get_soup(url_jogadores)

        container = soup.find("div", id="entity_bio")
        if not container:
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        # pega todas as divs com classe bio OU bio_half
        divs_info = container.find_all("div", class_=["bio", "bio_half"])
        dados = {}

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            # tenta pegar o valor da forma correta
            if "Nome" in campo:
                dados["nome"] = valor
            elif "Data de Nascimento" in campo:
                dados["data_de_nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "País de Nascimento" in campo:
                dados["pais_de_nascimento"] = valor
            elif "Posição" in campo:
                dados["posicao"] = valor
            elif "Pé preferencial" in campo:
                dados["pe_preferido"] = valor
            elif "Altura" in campo:
                dados["altura"] = valor
            elif "peso" in campo:
                dados["peso"] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["data_de_falecimento"] = valor
                else:
                    dados["situacao"] = valor

        self.lista_jogadores.append(dados)
        print(f"   ➤ Jogador '{dados.get('nome')}' adicionado.")

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
        self.salvar_csv(
            "novo_bd1971_robusto/csv_extraidos/jogadores.csv",
            self.lista_jogadores,
            ["nome", "data_de_nascimento", "nacionalidade", "pais_de_nascimento", "posicao", "pe_preferido", "altura", "peso", "data_de_falecimento", "situacao"]
        )


# =====================================================
# EXECUTAR
# =====================================================
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperModular(url)
    scraper.executar()
