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

    # =====================================================
    # FUNÇÕES BASE
    # =====================================================
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

    # =====================================================
    # LEITURA DA LISTA PRINCIPAL
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

        print(f"✅ {len(partidas)} partidas encontradas.")
        return partidas

    # =====================================================
    # LEITURA DE LINKS ESPECÍFICOS
    # =====================================================
    def ler_link_mandante(self, url_mandante):
        """Extrai informações específicas do mandante"""
        print(f"🏠 Lendo mandante: {url_mandante}")
        soup = self._get_soup(url_mandante)

        div_pai = soup.find("div", class_="zz-tpl-rb")
        if not div_pai:
            print("   ⚠ Div pai não encontrada no visitante.")
            return None

        div_especifica = div_pai.find("div", id="entity_bio")
        if not div_especifica:
            print("   ⚠ Div específica (filha) não encontrada no visitante.")
            return None

        dados = {}
        spans = div_especifica.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            if "Nome" in texto:
                dados["nome"] = texto
            if "Apelidos" in texto:
                dados["apelido"] = texto
            if "Ano de Fundação" in texto:
                dados["fundacao"] = texto.replace("-", "/").strip()
            if "Cidade" in texto:
                dados["cidade"] = texto
            if "País" in texto:
                dados["pais"] = texto
            if "Estado" in texto:
                dados["estado"] = texto

        print(f"   ➤ {len(dados)} dados extraídos do visitante.")
        return dados

    def ler_link_partida(self, url_partida):
        """Extrai informações específicas da partida"""
        print(f"⚽ Lendo partida: {url_partida}")
        soup = self._get_soup(url_partida)

        # Localiza a div pai
        div_pai = soup.find("div", class_="info")  # div principal da partida
        if not div_pai:
            print("   ⚠ Div pai da partida não encontrada.")
            return None

        # Dentro dela, pega uma div específica (por exemplo 'zzgameinfo')
        div_especifica = div_pai.find("div", class_="header")
        if not div_especifica:
            print("   ⚠ Div específica (filha) da partida não encontrada.")
            return None

        # Extrair dados pontuais (ex: estádio, árbitro, público)
        dados = {}
        for linha in div_especifica.find_all("a"):
            texto = linha.get_text(strip=True)
            if "Estádio" in texto:
                dados["estadio"] = texto.replace("Estádio:", "").strip()
            elif "Público" in texto:
                dados["publico"] = texto.replace("Público:", "").strip()
            elif "Árbitro" in texto:
                dados["arbitro"] = texto.replace("Árbitro:", "").strip()

        print(f"   ➤ {len(dados)} dados extraídos da partida.")
        return dados

    def ler_link_visitante(self, url_visitante):
        """Extrai informações específicas do visitante"""
        print(f"🛫 Lendo visitante: {url_visitante}")
        soup = self._get_soup(url_visitante)

        div_pai = soup.find("div", class_="zz-tpl-rb")
        if not div_pai:
            print("   ⚠ Div pai não encontrada no visitante.")
            return None

        div_especifica = div_pai.find("div", id="entity_bio")
        if not div_especifica:
            print("   ⚠ Div específica (filha) não encontrada no visitante.")
            return None

        dados = {}
        spans = div_especifica.find_all("span")
        for span in spans:
            texto = span.get_text(strip=True)
            if "Nome" in texto:
                dados["nome"] = texto
            if "Apelidos" in texto:
                dados["apelido"] = texto
            if "Ano de Fundação" in texto:
                dados["fundacao"] = texto.replace("-", "/").strip()
            if "Cidade" in texto:
                dados["cidade"] = texto
            if "País" in texto:
                dados["pais"] = texto
            if "Estado" in texto:
                dados["estado"] = texto

        print(f"   ➤ {len(dados)} dados extraídos do visitante.")
        return dados

    # =====================================================
    # EXPORTAÇÃO PARA CSV
    # =====================================================
    def salvar_csv(self, nome, dados_lista):
        if not dados_lista:
            print(f"⚠ Nenhum dado para salvar em {nome}.")
            return
        campos = sorted({k for d in dados_lista for k in d})
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
        partidas_csv, mandantes_csv, visitantes_csv = [], [], []

        for p in partidas:
            # Detalhes da partida
            if p["link_partida"]:
                dados_partida = self.ler_link_partida(p["link_partida"])
                if dados_partida:
                    partidas_csv.append({"partida": p["placar"], **dados_partida})

            # Mandante
            if p["link_mandante"]:
                dados_mandante = self.ler_link_mandante(p["link_mandante"])
                if dados_mandante:
                    mandantes_csv.append({"mandante": p["mandante"], **dados_mandante})

            # Visitante
            if p["link_visitante"]:
                dados_visitante = self.ler_link_visitante(p["link_visitante"])
                if dados_visitante:
                    visitantes_csv.append({"visitante": p["visitante"], **dados_visitante})

        # Salvar cada um em um CSV separado
        self.salvar_csv("partidas.csv", partidas_csv)
        self.salvar_csv("mandantes.csv", mandantes_csv)
        self.salvar_csv("visitantes.csv", visitantes_csv)


# =====================================================
# USO
# =====================================================
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperModular(url)
    scraper.executar()
