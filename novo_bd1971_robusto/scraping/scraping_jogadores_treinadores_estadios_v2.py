import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin
from datetime import datetime
import os

class OGolScraperRelacional:
    def __init__(self, url_lista):
        self.url_lista = url_lista
        self.base_url = "https://www.ogol.com.br"
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.delay = 15

        # Dicionários para IDs únicos
        self.clubes_dict = {}  # {url: {id, dados}}
        self.estadios_dict = {}  # {url: {id, dados}}
        self.jogadores_dict = {}  # {url: {id, dados}}
        self.treinadores_dict = {}  # {url: {id, dados}}
        self.locais_dict = {}  # {cidade_uf: {id, dados}}

        # Contadores de ID
        self.next_clube_id = 1
        self.next_estadio_id = 1
        self.next_jogador_id = 1
        self.next_treinador_id = 1
        self.next_local_id = 1
        self.next_partida_id = 1

        # Listas para CSVs finais
        self.partidas_lista = []
        self.jogadores_em_partida_lista = []
        self.treinadores_em_partida_lista = []
        self.eventos_partida_lista = []

    def _get_soup(self, url):
        time.sleep(self.delay)
        print(f"🌐 Acessando: {url}")
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    def _extrair_link(self, celula):
        tag = celula.find("a")
        texto = tag.get_text(strip=True) if tag else celula.get_text(strip=True)
        link = urljoin(self.base_url, tag["href"]) if tag and "href" in tag.attrs else None
        return texto, link

    def _valor_depois_do_span(self, span):
        for sib in span.next_siblings:
            if isinstance(sib, str):
                txt = sib.strip()
                if txt:
                    return txt
            else:
                try:
                    txt = sib.get_text(strip=True)
                    if txt:
                        return txt
                except:
                    pass
        return None

    def _get_ou_criar_local(self, cidade_completa):
        """Extrai e cria local único baseado em cidade (Estado)"""
        if not cidade_completa or cidade_completa == "-":
            return None

        # Parse: "São Paulo (SP)" -> cidade="São Paulo", uf="SP"
        if "(" in cidade_completa and ")" in cidade_completa:
            cidade = cidade_completa.split("(")[0].strip()
            uf = cidade_completa.split("(")[1].replace(")", "").strip()
        else:
            cidade = cidade_completa
            uf = ""

        chave = f"{cidade}_{uf}"

        if chave not in self.locais_dict:
            # Define região baseada no estado
            regioes = {
                'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
                'RS': 'Sul', 'SC': 'Sul', 'PR': 'Sul',
                'BA': 'Nordeste', 'PE': 'Nordeste', 'CE': 'Nordeste', 'RN': 'Nordeste',
                'PB': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 'PI': 'Nordeste', 'MA': 'Nordeste',
                'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
                'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'AC': 'Norte', 'RR': 'Norte', 'AP': 'Norte', 'TO': 'Norte'
            }

            self.locais_dict[chave] = {
                'id': self.next_local_id,
                'cidade': cidade,
                'uf': uf,
                'estado': uf,  # Para manter compatibilidade
                'regiao': regioes.get(uf, ''),
                'pais': 'Brasil'
            }
            self.next_local_id += 1

        return self.locais_dict[chave]['id']

    def processar_clube(self, url_clube):
        """Processa clube e retorna ID único"""
        if not url_clube:
            return None

        if url_clube in self.clubes_dict:
            return self.clubes_dict[url_clube]['id']

        print(f"🏟️ Processando clube: {url_clube}")
        soup = self._get_soup(url_clube)

        container = soup.find("div", id="entity_bio")
        if not container:
            return None

        dados = {'url': url_clube}
        divs_info = container.find_all("div", class_=["bio", "bio_half"])

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Apelido" in campo:
                dados["apelido"] = valor
            elif "Fundado" in campo or "Ano de Fundação" in campo:
                dados["fundacao"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor
            elif "Estado" in campo:
                dados["estado"] = valor
            elif "País" in campo:
                dados["pais"] = valor

        # Criar local
        cidade_completa = dados.get("cidade", "")
        local_id = self._get_ou_criar_local(cidade_completa)

        clube_id = self.next_clube_id
        self.clubes_dict[url_clube] = {
            'id': clube_id,
            'clube': dados.get('nome', ''),
            'apelido': dados.get('apelido', ''),
            'local_id': local_id,
            'fundacao': dados.get('fundacao', ''),
            'ativo': 1
        }
        self.next_clube_id += 1

        return clube_id

    def processar_estadio(self, url_estadio):
        """Processa estádio e retorna ID único"""
        if not url_estadio:
            return None

        if url_estadio in self.estadios_dict:
            return self.estadios_dict[url_estadio]['id']

        print(f"🏟️ Processando estádio: {url_estadio}")
        soup = self._get_soup(url_estadio)

        container = soup.find("div", id="entity_bio")
        if not container:
            return None

        dados = {}
        divs_info = container.find_all("div", class_=["bio", "bio_half"])

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            if "Nome" in campo:
                dados["nome"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor
            elif "Ano de Inauguração" in campo:
                dados["inauguracao"] = valor
            elif "Lotação" in campo:
                # Remove pontos e espaços: "68 000" -> 68000
                cap = valor.replace(".", "").replace(" ", "").replace(",", "")
                try:
                    dados["capacidade"] = int(cap)
                except:
                    dados["capacidade"] = None

        # Criar local do estádio
        local_id = self._get_ou_criar_local(dados.get("cidade", ""))

        estadio_id = self.next_estadio_id
        self.estadios_dict[url_estadio] = {
            'id': estadio_id,
            'estadio': dados.get('nome', ''),
            'capacidade': dados.get('capacidade'),
            'local_id': local_id,
            'inauguracao': dados.get('inauguracao', ''),
            'ativo': 1
        }
        self.next_estadio_id += 1

        return estadio_id

    def processar_jogador(self, url_jogador):
        """Processa jogador e retorna ID único"""
        if not url_jogador:
            return None

        if url_jogador in self.jogadores_dict:
            return self.jogadores_dict[url_jogador]['id']

        print(f"⚽ Processando jogador: {url_jogador}")
        soup = self._get_soup(url_jogador)

        container = soup.find("div", id="entity_bio")
        if not container:
            return None

        dados = {}
        divs_info = container.find_all("div", class_=["bio", "bio_half"])

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Naturalidade" in campo:
                dados["naturalidade"] = valor
            elif "Posição" in campo:
                dados["posicao"] = valor
            elif "Pé preferencial" in campo:
                dados["pe_preferido"] = valor
            elif "Altura" in campo:
                # "175 cm" -> 175
                alt = valor.replace("cm", "").strip()
                try:
                    dados["altura"] = int(alt)
                except:
                    dados["altura"] = None
            elif "Peso" in campo:
                peso = valor.replace("kg", "").strip()
                try:
                    dados["peso"] = int(peso)
                except:
                    dados["peso"] = None
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                    dados["aposentado"] = 1
                elif "Aposentado" in valor:
                    dados["aposentado"] = 1

        jogador_id = self.next_jogador_id
        self.jogadores_dict[url_jogador] = {
            'id': jogador_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'nacionalidade': dados.get('nacionalidade', ''),
            'altura': dados.get('altura'),
            'peso': dados.get('peso'),
            'posicao': dados.get('posicao', ''),
            'pe_preferido': dados.get('pe_preferido', ''),
            'aposentado': dados.get('aposentado', 0)
        }
        self.next_jogador_id += 1

        return jogador_id

    def processar_treinador(self, url_treinador):
        """Processa treinador e retorna ID único"""
        if not url_treinador:
            return None

        if url_treinador in self.treinadores_dict:
            return self.treinadores_dict[url_treinador]['id']

        print(f"👔 Processando treinador: {url_treinador}")
        soup = self._get_soup(url_treinador)

        container = soup.find("div", id="entity_bio")
        if not container:
            return None

        dados = {}
        divs_info = container.find_all("div", class_=["bio", "bio_half"])

        for div in divs_info:
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor

        treinador_id = self.next_treinador_id
        self.treinadores_dict[url_treinador] = {
            'id': treinador_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'nacionalidade': dados.get('nacionalidade', '')
        }
        self.next_treinador_id += 1

        return treinador_id

    def processar_detalhes_partida(self, url_partida, partida_id, mandante_id, visitante_id):
        """Processa detalhes da partida: escalações, eventos, etc."""
        if not url_partida:
            return None, None

        print(f"📋 Processando detalhes da partida: {url_partida}")
        soup = self._get_soup(url_partida)

        estadio_id = None

        # 1. Buscar informações do estádio
        header = soup.find("div", class_="header")
        if header:
            for a_tag in header.find_all("a", href=True):
                link = urljoin(self.base_url, a_tag["href"])
                if "estadio" in link.lower():
                    estadio_id = self.processar_estadio(link)
                    break

        # 2. Buscar escalações e eventos
        box_container = soup.find("div", class_="box_container")
        if not box_container:
            return estadio_id, None

        # Identificar times (geralmente há 2 divs principais)
        team_sections = box_container.find_all("div", class_="row", recursive=False)

        for idx, section in enumerate(team_sections[:2]):  # Processar só os 2 primeiros (mandante e visitante)
            clube_id = mandante_id if idx == 0 else visitante_id

            # Buscar jogadores nesta seção
            jogadores_links = section.find_all("a", href=True)

            for link_tag in jogadores_links:
                link = urljoin(self.base_url, link_tag["href"])

                # Identificar se é jogador ou treinador pelo URL
                if "/jogador/" in link:
                    jogador_id = self.processar_jogador(link)
                    if jogador_id:
                        # Determinar se é titular ou reserva (pode precisar de lógica mais sofisticada)
                        # Por enquanto, assume titular
                        self.jogadores_em_partida_lista.append({
                            'partida_id': partida_id,
                            'jogador_id': jogador_id,
                            'clube_id': clube_id,
                            'titular': 1,
                            'posicao_jogada': '',
                            'numero_camisa': None
                        })

                elif "/treinador/" in link or "/tecnico/" in link:
                    treinador_id = self.processar_treinador(link)
                    if treinador_id:
                        self.treinadores_em_partida_lista.append({
                            'partida_id': partida_id,
                            'treinador_id': treinador_id,
                            'clube_id': clube_id,
                            'tipo': 'Titular'
                        })

        return estadio_id, None

    def executar(self, edicao_id=1):
        """Execução principal do scraper"""
        print("🚀 Iniciando scraping...")

        # 1. Ler lista de partidas
        soup = self._get_soup(self.url_lista)
        tabela = soup.find("table", class_="zztable stats")

        if not tabela:
            print("❌ Tabela de partidas não encontrada")
            return

        # 2. Processar cada partida
        for linha in tabela.find_all("tr"):
            celulas = linha.find_all("td")
            if len(celulas) < 6:
                continue

            # Extrair dados básicos
            data = celulas[1].get_text(strip=True)
            hora = celulas[2].get_text(strip=True)
            mandante_nome, link_mandante = self._extrair_link(celulas[3])
            placar, link_partida = self._extrair_link(celulas[5])
            visitante_nome, link_visitante = self._extrair_link(celulas[7])
            fase = celulas[8].get_text(strip=True) if len(celulas) > 8 else ""

            print(f"\n{'='*60}")
            print(f"⚽ {mandante_nome} x {visitante_nome}")
            print(f"{'='*60}")

            # Processar clubes
            mandante_id = self.processar_clube(link_mandante)
            visitante_id = self.processar_clube(link_visitante)

            if not mandante_id or not visitante_id:
                print("⚠️ Erro ao processar clubes, pulando partida")
                continue

            # Parse do placar: "2 - 1" -> mandante=2, visitante=1
            placar_split = placar.split("-")
            mandante_placar = int(placar_split[0].strip()) if len(placar_split) == 2 else None
            visitante_placar = int(placar_split[1].strip()) if len(placar_split) == 2 else None

            partida_id = self.next_partida_id

            # Processar detalhes da partida
            estadio_id, eventos = self.processar_detalhes_partida(
                link_partida, partida_id, mandante_id, visitante_id
            )

            # Adicionar partida
            self.partidas_lista.append({
                'id': partida_id,
                'edicao_id': edicao_id,
                'estadio_id': estadio_id,
                'data': data,
                'hora': hora,
                'fase': fase,
                'rodada': None,
                'mandante_id': mandante_id,
                'visitante_id': visitante_id,
                'mandante_placar': mandante_placar,
                'visitante_placar': visitante_placar,
                'mandante_penalti': None,
                'visitante_penalti': None,
                'prorrogacao': 0
            })

            self.next_partida_id += 1

        # 3. Salvar CSVs
        self.salvar_csvs()
        print("\n✅ Scraping concluído!")

    def salvar_csvs(self):
        """Salva todos os CSVs"""
        os.makedirs("output_csvs", exist_ok=True)

        # Locais
        with open("output_csvs/locais.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'cidade', 'uf', 'estado', 'regiao', 'pais']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for local in self.locais_dict.values():
                writer.writerow(local)
        print("💾 locais.csv salvo")

        # Clubes
        with open("output_csvs/clubes.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'clube', 'apelido', 'local_id', 'fundacao', 'ativo']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for clube in self.clubes_dict.values():
                writer.writerow(clube)
        print("💾 clubes.csv salvo")

        # Estádios
        with open("output_csvs/estadios.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'estadio', 'capacidade', 'local_id', 'inauguracao', 'ativo']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for estadio in self.estadios_dict.values():
                writer.writerow(estadio)
        print("💾 estadios.csv salvo")

        # Jogadores
        with open("output_csvs/jogadores.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'nome', 'nascimento', 'falecimento', 'nacionalidade',
                     'altura', 'peso', 'posicao', 'pe_preferido', 'aposentado']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for jogador in self.jogadores_dict.values():
                writer.writerow(jogador)
        print("💾 jogadores.csv salvo")

        # Treinadores
        with open("output_csvs/treinadores.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'nome', 'nascimento', 'falecimento', 'nacionalidade']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for treinador in self.treinadores_dict.values():
                writer.writerow(treinador)
        print("💾 treinadores.csv salvo")

        # Partidas
        with open("output_csvs/partidas.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['id', 'edicao_id', 'estadio_id', 'data', 'hora', 'fase', 'rodada',
                     'mandante_id', 'visitante_id', 'mandante_placar', 'visitante_placar',
                     'mandante_penalti', 'visitante_penalti', 'prorrogacao']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(self.partidas_lista)
        print("💾 partidas.csv salvo")

        # Jogadores em Partida
        with open("output_csvs/jogadores_em_partida.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['partida_id', 'jogador_id', 'clube_id', 'titular',
                     'posicao_jogada', 'numero_camisa']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(self.jogadores_em_partida_lista)
        print("💾 jogadores_em_partida.csv salvo")

        # Treinadores em Partida
        with open("output_csvs/treinadores_em_partida.csv", "w", newline="", encoding="utf-8") as f:
            campos = ['partida_id', 'treinador_id', 'clube_id', 'tipo']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(self.treinadores_em_partida_lista)
        print("💾 treinadores_em_partida.csv salvo")


# Executar
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperRelacional(url)
    scraper.executar(edicao_id=1)  # edicao_id=1 corresponde ao ano 1971
