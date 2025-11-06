import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin
import os
import re

class OGolScraperRelacional:
    def __init__(self, url_lista):
        self.url_lista = url_lista
        self.base_url = "https://www.ogol.com.br"
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.delay = 30

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
        self.next_evento_id = 1

        # Listas temporárias (buffer antes de salvar)
        self.partidas_lista = []
        self.jogadores_em_partida_lista = []
        self.treinadores_em_partida_lista = []
        self.eventos_partida_lista = []

        # Caminho dos CSVs
        self.output_dir = "output_csvs"
        os.makedirs(self.output_dir, exist_ok=True)

        # Caminho do CHECKPOINT
        self.checkpoint_path = os.path.join(self.output_dir, "checkpoint.txt")


    # ======================================================
    # Funções utilitárias
    # ======================================================

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

    # ======================================================
    # Processadores
    # ======================================================

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

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
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
        local_id = self._get_ou_criar_local(dados.get("cidade", ""))
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
        for div in container.find_all("div", class_=["bio", "bio_half"]):
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
                # cap = valor.replace(".", "").replace(" ", "").replace(",", "")
                try:
                    dados["capacidade"] = int(re.sub(r"[^\d]", "", valor))
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
            return None

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

        # 2. Buscar o container principal da partida
        box_container = soup.find("div", id="game_report")
        if not box_container:
            print("⚠️ Div 'game_report' não encontrada")
            return estadio_id

        # 3. Buscar todas as linhas (rows) do relatório
        rows = box_container.find_all("div", class_="zz-tpl-row game_report")

        if not rows:
            print("⚠️ Nenhuma linha encontrada no game_report")
            return estadio_id

        def registrar_evento(jogador_id, clube_id, tipo, minuto=None):
            self.eventos_partida_lista.append({
                'id': self.next_evento_id,
                'parida_id': partida_id,
                'jogador_id': jogador_id,
                'clube_id': clube_id,
                'tipo_evento': tipo,
                'minuto': minuto
            })
            self.next_evento_id += 1

        # ---------------- TITULARES ----------------
        primeira_linha = rows[0]
        colunas = primeira_linha.find_all("div", class_="zz-tpl-col is-6 fl-c", recursive=False)

        for idx, coluna in enumerate(colunas):
            clube_id = mandante_id if idx == 0 else visitante_id

            # Buscar subtitle para confirmar o time
            subtitle = coluna.find("div", class_="subtitle")
            time_nome = subtitle.get_text(strip=True) if subtitle else ""
            print(f"   📝 Processando titulares de: {time_nome}")

            for player_div in coluna.find_all("div", class_="player"):
                # Buscar o link do jogador
                link_tag = player_div.find("a", href=lambda x: x and "/jogador/" in x)
                if not link_tag:
                    continue

                jogador_id = self.processar_jogador(urljoin(self.base_url, link_tag["href"]))
                if not jogador_id:
                    continue

                numero = player_div.find("div", class_="number")
                numero_camisa = numero.get_text(strip=True) if numero else None
                numero_camisa = int(numero_camisa) if numero_camisa and numero_camisa.isdigit() else None

                self.jogadores_em_partida_lista.append({
                    'partida_id': partida_id,
                    'jogador_id': jogador_id,
                    'clube_id': clube_id,
                    'titular': 1,
                    'posicao_jogada': '',
                    'numero_camisa': numero_camisa,
                })

                # ---------------- EVENTOS ----------------
                events_div = player_div.find("div", class_="events")
                if events_div:
                    for icon in events_div.find_all("span", class_="icn_zerozero"):
                        txt = icon.get_text(strip=True)
                        if txt == "8":
                            registrar_evento(jogador_id, clube_id, "Gol")
                        elif txt == "4":
                            registrar_evento(jogador_id, clube_id, "Amarelo")
                        elif txt == "5":
                            registrar_evento(jogador_id, clube_id, "Vermelho")
                        elif txt == "7":
                            registrar_evento(jogador_id, clube_id, "Substituição")

        # ---------------- RESERVAS ----------------
        if len(rows) > 1:
            segunda_linha = rows[1]
            colunas = segunda_linha.find_all("div", class_="zz-tpl-col is-6 fl-c", recursive=False)

            for idx, coluna in enumerate(colunas):
                clube_id = mandante_id if idx == 0 else visitante_id

                for player_div in coluna.find_all("div", class_="player"):
                    link_tag = player_div.find("a", href=lambda x: x and "/jogador/" in x)
                    if not link_tag:
                        continue

                    link = urljoin(self.base_url, link_tag["href"])
                    jogador_id = self.processar_jogador(link)

                    if jogador_id:
                        self.jogadores_em_partida_lista.append({
                            'partida_id': partida_id,
                            'jogador_id': jogador_id,
                            'clube_id': clube_id,
                            'titular': 0, # É RESERVA
                            'posicao_jogada': '',
                            'numero_camisa': numero_camisa
                        })
                        events_div = player_div.find("div", class_="events")
                        if events_div and events_div.find("span", title="Entrou"):
                            registrar_evento(jogador_id, clube_id, "Entrou")

        # ---------------- TREINADORES ----------------
        if len(rows) > 2:
            terceira_linha = rows[2]
            colunas = terceira_linha.find_all("div", class_="zz-tpl-col is-6 fl-c", recursive=False)

            for idx, coluna in enumerate(colunas):
                clube_id = mandante_id if idx == 0 else visitante_id
                link_tag = coluna.find("a", href=lambda x: x and "/treinador/" in x)
                if link_tag:
                    link = urljoin(self.base_url, link_tag["href"])
                    treinador_id = self.processar_treinador(link)
                    if treinador_id:
                        self.treinadores_em_partida_lista.append({
                            'partida_id': partida_id,
                            'treinador_id': treinador_id,
                            'clube_id': clube_id,
                            'tipo': 'Titular'
                        })

        return estadio_id

    # ======================================================
    # Execução principal
    # ======================================================

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
            placar_split = placar.strip().upper()
            if "WO" in placar_split or "ANU" in placar_split or "IC" in placar_split:
                # Define regra
                mandante_placar, visitante_placar = '-', '-'
            else:
                placar_split = placar.strip().lower()
                penalti_mandante = penalti_visitante = None
                prorrogacao = 0

                # Verifica se há pênaltis no placar (ex: "1-1 (4-3 pen.)")
                match_penaltis = re.search(r'\((\d+)-(\d+)\s*pen', placar_split)
                if match_penaltis:
                    penalti_mandante = int(match_penaltis.group(1))
                    penalti_visitante = int(match_penaltis.group(2))

                if 'pro.' in placar_split:
                    prorrogacao = 1

                if '-' not in placar_split:
                    print(f"Placar inválido: {placar}, pulando partida")
                    continue
                try:
                    placar_limpo = re.search(r'(\d+)\s*-\s*(\d+)', placar)
                    if placar_limpo:
                        mandante_placar = int(placar_limpo.group(1))
                        visitante_placar = int(placar_limpo.group(2))
                    else:
                        print(f"Placar mal formatado: {placar}, pulando partida")
                        continue

                except ValueError:
                    print(f"Erro ao converter placar: {placar}, pulando partida")
                    continue

            partida_id = self.next_partida_id

            # Processar detalhes da partida
            estadio_id = self.processar_detalhes_partida(
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
                'mandante_penalti': penalti_mandante,
                'visitante_penalti': penalti_visitante,
                'prorrogacao': prorrogacao
            })

            self.next_partida_id += 1

        # 3. Salvar CSVs
        self.salvar_csvs()
        print("\n✅ Scraping concluído!")

    def salvar_csvs(self):
        os.makedirs("output_csvs", exist_ok=True)

        def salvar(nome, campos, dados):
            path = f"output_csvs/{nome}.csv"
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=campos)
                w.writeheader()
                # dados pode ser dict_values (para dicts de entidade) ou lista
                if hasattr(dados, "values"):
                    rows = list(dados)
                    # if it's dict_values, iterate differently:
                    try:
                        for item in dados:
                            w.writerow(item)
                    except Exception:
                        for item in list(dados):
                            w.writerow(item)
                else:
                    w.writerows(dados)
            print(f"💾 {nome}.csv salvo ({path})")

        # entidades
        salvar("locais", ['id','cidade','uf','estado','regiao','pais'], self.locais_dict.values())
        salvar("clubes", ['id','clube','apelido','local_id','fundacao','ativo'], self.clubes_dict.values())
        salvar("estadios", ['id','estadio','capacidade','local_id','inauguracao','ativo'], self.estadios_dict.values())
        salvar("jogadores", ['id','nome','nascimento','falecimento','nacionalidade','altura','peso','posicao','pe_preferido','aposentado'], self.jogadores_dict.values())
        salvar("treinadores", ['id','nome','nascimento','falecimento','nacionalidade'], self.treinadores_dict.values())

        # relacionais
        salvar("partidas", ['id','edicao_id','data','hora','mandante_id','visitante_id','estadio_id'], self.partidas_lista)
        salvar("jogadores_em_partida", ['partida_id','jogador_id','clube_id','titular','posicao_jogada','numero_camisa'], self.jogadores_em_partida_lista)
        salvar("treinadores_em_partida", ['partida_id','treinador_id','clube_id','tipo'], self.treinadores_em_partida_lista)
        salvar("eventos_partida", ['id','partida_id','jogador_id','clube_id','tipo_evento','minuto'], self.eventos_partida_lista)

# ---------------- executar ----------------
if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperRelacional(url)
    scraper.executar(edicao_id=1)  # edicao_id=1 corresponde ao ano 1971
