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
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive"
        }
        self.delay = 30

        # Dicionários para IDs únicos
        self.clubes_dict = {}  # {url: {id, dados}}
        self.estadios_dict = {}  # {url: {id, dados}}
        self.jogadores_dict = {}  # {url: {id, dados}}
        self.treinadores_dict = {}  # {url: {id, dados}}
        self.arbitros_dict = {} # {url: {id, dados}}
        self.locais_dict = {}  # {cidade_uf: {id, dados}}

        # Buffers de novos registros
        self._novo_clube = []
        self._novo_estadio = []
        self._novo_jogador = []
        self._novo_treinador = []
        self._novo_arbitro = []

        # Contadores de ID
        self.next_clube_id = 1
        self.next_estadio_id = 1
        self.next_jogador_id = 1
        self.next_treinador_id = 1
        self.next_arbitro_id = 1
        self.next_local_id = 1
        self.next_partida_id = 1
        self.next_evento_id = 1

        # Buffers relacionais
        self.partidas_lista = []
        self.jogadores_em_partida_lista = []
        self.treinadores_em_partida_lista = []
        self.arbitros_em_partida_lista = []
        self.eventos_partida_lista = []

        # Caminho dos CSVs
        self.output_dir = "novo_bd1971_robusto/output_csvs"
        os.makedirs(self.output_dir, exist_ok=True)

        # Caminho do CHECKPOINT
        self.checkpoint_path = os.path.join(self.output_dir, "checkpoint.txt")

        """# Carrega dados e sincroniza IDs existentes
        self._carregar_existentes()
        self._sincronizar_ids_existentes()"""


    # ======================================================
    # Funções utilitárias
    # ======================================================

    def _get_soup(self, url):
        """Faz a requisição HTTP com tentativas e tratamento de erro 429 (Too Many Requests)."""
        tentativa = 0
        max_tentativas = 5
        delay = self.delay

        while tentativa < max_tentativas:
            try:
                print(f"🌐 Acessando: {url}")
                r = requests.get(url, headers=self.headers)
                if r.status_code == 429:
                    tentativa += 1
                    espera = delay * (tentativa + 1)
                    print(f"⚠️ Erro 429 (Too Many Requests). Aguardando {espera} segundos antes de tentar novamente...")
                    time.sleep(espera)
                    continue
                r.raise_for_status()
                return BeautifulSoup(r.text, "html.parser")
            except requests.exceptions.RequestException as e:
                tentativa += 1
                espera = delay * (tentativa + 1)
                print(f"⚠️ Tentativa {tentativa} falhou ({e}). Aguardando {espera}s e tentando novamente...")
                time.sleep(espera)

        raise Exception(f"❌ Falha ao acessar {url} após {max_tentativas} tentativas")


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
            print(f"   ➤ Local '{cidade}, {uf}' adicionado.")
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
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)
            if not valor:
                a = div.find("a")
                if a:
                    valor = a.get_text(strip=True)
                else:
                    txtdiv = div.find("div", class_="text")
                    if txtdiv:
                        valor = txtdiv.get_text(strip=True)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Apelido" in campo:
                dados["apelido"] = valor
            elif "Fundado" in campo or "Ano de Fundação" in campo:
                dados["fundacao"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor

        # Criar local
        local_id = self._get_ou_criar_local(dados.get("cidade", ""))
        clube_id = self.next_clube_id
        registro = {
            'id': clube_id,
            'clube': dados.get('nome', ''),
            'apelido': dados.get('apelido', ''),
            'local_id': local_id,
            'fundacao': dados.get('fundacao', ''),
            'ativo': 1
        }
        print(f"   ➤ Clube '{dados.get('nome')}, Local_id '{local_id}' adicionado.")
        self.clubes_dict[url_clube] = registro
        self._novo_clube.append(registro)
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
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)
            if not valor:
                a = div.find("a")
                if a:
                    valor = a.get_text(strip=True)
                else:
                    txtdiv = div.find("div", class_="text")
                    if txtdiv:
                        valor = txtdiv.get_text(strip=True)

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
        registro = {
            'id': estadio_id,
            'estadio': dados.get('nome', ''),
            'capacidade': dados.get('capacidade'),
            'local_id': local_id,
            'inauguracao': dados.get('inauguracao', ''),
            'ativo': 1
        }
        print(f"   ➤ Estádio '{dados.get('nome')}' adicionado.")
        self.estadios_dict[url_estadio] = registro
        self._novo_estadio.append(registro)
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
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)
            if not valor:
                a = div.find("a")
                if a:
                    valor = a.get_text(strip=True)
                else:
                    txtdiv = div.find("div", class_="text")
                    if txtdiv:
                        valor = txtdiv.get_text(strip=True)

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
                # alt = valor.replace("cm", "").strip()
                try:
                    dados["altura"] = int(re.sub(r"[^\d]", "", valor))
                except:
                    dados["altura"] = None
            elif "Peso" in campo:
                # peso = valor.replace("kg", "").strip()
                try:
                    dados["peso"] = int(re.sub(r"[^\d]", "", valor))
                except:
                    dados["peso"] = None
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                    dados["aposentado"] = 1
                elif "Aposentado" in valor:
                    dados["aposentado"] = 1

        jogador_id = self.next_jogador_id
        registro = {
            'id': jogador_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'nacionalidade': dados.get('nacionalidade', ''),
            'naturalidade': dados.get('naturalidade', ''),
            'altura': dados.get('altura'),
            'peso': dados.get('peso'),
            'posicao': dados.get('posicao', ''),
            'pe_preferido': dados.get('pe_preferido', ''),
            'aposentado': dados.get('aposentado', 0)
        }
        print(f"   ➤ Jogador '{dados.get('nome')}' adicionado.")
        self.jogadores_dict[url_jogador] = registro
        self._novo_jogador.append(registro)
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
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)
            if not valor:
                a = div.find("a")
                if a:
                    valor = a.get_text(strip=True)
                else:
                    txtdiv = div.find("div", class_="text")
                    if txtdiv:
                        valor = txtdiv.get_text(strip=True)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Naturalidade" in campo:
                dados['naturalidade'] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                else:
                    dados["situacao"] = valor

        treinador_id = self.next_treinador_id
        registro = {
            'id': treinador_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'naturalidade': dados.get('naturalidade', ''),
            'nacionalidade': dados.get('nacionalidade', '')
        }
        print(f"   ➤ Treinador '{dados.get('nome')}' adicionado.")
        self.treinadores_dict[url_treinador] = registro
        self._novo_treinador.append(registro)
        self.next_treinador_id += 1

        return treinador_id

    def processar_arbitro(self, url_arbitro):
        """Processa treinador e retorna ID único"""
        if not url_arbitro:
            return None

        if url_arbitro in self.arbitros_dict:
            return self.arbitros_dict[url_arbitro]['id']

        print(f"👔 Processando arbitro: {url_arbitro}")
        soup = self._get_soup(url_arbitro)

        container = soup.find("div", id="entity_bio")
        if not container:
            print("⚠ Div 'entity_bio' não encontrada.")
            return

        dados = {}
        for div in container.find_all("div", class_=["bio", "bio_half"]):
            span = div.find("span")
            if not span:
                continue

            campo = span.get_text(strip=True)
            valor = self._valor_depois_do_span(span)
            if not valor:
                a = div.find("a")
                if a:
                    valor = a.get_text(strip=True)
                else:
                    txtdiv = div.find("div", class_="text")
                    if txtdiv:
                        valor = txtdiv.get_text(strip=True)

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Naturalidade" in campo:
                dados['naturalidade'] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                else:
                    dados["situacao"] = valor

        arbitro_id = self.next_arbitro_id
        registro = {
            'id': arbitro_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'naturalidade': dados.get('naturalidade', ''),
            'nacionalidade': dados.get('nacionalidade', '')
        }
        print(f"   ➤ Jogador '{dados.get('nome')}' adicionado.")
        self.arbitros_dict[url_arbitro] = registro
        self._novo_arbitro.append(registro)
        self.next_arbitro_id += 1

        return arbitro_id

    # ======================================================
    # Eventos
    # ======================================================

    def registrar_evento(self, partida_id, jogador_id, clube_id, tipo_evento, tipo_gol=None, minuto=None):
            """Registra evento evitando duplicação exata, mas permitindo múltiplos eventos por jogador"""
            if not all([partida_id is not None, jogador_id is not None, clube_id is not None, tipo_evento]):
                return

            evento = {
                'id': self.next_evento_id,
                'partida_id': partida_id,
                'jogador_id': jogador_id,
                'clube_id': clube_id,
                'tipo_evento': tipo_evento,
                'tipo_gol': tipo_gol or '',
                'minuto': minuto or ''
            }
            print(f"   ➤ Evento '{tipo_evento}' adicionado.")

            # Chave única por partida - permite repetições em minutos diferentes
            chave = (partida_id, jogador_id, tipo_evento, tipo_gol or '', minuto or '')
            existing_keys = {(e['partida_id'], e['jogador_id'], e['tipo_evento'], e['tipo_gol'], e['minuto']) for e in self.eventos_partida_lista}

            if chave not in existing_keys:
                self.eventos_partida_lista.append(evento)
                self.next_evento_id += 1
                print(f"   ➤ Evento '{tipo_evento}' registrado (Partida {partida_id}, Jogador {jogador_id}, Minuto {minuto}, TipoGol {tipo_gol})")
            else:
                print(f"   ➤ Evento duplicado ignorado: {chave}")

    # ======================================================
    # Detalhes da partida
    # ======================================================

    def processar_detalhes_partida(self, url_partida, partida_id, mandante_id, visitante_id):
        """Processa detalhes da partida: escalações, eventos, etc."""
        if not url_partida:
            return None

        print(f"📋 Processando detalhes da partida: {url_partida}")
        try:
            soup = self._get_soup(url_partida)
        except Exception as e:
            print(f"❌ Falha ao acessar partida ({url_partida}): {e}")
            return

        estadio_id = None
        arbitro_id = None

        # ---------- Estádio e Árbitro ----------
        header = soup.find("div", class_="header")
        if header:
            for a_tag in header.find_all("a", href=True):
                link = urljoin(self.base_url, a_tag["href"])
                if "estadio" in link.lower():
                    try:
                        estadio_id = self.processar_estadio(link)
                    except Exception as e:
                        print(f"⚠️ Erro ao processar estádio ({link}): {e}")
                elif "arbitro" in link.lower():
                    try:
                        arbitro_id = self.processar_arbitro(link)
                    except Exception as e:
                        print(f"⚠️ Erro ao processar árbitro ({link}): {e}")

        # Sempre registrar arbitro (mesmo None)
        self.arbitros_em_partida_lista.append({
            'partida_id': partida_id,
            'arbitro_id': arbitro_id or None
        })

        # ---------- Container principal ----------
        box_container = soup.find("div", id="game_report")
        if not box_container:
            print("⚠️ Div 'game_report' não encontrada")
            return estadio_id

        # 3. Buscar todas as linhas (rows) do relatório
        rows = box_container.find_all("div", class_="zz-tpl-row game_report")

        if not rows:
            print("⚠️ Nenhuma linha encontrada no game_report")
            return estadio_id

        # ---------------- TITULARES ----------------
        primeira_linha = rows[0]
        colunas = primeira_linha.find_all("div", class_=lambda c: c and "zz-tpl-col" in c)

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

                try:
                    jogador_id = self.processar_jogador(urljoin(self.base_url, link_tag["href"]))
                except Exception as e:
                    print(f"⚠️ Erro ao processar jogador titular ({link_tag['href']}): {e}")
                    continue

                # Número da camisa (Quando existir)
                numero = player_div.find("div", class_="number")
                numero_camisa = numero.get_text(strip=True) if numero else None
                numero_camisa = int(numero_camisa) if numero_camisa and numero_camisa.isdigit() else None

                # Adiciona relacionamento jogador-em-partida
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
                if not events_div:
                    print("⚠ Div 'events' não encontrada.")
                    continue
                else:
                    # Vamos iterar sobre os filhos: span (icone) seguido possivelmente por um ou mais div com tempos/texto
                    children = [c for c in events_div.children if (not isinstance(c, str)) and (c)]
                    i = 0
                    while i < len(children):
                        node = children[i]
                        # se for span -> evento
                        if node.name == "span":
                            span = node
                            title = span.get("title", "").strip().lower()
                            classe = " ".join(span.get("class", [])).lower()
                            texto_icone = span.get_text(strip=True)

                            tipo_evento = None
                            tipo_gol = None

                            # identificar tipo do ícone/span
                            if "gol" in title or "fut-11" in classe:
                                tipo_evento = "Gol"
                                # verificar no próximo(s) textos se aparece "(pen.)" ou "(g.c.)"
                                # Observação: às vezes o texto com (pen.) aparece na mesma div de minuto, tratamos adiante
                            elif "público" in title or "icn_zerozero2 grey" in classe:
                                tipo_evento = "Assistência"
                            elif "amarel" in title or "icn_zerozero yellow" in classe:
                                tipo_evento = "Cartão Amarelo"
                            elif "vermelh" in title or "icn_zerozero red" in classe:
                                tipo_evento = "Cartão Vermelho"
                            elif "entrou" in title or texto_icone == "7":
                                tipo_evento = "Entrou"
                            elif texto_icone == "8":
                                # 8 costuma representar substituição
                                tipo_evento = "Substituição"
                            else:
                                # caso não saiba, pula
                                tipo_evento = None

                            # procurar o(s) div(s) seguintes que contenham minutos/texto
                            minutos_texto = ""
                            j = i + 1
                            while j < len(children) and children[j].name == "div":
                                minutos_texto += " " + children[j].get_text(" ", strip=True)
                                j += 1

                            minutos_texto = minutos_texto.strip()

                            # se for gol, detectar (pen.) ou (g.c.) no minutos_texto
                            if tipo_evento == "Gol":
                                if "(pen.)" in minutos_texto.lower() or "(pen.)" in texto_icone.lower():
                                    tipo_gol = "Penalti"
                                elif "(g.c.)" in minutos_texto.lower() or "(g.c.)" in texto_icone.lower():
                                    tipo_gol = "Gol Contra"
                                else:
                                    tipo_gol = ""

                            # extrair múltiplos tempos — regex robusta:
                            # exemplos que queremos pegar: "35' (pen.)48' 67' ", "45+3'", "70", "45' 60'"
                            # usamos regex para capturar padrões de minuto, incluindo "45+2", "45+3"
                            if minutos_texto:
                                # substitui apostrofos por espaços para normalizar e procurar padrões
                                # vamos procurar por padrões como 45+2, 45, 85, etc
                                tempos = re.findall(r"(\d+\+?\d*)'", minutos_texto)
                                if not tempos:
                                    # às vezes o site coloca sem apóstrofo: "67 " ou "67' " — tentamos pegar números isolados
                                    tempos = re.findall(r"(\d+\+?\d*)", minutos_texto)
                                # se tempos vazios e tipo_evento identificado (ex: assistência sem minuto visível), gravar um evento sem minuto
                                if tempos:
                                    for t in tempos:
                                        t_clean = t.replace("'", "").strip()
                                        self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, t_clean)
                                else:
                                    # registra sem minuto explícito
                                    self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)
                            else:
                                # sem div de minuto, registra sem minuto (algumas vezes o ícone aparece sozinho)
                                self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)

                            # avançar i até j (pular as divs de minuto que já consumimos)
                            i = j
                        else:
                            # se não for span (pode ser div vazio), apenas avança
                            i += 1


        # ---------------- RESERVAS ----------------
        if len(rows) > 1:
            segunda_linha = rows[1]
            colunas = segunda_linha.find_all("div", class_=lambda c: c and "zz-tpl-col" in c)

            for idx, coluna in enumerate(colunas):
                clube_id = mandante_id if idx == 0 else visitante_id

                for player_div in coluna.find_all("div", class_="player"):
                    link_tag = player_div.find("a", href=lambda x: x and "/jogador/" in x)
                    if not link_tag:
                        continue
                    try:
                        jogador_id = self.processar_jogador(urljoin(self.base_url, link_tag["href"]))
                    except Exception as e:
                        print(f"⚠️ Erro ao processar reserva ({link_tag['href']}): {e}")
                        continue

                    self.jogadores_em_partida_lista.append({
                            'partida_id': partida_id,
                            'jogador_id': jogador_id,
                            'clube_id': clube_id,
                            'titular': 0, # É RESERVA
                            'posicao_jogada': '',
                            'numero_camisa': numero_camisa
                    })

                    # ---------------- EVENTOS ----------------
                    events_div = player_div.find("div", class_="events")
                    if not events_div:
                        print("⚠ Div 'events' não encontrada.")
                        continue
                    else:
                        # Vamos iterar sobre os filhos: span (icone) seguido possivelmente por um ou mais div com tempos/texto
                        children = [c for c in events_div.children if (not isinstance(c, str)) and (c)]
                        i = 0
                        while i < len(children):
                            node = children[i]
                            # se for span -> evento
                            if node.name == "span":
                                span = node
                                title = span.get("title", "").strip().lower()
                                classe = " ".join(span.get("class", [])).lower()
                                texto_icone = span.get_text(strip=True)

                                tipo_evento = None
                                tipo_gol = None

                                # identificar tipo do ícone/span
                                if "gol" in title or "fut-11" in classe:
                                    tipo_evento = "Gol"
                                    # verificar no próximo(s) textos se aparece "(pen.)" ou "(g.c.)"
                                    # Observação: às vezes o texto com (pen.) aparece na mesma div de minuto, tratamos adiante
                                elif "público" in title or "icn_zerozero2 grey" in classe:
                                    tipo_evento = "Assistência"
                                elif "amarel" in title or "icn_zerozero yellow" in classe:
                                    tipo_evento = "Cartão Amarelo"
                                elif "vermelh" in title or "icn_zerozero red" in classe:
                                    tipo_evento = "Cartão Vermelho"
                                elif "entrou" in title or texto_icone == "7":
                                    tipo_evento = "Entrou"
                                elif texto_icone == "8":
                                    # 8 costuma representar substituição
                                    tipo_evento = "Substituição"
                                else:
                                    # caso não saiba, pula
                                    tipo_evento = None

                                # procurar o(s) div(s) seguintes que contenham minutos/texto
                                minutos_texto = ""
                                j = i + 1
                                while j < len(children) and children[j].name == "div":
                                    minutos_texto += " " + children[j].get_text(" ", strip=True)
                                    j += 1

                                minutos_texto = minutos_texto.strip()

                                # se for gol, detectar (pen.) ou (g.c.) no minutos_texto
                                if tipo_evento == "Gol":
                                    if "(pen.)" in minutos_texto.lower() or "(pen.)" in texto_icone.lower():
                                        tipo_gol = "Penalti"
                                    elif "(g.c.)" in minutos_texto.lower() or "(g.c.)" in texto_icone.lower():
                                        tipo_gol = "Gol Contra"
                                    else:
                                        tipo_gol = "Normal"

                                # extrair múltiplos tempos — regex robusta:
                                # exemplos que queremos pegar: "35' (pen.)48' 67' ", "45+3'", "70", "45' 60'"
                                # usamos regex para capturar padrões de minuto, incluindo "45+2", "45+3"
                                if minutos_texto:
                                    # substitui apostrofos por espaços para normalizar e procurar padrões
                                    # vamos procurar por padrões como 45+2, 45, 85, etc
                                    tempos = re.findall(r"(\d+\+?\d*)'", minutos_texto)
                                    if not tempos:
                                        # às vezes o site coloca sem apóstrofo: "67 " ou "67' " — tentamos pegar números isolados
                                        tempos = re.findall(r"(\d+\+?\d*)", minutos_texto)
                                    # se tempos vazios e tipo_evento identificado (ex: assistência sem minuto visível), gravar um evento sem minuto
                                    if tempos:
                                        for t in tempos:
                                            t_clean = t.replace("'", "").strip()
                                            self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, t_clean)
                                    else:
                                        # registra sem minuto explícito
                                        self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)
                                else:
                                    # sem div de minuto, registra sem minuto (algumas vezes o ícone aparece sozinho)
                                    self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)

                                # avançar i até j (pular as divs de minuto que já consumimos)
                                i = j
                            else:
                                # se não for span (pode ser div vazio), apenas avança
                                i += 1

        # ---------------- TREINADORES ----------------
        if len(rows) > 2:
            terceira_linha = rows[2]
            colunas = terceira_linha.find_all("div", class_=lambda c: c and "zz-tpl-col" in c)

            for idx, coluna in enumerate(colunas):
                clube_id = mandante_id if idx == 0 else visitante_id
                link_tag = coluna.find("a", href=lambda x: x and "/treinador/" in x)
                if link_tag:
                    try:
                        treinador_id = self.processar_treinador(urljoin(self.base_url, link_tag["href"]))
                    except Exception as e:
                        print(f"⚠️ Erro ao processar treinador ({link_tag['href']}): {e}")
                        treinador_id = None
                else:
                    print(f"⚠️ Nenhum treinador encontrado para clube_id {clube_id}")
                    treinador_id = None

                self.treinadores_em_partida_lista.append({
                    'partida_id': partida_id,
                    'treinador_id': treinador_id,
                    'clube_id': clube_id,
                    'titular': 1 if treinador_id else 0
                })

        return estadio_id

    # ======================================================
    # Salvar CSVs
    # ======================================================

    def salvar_csvs(self):
        def append_rows(path, campos, rows):
            existe = os.path.exists(path)
            registros_existentes = set()
            if existe:
                with open(path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        # Cria chave única pela combinação de todos os valores
                        chave = tuple(r[c].strip() for c in campos if c in r)
                        registros_existentes.add(chave)
            novas_linhas = []
            for r in rows:
                chave = tuple(str(r.get(c, "")).strip() for c in campos)
                if chave not in registros_existentes:
                    novas_linhas.append(r)
                    registros_existentes.add(chave)

            if not novas_linhas:
                return

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=campos)
                if not existe:
                    w.writeheader()
                w.writerows(novas_linhas)

        # Entidades: gravar apenas os novos (buffers), depois limpar buffers
        if self._novo_clube:
            path = os.path.join(self.output_dir, "clubes.csv")
            campos = ['id','clube','apelido','local_id','fundacao','ativo']
            append_rows(path, campos, self._novo_clube)
            self._novo_clube.clear()
            print("💾 clubes.csv atualizado")

        if self._novo_estadio:
            path = os.path.join(self.output_dir, "estadios.csv")
            campos = ['id','estadio','capacidade','local_id','inauguracao','ativo']
            append_rows(path, campos, self._novo_estadio)
            self._novo_estadio.clear()
            print("💾 estadios.csv atualizado")

        if self._novo_jogador:
            path = os.path.join(self.output_dir, "jogadores.csv")
            campos = ['id','nome','nascimento','falecimento','nacionalidade', 'naturalidade', 'altura','peso','posicao','pe_preferido','aposentado']
            append_rows(path, campos, self._novo_jogador)
            self._novo_jogador.clear()
            print("💾 jogadores.csv atualizado")

        if self._novo_treinador:
            path = os.path.join(self.output_dir, "treinadores.csv")
            campos = ['id', 'nome', 'nascimento', 'falecimento','nacionalidade', 'naturalidade', 'situacao']
            append_rows(path, campos, self._novo_treinador)
            self._novo_treinador.clear()
            print("💾 treinadores.csv atualizado")

        if self._novo_arbitro:
            path = os.path.join(self.output_dir, "arbitros.csv")
            campos = ['id', 'nome', 'nascimento', 'falecimento','nacionalidade', 'naturalidade', 'situacao']
            append_rows(path, campos, self._novo_arbitro)
            self._novo_arbitro.clear()
            print("💾 arbitros.csv atualizado")

        # Locais (re-escrever inteiramente só na primeira vez ou quando houver novos)
        if self.locais_dict:
            path = os.path.join(self.output_dir, "locais.csv")
            campos = ['id','cidade','uf','estado','regiao','pais']
            # reescrever: para locais é mais simples reescrever todo o arquivo (geralmente pequeno)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=campos)
                w.writeheader()
                for v in self.locais_dict.values():
                    w.writerow(v)
            print("💾 locais.csv reescrito")

        # Relacionais: partidas, jogadores_em_partida, treinadores_em_partida, eventos_partida append
        if self.partidas_lista:
            path = os.path.join(self.output_dir, "partidas.csv")
            campos = ['id','edicao_id', 'campeonato_id', 'data','hora','fase', 'rodada', 'estadio_id', 'mandante_id','visitante_id','mandante_placar','visitante_placar', 'mandante_penalti', 'visitante_penalti', 'prorrogacao']
            append_rows(path, campos, self.partidas_lista)
            self.partidas_lista.clear()
            print("💾 partidas.csv atualizado")

        if self.jogadores_em_partida_lista:
            path = os.path.join(self.output_dir, "jogadores_em_partida.csv")
            campos = ['partida_id','jogador_id','clube_id','titular','posicao_jogada','numero_camisa']
            append_rows(path, campos, self.jogadores_em_partida_lista)
            self.jogadores_em_partida_lista.clear()
            print("💾 jogadores_em_partida.csv atualizado")

        if self.treinadores_em_partida_lista:
            path = os.path.join(self.output_dir, "treinadores_em_partida.csv")
            campos = ['partida_id','treinador_id','clube_id','titular']
            append_rows(path, campos, self.treinadores_em_partida_lista)
            self.treinadores_em_partida_lista.clear()
            print("💾 treinadores_em_partida.csv atualizado")

        if self.arbitros_em_partida_lista:
            path = os.path.join(self.output_dir, "arbitros_em_partida.csv")
            campos = ['partida_id','arbitro_id']
            append_rows(path, campos, self.arbitros_em_partida_lista)
            self.arbitros_em_partida_lista.clear()
            print("💾 arbitros_em_partida.csv atualizado")

        if self.eventos_partida_lista:
            path = os.path.join(self.output_dir, "eventos_partida.csv")
            campos = ['id','partida_id','jogador_id','clube_id', 'tipo_evento', 'tipo_gol', 'minuto']
            append_rows(path, campos, self.eventos_partida_lista)
            self.eventos_partida_lista.clear()
            print("💾 eventos_partida.csv atualizado")

        self._novo_clube.clear()
        self._novo_estadio.clear()
        self._novo_jogador.clear()
        self._novo_treinador.clear()
        self._novo_arbitro.clear()


    # ======================================================
    # Execução principal
    # ======================================================

    def executar(self, edicao_id=1):
        """Execução principal do scraper"""
        print("🚀 Iniciando scraping...")
        ultimo_jogo = None
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                ultimo_jogo = f.read().strip()
                if ultimo_jogo:
                    print(f"🔁 Retomando após: {ultimo_jogo}")

        # 1. Ler lista de partidas
        soup = self._get_soup(self.url_lista)
        tabela = soup.find("table", class_="zztable stats")
        if not tabela:
            print("❌ Tabela de partidas não encontrada")
            return

        skip = bool(ultimo_jogo)
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

            if skip:
                if link_partida == ultimo_jogo:
                    skip = False
                continue

            print(f"\n{'='*60}")
            print(f"⚽ {mandante_nome} x {visitante_nome}")
            print(f"{'='*60}")

            mandante_id = self.processar_clube(link_mandante)
            visitante_id = self.processar_clube(link_visitante)

            if not (mandante_id and visitante_id):
                print("⚠️ Erro ao processar clubes, pulando partida")
                # Salva buffers antes de pular
                self.salvar_csvs()
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
            self.next_partida_id += 1

            # processar detalhes da partida (escalação, eventos, estádio)
            estadio_id = None
            try:
                estadio_id = self.processar_detalhes_partida(link_partida, partida_id, mandante_id, visitante_id)
            except Exception as e:
                print("⚠️ Erro ao processar detalhes:", e)

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

            # Checkpoint (salva link da partida processada)
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write(link_partida or "")

            # Salvar incrementalmente tudo que está nos buffers
            self.salvar_csvs()

        print("\n✅ Scraping concluído!")

if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1971/2477/calendario"
    scraper = OGolScraperRelacional(url)
    scraper.executar(edicao_id=1)  # edicao_id=1 corresponde ao ano 1971
