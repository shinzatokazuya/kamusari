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

        # Dicionários usando chaves de atributos (não URLs!)
        self.clubes_dict = {}
        self.estadios_dict = {}
        self.jogadores_dict = {}
        self.treinadores_dict = {}
        self.arbitros_dict = {}
        self.locais_dict = {}

        # Cache de URLs para esta sessão (evita reprocessar mesma URL)
        self.url_cache = {
            'jogadores': {},
            'treinadores': {},
            'arbitros': {},
            'clubes': {},
            'estadios': {}
        }

        # Buffers de novos registros
        self._novo_clube = []
        self._novo_estadio = []
        self._novo_jogador = []
        self._novo_treinador = []
        self._novo_arbitro = []

        # Caminho dos CSVs
        self.output_dir = "output_csvs"
        os.makedirs(self.output_dir, exist_ok=True)

        # Carrega IDs existentes
        self._carregar_ids_existentes()

        # Buffers relacionais
        self.partidas_lista = []
        self.jogadores_em_partida_lista = []
        self.treinadores_em_partida_lista = []
        self.arbitros_em_partida_lista = []
        self.eventos_partida_lista = []

        # Caminho do CHECKPOINT
        self.checkpoint_path = os.path.join(self.output_dir, "checkpoint.txt")

    def _carregar_ids_existentes(self):
        """
        Carrega os IDs existentes dos CSVs usando chaves baseadas em atributos.
        Isso garante que não tenhamos duplicações independente de URLs diferentes.
        """
        print("📂 Carregando IDs e registros existentes dos CSVs...")

        # Inicializa contadores
        self.next_clube_id = 1
        self.next_estadio_id = 1
        self.next_jogador_id = 1
        self.next_treinador_id = 1
        self.next_arbitro_id = 1
        self.next_local_id = 1
        self.next_partida_id = 1
        self.next_evento_id = 1

        def obter_max_id(filename, id_field='id'):
            path = os.path.join(self.output_dir, filename)
            if not os.path.exists(path):
                return 0

            max_id = 0
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        current_id = int(row.get(id_field, 0))
                        if current_id > max_id:
                            max_id = current_id
                    except (ValueError, TypeError):
                        continue
            return max_id

        # ========== CARREGA LOCAIS ==========
        path_locais = os.path.join(self.output_dir, 'locais.csv')
        if os.path.exists(path_locais):
            with open(path_locais, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['cidade']}_{row['uf']}"
                    self.locais_dict[chave] = {
                        'id': int(row['id']),
                        'cidade': row['cidade'],
                        'uf': row['uf'],
                        'estado': row['estado'],
                        'regiao': row['regiao'],
                        'pais': row['pais']
                    }
            self.next_local_id = obter_max_id('locais.csv') + 1
            print(f"   ✓ {len(self.locais_dict)} locais carregados")

        # ========== CARREGA CLUBES ==========
        path_clubes = os.path.join(self.output_dir, 'clubes.csv')
        if os.path.exists(path_clubes):
            with open(path_clubes, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['clube']}_{row.get('local_id', '')}"
                    self.clubes_dict[chave] = {
                        'id': int(row['id']),
                        'clube': row['clube'],
                        'apelido': row.get('apelido', ''),
                        'local_id': int(row['local_id']) if row.get('local_id') else None,
                        'fundacao': row.get('fundacao', ''),
                        'ativo': int(row.get('ativo', 1))
                    }
            self.next_clube_id = obter_max_id('clubes.csv') + 1
            print(f"   ✓ {len(self.clubes_dict)} clubes carregados")

        # ========== CARREGA ESTÁDIOS ==========
        path_estadios = os.path.join(self.output_dir, 'estadios.csv')
        if os.path.exists(path_estadios):
            with open(path_estadios, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['estadio']}_{row.get('local_id', '')}"
                    self.estadios_dict[chave] = {
                        'id': int(row['id']),
                        'estadio': row['estadio'],
                        'capacidade': int(row['capacidade']) if row.get('capacidade') else None,
                        'local_id': int(row['local_id']) if row.get('local_id') else None,
                        'inauguracao': row.get('inauguracao', ''),
                        'ativo': int(row.get('ativo', 1))
                    }
            self.next_estadio_id = obter_max_id('estadios.csv') + 1
            print(f"   ✓ {len(self.estadios_dict)} estádios carregados")

        # ========== CARREGA JOGADORES ==========
        path_jogadores = os.path.join(self.output_dir, 'jogadores.csv')
        if os.path.exists(path_jogadores):
            with open(path_jogadores, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['nome']}_{row.get('nascimento', '')}"
                    self.jogadores_dict[chave] = {
                        'id': int(row['id']),
                        'nome': row['nome'],
                        'nascimento': row.get('nascimento', ''),
                        'falecimento': row.get('falecimento', ''),
                        'nacionalidade': row.get('nacionalidade', ''),
                        'naturalidade': row.get('naturalidade', ''),
                        'altura': int(row['altura']) if row.get('altura') and row['altura'] != '0' else None,
                        'peso': int(row['peso']) if row.get('peso') and row['peso'] != '0' else None,
                        'posicao': row.get('posicao', ''),
                        'pe_preferido': row.get('pe_preferido', ''),
                        'aposentado': int(row.get('aposentado', 0))
                    }
            self.next_jogador_id = obter_max_id('jogadores.csv') + 1
            print(f"   ✓ {len(self.jogadores_dict)} jogadores carregados")

        # ========== CARREGA TREINADORES ==========
        path_treinadores = os.path.join(self.output_dir, 'treinadores.csv')
        if os.path.exists(path_treinadores):
            with open(path_treinadores, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['nome']}_{row.get('nascimento', '')}"
                    self.treinadores_dict[chave] = {
                        'id': int(row['id']),
                        'nome': row['nome'],
                        'nascimento': row.get('nascimento', ''),
                        'falecimento': row.get('falecimento', ''),
                        'nacionalidade': row.get('nacionalidade', ''),
                        'naturalidade': row.get('naturalidade', ''),
                        'aposentado': row.get('aposentado', '')
                    }
            self.next_treinador_id = obter_max_id('treinadores.csv') + 1
            print(f"   ✓ {len(self.treinadores_dict)} treinadores carregados")

        # ========== CARREGA ÁRBITROS ==========
        path_arbitros = os.path.join(self.output_dir, 'arbitros.csv')
        if os.path.exists(path_arbitros):
            with open(path_arbitros, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    chave = f"{row['nome']}_{row.get('nascimento', '')}"
                    self.arbitros_dict[chave] = {
                        'id': int(row['id']),
                        'nome': row['nome'],
                        'nascimento': row.get('nascimento', ''),
                        'falecimento': row.get('falecimento', ''),
                        'nacionalidade': row.get('nacionalidade', ''),
                        'naturalidade': row.get('naturalidade', ''),
                        'aposentado': row.get('aposentado', '')
                    }
            self.next_arbitro_id = obter_max_id('arbitros.csv') + 1
            print(f"   ✓ {len(self.arbitros_dict)} árbitros carregados")

        self.next_partida_id = obter_max_id('partidas.csv') + 1
        self.next_evento_id = obter_max_id('eventos_partida.csv') + 1

        print(f"\n   📊 Próximos IDs: Clube={self.next_clube_id}, Estádio={self.next_estadio_id}, "
              f"Jogador={self.next_jogador_id}, Treinador={self.next_treinador_id}, "
              f"Árbitro={self.next_arbitro_id}, Local={self.next_local_id}, "
              f"Partida={self.next_partida_id}, Evento={self.next_evento_id}\n")

    def _get_soup(self, url):
        """Faz a requisição HTTP com tratamento de erros."""
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
                    print(f"⚠️ Erro 429. Aguardando {espera}s...")
                    time.sleep(espera)
                    continue
                r.raise_for_status()
                return BeautifulSoup(r.text, "html.parser")
            except requests.exceptions.RequestException as e:
                tentativa += 1
                espera = delay * (tentativa + 1)
                print(f"⚠️ Tentativa {tentativa} falhou. Aguardando {espera}s...")
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

        if "(" in cidade_completa and ")" in cidade_completa:
            cidade = cidade_completa.split("(")[0].strip()
            uf = cidade_completa.split("(")[1].replace(")", "").strip()
        else:
            cidade = cidade_completa
            uf = ""

        chave = f"{cidade}_{uf}"

        # Se já existe com essa chave exata, retorna
        if chave in self.locais_dict:
            return self.locais_dict[chave]['id']

        # Se a UF está vazia (informação incompleta), verifica se já existe
        # algum registro com esse nome de cidade (com UF preenchida)
        if not uf:
            for chave_existente, local_existente in self.locais_dict.items():
                if local_existente['cidade'] == cidade and local_existente['uf']:
                    # Encontrou uma cidade com mesmo nome mas com UF preenchida
                    # Retorna esse registro ao invés de criar um vazio
                    print(f"   ℹ️ Local '{cidade}' encontrado com UF '{local_existente['uf']}', reutilizando...")
                    return local_existente['id']

        # Se chegou aqui, pode criar o novo local
        # Mas só cria se tiver pelo menos a UF (informação mínima)
        if not uf:
            print(f"   ⚠️ Local '{cidade}' sem UF, não será criado")
            return None

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
            'estado': uf,
            'regiao': regioes.get(uf, ''),
            'pais': 'Brasil'
        }
        print(f"   ➤ Local '{cidade}, {uf}' adicionado.")
        self.next_local_id += 1

        return self.locais_dict[chave]['id']

    # ======================================================
    # Processadores usando chaves de atributos
    # ======================================================

    def processar_clube(self, url_clube):
        """Processa clube usando chave de atributos (nome + local_id)"""
        if not url_clube:
            return None

        # Verifica cache de URL desta sessão
        if url_clube in self.url_cache['clubes']:
            return self.url_cache['clubes'][url_clube]

        print(f"🏟️ Processando clube: {url_clube}")
        try:
            soup = self._get_soup(url_clube)
        except Exception as e:
            print(f"❌ Erro ao acessar URL: {e}")
            return None

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
            if not valor:
                a = div.find("a")
                valor = a.get_text(strip=True) if a else None

            if "Nome" in campo and "nome" not in dados:
                dados["nome"] = valor
            elif "Apelido" in campo:
                dados["apelido"] = valor
            elif "Ano de Fundação" in campo:
                dados["fundacao"] = valor
            elif "Cidade" in campo:
                dados["cidade"] = valor

        local_id = self._get_ou_criar_local(dados.get("cidade", ""))

        # Cria chave de atributos
        chave_atributos = f"{dados.get('nome', '')}_{local_id}"

        # Verifica se clube já existe
        if chave_atributos in self.clubes_dict:
            clube_id = self.clubes_dict[chave_atributos]['id']
            print(f"   ✓ Clube já existente: {dados.get('nome', '')} (ID: {clube_id})")
            self.url_cache['clubes'][url_clube] = clube_id
            return clube_id

        # Clube novo
        clube_id = self.next_clube_id
        print(f"   ➕ Novo clube: {dados.get('nome', '')} (ID: {clube_id})")

        registro = {
            'id': clube_id,
            'clube': dados.get('nome', ''),
            'apelido': dados.get('apelido', ''),
            'local_id': local_id,
            'fundacao': dados.get('fundacao', ''),
            'ativo': 1
        }

        self.clubes_dict[chave_atributos] = registro
        self.url_cache['clubes'][url_clube] = clube_id
        self._novo_clube.append(registro)
        self.next_clube_id += 1
        return clube_id

    def processar_estadio(self, url_estadio):
        """Processa estádio usando chave de atributos (nome + local_id)"""
        if not url_estadio:
            return None

        # Verifica cache de URL desta sessão
        if url_estadio in self.url_cache['estadios']:
            return self.url_cache['estadios'][url_estadio]

        print(f"🏟️ Processando estádio: {url_estadio}")
        try:
            soup = self._get_soup(url_estadio)
        except Exception as e:
            print(f"❌ Erro ao acessar URL: {e}")
            return None

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
                try:
                    dados["capacidade"] = int(re.sub(r"[^\d]", "", valor))
                except:
                    dados["capacidade"] = None

        local_id = self._get_ou_criar_local(dados.get("cidade", ""))

        # Cria chave de atributos
        chave_atributos = f"{dados.get('nome', '')}_{local_id}"

        # Verifica se estádio já existe
        if chave_atributos in self.estadios_dict:
            estadio_id = self.estadios_dict[chave_atributos]['id']
            print(f"   ✓ Estádio já existente: {dados.get('nome', '')} (ID: {estadio_id})")
            self.url_cache['estadios'][url_estadio] = estadio_id
            return estadio_id

        # Estádio novo
        estadio_id = self.next_estadio_id
        print(f"   ➕ Novo estádio: {dados.get('nome', '')} (ID: {estadio_id})")

        registro = {
            'id': estadio_id,
            'estadio': dados.get('nome', ''),
            'capacidade': dados.get('capacidade'),
            'local_id': local_id,
            'inauguracao': dados.get('inauguracao', ''),
            'ativo': 1
        }

        self.estadios_dict[chave_atributos] = registro
        self.url_cache['estadios'][url_estadio] = estadio_id
        self._novo_estadio.append(registro)
        self.next_estadio_id += 1
        return estadio_id

    def processar_jogador(self, url_jogador):
        """Processa jogador usando chave de atributos (nome + nascimento)"""
        if not url_jogador:
            return None

        # Verifica cache de URL desta sessão
        if url_jogador in self.url_cache['jogadores']:
            return self.url_cache['jogadores'][url_jogador]

        print(f"⚽ Processando jogador: {url_jogador}")
        try:
            soup = self._get_soup(url_jogador)
        except Exception as e:
            print(f"❌ Erro ao acessar URL: {e}")
            return None

        container = soup.find("div", id="entity_bio")
        if not container:
            print("⚠️ Container de biografia não encontrado")
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
                try:
                    dados["altura"] = int(re.sub(r"[^\d]", "", valor))
                except:
                    dados["altura"] = None
            elif "Peso" in campo:
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

        # Cria chave de atributos
        chave_atributos = f"{dados.get('nome', '')}_{dados.get('nascimento', '')}"

        # Verifica se jogador já existe
        if chave_atributos in self.jogadores_dict:
            jogador_id = self.jogadores_dict[chave_atributos]['id']
            print(f"   ✓ Jogador já existente: {dados.get('nome', '')} (ID: {jogador_id})")
            self.url_cache['jogadores'][url_jogador] = jogador_id
            return jogador_id

        # Jogador novo
        jogador_id = self.next_jogador_id
        print(f"   ➕ Novo jogador: {dados.get('nome', '')} (ID: {jogador_id})")

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

        self.jogadores_dict[chave_atributos] = registro
        self.url_cache['jogadores'][url_jogador] = jogador_id
        self._novo_jogador.append(registro)
        self.next_jogador_id += 1
        return jogador_id

    def processar_treinador(self, url_treinador):
        """Processa treinador usando chave de atributos (nome + nascimento)"""
        if not url_treinador:
            return None

        # Verifica cache de URL desta sessão
        if url_treinador in self.url_cache['treinadores']:
            return self.url_cache['treinadores'][url_treinador]

        print(f"👔 Processando treinador: {url_treinador}")
        try:
            soup = self._get_soup(url_treinador)
        except Exception as e:
            print(f"❌ Erro ao acessar URL: {e}")
            return None

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
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Naturalidade" in campo:
                dados['naturalidade'] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                    dados["aposentado"] = 1
                else:
                    dados["aposentado"] = 1

        # Cria chave de atributos
        chave_atributos = f"{dados.get('nome', '')}_{dados.get('nascimento', '')}"

        # Verifica se treinador já existe
        if chave_atributos in self.treinadores_dict:
            treinador_id = self.treinadores_dict[chave_atributos]['id']
            print(f"   ✓ Treinador já existente: {dados.get('nome', '')} (ID: {treinador_id})")
            self.url_cache['treinadores'][url_treinador] = treinador_id
            return treinador_id

        # Treinador novo
        treinador_id = self.next_treinador_id
        print(f"   ➕ Novo treinador: {dados.get('nome', '')} (ID: {treinador_id})")

        registro = {
            'id': treinador_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'naturalidade': dados.get('naturalidade', ''),
            'nacionalidade': dados.get('nacionalidade', ''),
            'aposentado': dados.get('aposentado', '')
        }

        self.treinadores_dict[chave_atributos] = registro
        self.url_cache['treinadores'][url_treinador] = treinador_id
        self._novo_treinador.append(registro)
        self.next_treinador_id += 1
        return treinador_id

    def processar_arbitro(self, url_arbitro):
        """Processa árbitro usando chave de atributos (nome + nascimento)"""
        if not url_arbitro:
            return None

        # Verifica cache de URL desta sessão
        if url_arbitro in self.url_cache['arbitros']:
            return self.url_cache['arbitros'][url_arbitro]

        print(f"🧑‍⚖️ Processando árbitro: {url_arbitro}")
        try:
            soup = self._get_soup(url_arbitro)
        except Exception as e:
            print(f"❌ Erro ao acessar URL: {e}")
            return None

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
            elif "Data de Nascimento" in campo:
                dados["nascimento"] = valor
            elif "Nacionalidade" in campo:
                dados["nacionalidade"] = valor
            elif "Naturalidade" in campo:
                dados['naturalidade'] = valor
            elif "Situação" in campo:
                if "Falecido" in valor:
                    dados["falecimento"] = valor
                    dados["aposentado"] = 1
                else:
                    dados["aposentado"] = 1

        # Cria chave de atributos
        chave_atributos = f"{dados.get('nome', '')}_{dados.get('nascimento', '')}"

        # Verifica se árbitro já existe
        if chave_atributos in self.arbitros_dict:
            arbitro_id = self.arbitros_dict[chave_atributos]['id']
            print(f"   ✓ Árbitro já existente: {dados.get('nome', '')} (ID: {arbitro_id})")
            self.url_cache['arbitros'][url_arbitro] = arbitro_id
            return arbitro_id

        # Árbitro novo
        arbitro_id = self.next_arbitro_id
        print(f"   ➕ Novo árbitro: {dados.get('nome', '')} (ID: {arbitro_id})")

        registro = {
            'id': arbitro_id,
            'nome': dados.get('nome', ''),
            'nascimento': dados.get('nascimento', ''),
            'falecimento': dados.get('falecimento', ''),
            'naturalidade': dados.get('naturalidade', ''),
            'nacionalidade': dados.get('nacionalidade', ''),
            'aposentado': dados.get('aposentado', '')
        }

        self.arbitros_dict[chave_atributos] = registro
        self.url_cache['arbitros'][url_arbitro] = arbitro_id
        self._novo_arbitro.append(registro)
        self.next_arbitro_id += 1
        return arbitro_id

    def registrar_evento(self, partida_id, jogador_id, clube_id, tipo_evento, tipo_gol=None, minuto=None):
        """Registra evento evitando duplicação exata."""
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

        chave = (partida_id, jogador_id, tipo_evento, tipo_gol or '', minuto or '')
        existing_keys = {
            (e['partida_id'], e['jogador_id'], e['tipo_evento'], e['tipo_gol'], e['minuto'])
            for e in self.eventos_partida_lista
        }

        if chave not in existing_keys:
            self.eventos_partida_lista.append(evento)
            self.next_evento_id += 1
            print(f"   ➤ Evento '{tipo_evento}' registrado (minuto {minuto})")

    def processar_detalhes_partida(self, url_partida, partida_id, mandante_id, visitante_id):
        """Processa detalhes da partida com tratamento melhorado para múltiplos eventos."""
        if not url_partida:
            return None

        print(f"📋 Processando detalhes da partida: {url_partida}")
        try:
            soup = self._get_soup(url_partida)
        except Exception as e:
            print(f"❌ Falha ao acessar partida: {e}")
            return None

        estadio_id = None
        arbitro_id = None

        # Processa estádio e árbitro
        header = soup.find("div", class_="header")
        if header:
            for a_tag in header.find_all("a", href=True):
                link = urljoin(self.base_url, a_tag["href"])
                if "estadio" in link.lower():
                    try:
                        estadio_id = self.processar_estadio(link)
                    except Exception as e:
                        print(f"⚠️ Erro ao processar estádio: {e}")
                elif "arbitro" in link.lower():
                    try:
                        arbitro_id = self.processar_arbitro(link)
                    except Exception as e:
                        print(f"⚠️ Erro ao processar árbitro: {e}")

        self.arbitros_em_partida_lista.append({
            'partida_id': partida_id,
            'arbitro_id': arbitro_id
        })

        # Processa escalações
        game_report = soup.find("div", id="game_report")
        if not game_report:
            print("⚠️ Div 'game_report' não encontrada")
            return estadio_id

        rows = game_report.find_all("div", class_="zz-tpl-row game_report")
        if not rows:
            return estadio_id

        # TITULARES (primeira linha)
        primeira_linha = rows[0]
        colunas = primeira_linha.find_all("div", class_=lambda c: c and "zz-tpl-col" in c)

        for idx, coluna in enumerate(colunas):
            clube_id = mandante_id if idx == 0 else visitante_id

            for player_div in coluna.find_all("div", class_="player"):
                link_tag = player_div.find("a", href=lambda x: x and "/jogador/" in x)
                if not link_tag:
                    continue

                try:
                    jogador_id = self.processar_jogador(urljoin(self.base_url, link_tag["href"]))
                except Exception as e:
                    print(f"⚠️ Erro ao processar jogador: {e}")
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

                # PROCESSA EVENTOS
                events_div = player_div.find("div", class_="events")
                if not events_div:
                    continue

                spans = events_div.find_all("span")
                divs_minutos = events_div.find_all("div")

                for i, span in enumerate(spans):
                    tipo_evento, tipo_gol = None, None
                    title = span.get("title", "").strip().lower()
                    classe = " ".join(span.get("class", [])).lower()
                    texto_icone = span.get_text(strip=True)

                    if "gol" in title or "fut-11" in classe:
                        tipo_evento = "Gol"
                        tipo_gol = "Normal"
                    elif "público" in title or texto_icone == "B":
                        tipo_evento = "Assistência"
                    elif ("amarel" in title or "icn_zerozero yellow" in classe) and texto_icone == "R":
                        tipo_evento = "Cartão Amarelo"
                    elif texto_icone == "S":
                        tipo_evento = "Segundo Amarelo"
                    elif texto_icone == "R" and "icn_zerozero red" in classe:
                        tipo_evento = "Cartão Vermelho"
                    elif "entrou" in title or texto_icone == "7":
                        tipo_evento = "Entrou"
                    elif texto_icone == "8":
                        tipo_evento = "Substituição"

                    if not tipo_evento:
                        continue

                    if i >= len(divs_minutos):
                        continue

                    texto_completo = divs_minutos[i].get_text(strip=True)

                    if tipo_evento == "Gol":
                        texto_normalizado = texto_completo.strip()
                        padrao_minutos = r"(\d+(?:\+\d+)?)'?\s*(?:\(([^)]+)\))?"
                        matches = re.findall(padrao_minutos, texto_normalizado)

                        if matches:
                            for minuto_bruto, modificador in matches:
                                minuto_limpo = minuto_bruto.strip()

                                if modificador:
                                    mod_lower = modificador.lower()
                                    if "pen" in mod_lower:
                                        tipo_gol = "Penalti"
                                    elif "g.c" in mod_lower or "contra" in mod_lower:
                                        tipo_gol = "Gol Contra"
                                    else:
                                        tipo_gol = "Normal"
                                else:
                                    tipo_gol = "Normal"

                                self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, minuto_limpo)
                        else:
                            self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)
                    else:
                        match_minuto = re.search(r'(\d+(?:\+\d+)?)', texto_completo)
                        minuto = match_minuto.group(1) if match_minuto else None
                        self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, minuto)

        # RESERVAS (segunda linha)
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
                    except:
                        continue

                    numero = player_div.find("div", class_="number")
                    numero_camisa = int(numero.get_text(strip=True)) if numero and numero.get_text(strip=True).isdigit() else None

                    self.jogadores_em_partida_lista.append({
                        'partida_id': partida_id,
                        'jogador_id': jogador_id,
                        'clube_id': clube_id,
                        'titular': 0,
                        'posicao_jogada': '',
                        'numero_camisa': numero_camisa
                    })

                    events_div = player_div.find("div", class_="events")
                    if events_div:
                        spans = events_div.find_all("span")
                        divs_minutos = events_div.find_all("div")

                        for i, span in enumerate(spans):
                            tipo_evento, tipo_gol = None, None
                            title = span.get("title", "").strip().lower()
                            classe = " ".join(span.get("class", [])).lower()
                            texto_icone = span.get_text(strip=True)

                            if "gol" in title or "fut-11" in classe:
                                tipo_evento = "Gol"
                                tipo_gol = "Normal"
                            elif "público" in title or texto_icone == "B":
                                tipo_evento = "Assistência"
                            elif ("amarel" in title or "icn_zerozero yellow" in classe) and texto_icone == "R":
                                tipo_evento = "Cartão Amarelo"
                            elif texto_icone == "S":
                                tipo_evento = "Segundo Amarelo"
                            elif texto_icone == "R" and "icn_zerozero red" in classe:
                                tipo_evento = "Cartão Vermelho"
                            elif "entrou" in title or texto_icone == "7":
                                tipo_evento = "Entrou"
                            elif texto_icone == "8":
                                tipo_evento = "Substituição"

                            if not tipo_evento:
                                continue

                            if i >= len(divs_minutos):
                                continue

                            texto_completo = divs_minutos[i].get_text(strip=True)

                            if tipo_evento == "Gol":
                                texto_normalizado = texto_completo.strip()
                                padrao_minutos = r"(\d+(?:\+\d+)?)'?\s*(?:\(([^)]+)\))?"
                                matches = re.findall(padrao_minutos, texto_normalizado)

                                if matches:
                                    for minuto_bruto, modificador in matches:
                                        minuto_limpo = minuto_bruto.strip()

                                        if modificador:
                                            mod_lower = modificador.lower()
                                            if "pen" in mod_lower:
                                                tipo_gol = "Penalti"
                                            elif "g.c" in mod_lower or "contra" in mod_lower:
                                                tipo_gol = "Gol Contra"
                                            else:
                                                tipo_gol = "Normal"
                                        else:
                                            tipo_gol = "Normal"

                                        self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, minuto_limpo)
                                else:
                                    self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, None)
                            else:
                                match_minuto = re.search(r'(\d+(?:\+\d+)?)', texto_completo)
                                minuto = match_minuto.group(1) if match_minuto else None
                                self.registrar_evento(partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, minuto)

        # TREINADORES (terceira linha)
        # SEMPRE registrar os treinadores, mesmo que a linha não exista
        treinador_mandante_id = None
        treinador_visitante_id = None

        # Primeiro, tentamos processar a terceira linha se ela existir
        if len(rows) > 2:
            terceira_linha = rows[2]
            colunas = terceira_linha.find_all("div", class_=lambda c: c and "zz-tpl-col" in c)

            for idx, coluna in enumerate(colunas):
                link_tag = coluna.find("a", href=lambda x: x and "/treinador/" in x)

                if link_tag:
                    try:
                        treinador_id = self.processar_treinador(urljoin(self.base_url, link_tag["href"]))
                        # Salva na variável apropriada
                        if idx == 0:
                            treinador_mandante_id = treinador_id
                        else:
                            treinador_visitante_id = treinador_id
                    except Exception as e:
                        print(f"⚠️ Erro ao processar treinador: {e}")

        # SEMPRE registramos os dois clubes, independente de ter encontrado treinadores
        # Isso garante que sempre teremos 2 registros por partida
        self.treinadores_em_partida_lista.append({
            'partida_id': partida_id,
            'treinador_id': treinador_mandante_id,
            'clube_id': mandante_id,
            'tipo': "Titular" if treinador_mandante_id else ""
        })

        self.treinadores_em_partida_lista.append({
            'partida_id': partida_id,
            'treinador_id': treinador_visitante_id,
            'clube_id': visitante_id,
            'tipo': "Titular" if treinador_visitante_id else ""
        })

        # Mensagens informativas para acompanhar o processo
        if treinador_mandante_id:
            print(f"   ✓ Treinador mandante registrado: clube_id={mandante_id}, treinador_id={treinador_mandante_id}")
        else:
            print(f"   ℹ️ Sem treinador mandante: clube_id={mandante_id}, treinador_id=None")

        if treinador_visitante_id:
            print(f"   ✓ Treinador visitante registrado: clube_id={visitante_id}, treinador_id={treinador_visitante_id}")
        else:
            print(f"   ℹ️ Sem treinador visitante: clube_id={visitante_id}, treinador_id=None")

        return estadio_id

    # ======================================================
    # Salvar CSVs
    # ======================================================

    def salvar_csvs(self):
        """Salva os CSVs de forma incremental, evitando duplicatas."""
        def append_rows(path, campos, rows):
            existe = os.path.exists(path)
            registros_existentes = set()

            if existe:
                with open(path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
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

        # Salva entidades
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
            campos = ['id','nome','nascimento','falecimento','nacionalidade','naturalidade','altura','peso','posicao','pe_preferido','aposentado']
            append_rows(path, campos, self._novo_jogador)
            self._novo_jogador.clear()
            print("💾 jogadores.csv atualizado")

        if self._novo_treinador:
            path = os.path.join(self.output_dir, "treinadores.csv")
            campos = ['id','nome','nascimento','falecimento','nacionalidade','naturalidade','aposentado']
            append_rows(path, campos, self._novo_treinador)
            self._novo_treinador.clear()
            print("💾 treinadores.csv atualizado")

        if self._novo_arbitro:
            path = os.path.join(self.output_dir, "arbitros.csv")
            campos = ['id','nome','nascimento','falecimento','nacionalidade','naturalidade','aposentado']
            append_rows(path, campos, self._novo_arbitro)
            self._novo_arbitro.clear()
            print("💾 arbitros.csv atualizado")

        # Salva locais (reescreve sempre pois é pequeno)
        if self.locais_dict:
            path = os.path.join(self.output_dir, "locais.csv")
            campos = ['id','cidade','uf','estado','regiao','pais']
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=campos)
                w.writeheader()
                for v in self.locais_dict.values():
                    w.writerow(v)
            print("💾 locais.csv reescrito")

        # Salva relacionais
        if self.partidas_lista:
            path = os.path.join(self.output_dir, "partidas.csv")
            campos = ['id','edicao_id','campeonato_id','data','hora','fase','rodada','estadio_id','mandante_id','visitante_id','mandante_placar','visitante_placar','mandante_penalti','visitante_penalti','prorrogacao']
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
            campos = ['partida_id','treinador_id','clube_id','tipo']
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
            campos = ['id','partida_id','jogador_id','clube_id','tipo_evento','tipo_gol','minuto']
            append_rows(path, campos, self.eventos_partida_lista)
            self.eventos_partida_lista.clear()
            print("💾 eventos_partida.csv atualizado")

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

        soup = self._get_soup(self.url_lista)
        tabela = soup.find("table", class_="zztable stats")
        if not tabela:
            print("❌ Tabela de partidas não encontrada")
            return

        skip = bool(ultimo_jogo)

        for linha in tabela.find_all("tr"):
            celulas = linha.find_all("td")
            if len(celulas) < 6:
                continue

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
                self.salvar_csvs()
                continue

            # Parse do placar
            placar_split = placar.strip().upper()
            if "WO" in placar_split or "ANU" in placar_split or "IC" in placar_split:
                mandante_placar, visitante_placar = '-', '-'
                penalti_mandante = penalti_visitante = None
                prorrogacao = 0
            else:
                placar_split = placar.strip().lower()
                penalti_mandante = penalti_visitante = None
                prorrogacao = 0

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

            estadio_id = None
            try:
                estadio_id = self.processar_detalhes_partida(link_partida, partida_id, mandante_id, visitante_id)
            except Exception as e:
                print(f"⚠️ Erro ao processar detalhes: {e}")

            self.partidas_lista.append({
                'id': partida_id,
                'edicao_id': edicao_id,
                'campeonato_id': 1,
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

            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write(link_partida or "")

            self.salvar_csvs()

        print("\n✅ Scraping concluído!")


if __name__ == "__main__":
    url = "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1973/2482/calendario?fase_in=0&equipa=0&estado=1&filtro=&page=1&op=calendario"
    scraper = OGolScraperRelacional(url)
    scraper.executar(edicao_id=2)
