import requests
from bs4 import BeautifulSoup
import csv
import re
import time
from datetime import datetime
from urllib.parse import urljoin
import json

class OGolScraperAvancado:
    """
    Scraper avançado para extrair dados completos de partidas do OGol,
    incluindo navegação em páginas de jogadores para dados detalhados.
    Integra-se com banco de dados existente usando IDs corretos.
    """

    def __init__(self, url_partida, clubes_db, partidas_db=None):
        """
        Inicializa o scraper com a URL da partida e dados do banco.

        Args:
            url_partida: URL da página da partida no OGol
            clubes_db: Dicionário ou CSV com dados dos clubes {nome: {id, cidade, etc}}
            partidas_db: Dicionário opcional com dados da partida no seu banco
        """
        self.url_partida = url_partida
        self.base_url = "https://www.ogol.com.br"
        self.clubes_db = self._carregar_clubes(clubes_db)
        self.partidas_db = partidas_db or {}

        # Cache para evitar requisições duplicadas
        self.cache_jogadores = {}

        # Contadores para IDs quando necessário criar novos registros
        self.proximo_jogador_id = 1
        self.proximo_treinador_id = 1

        # Delay entre requisições para não sobrecarregar o servidor
        self.delay_requisicao = 5 # segundos

    def _carregar_clubes(self, clubes_source):
        """
        Carrega dados dos clubes do CSV fornecido.
        Retorna dicionário {nome_clube: dados_completos}
        """
        clubes = {}

        if isinstance(clubes_source, str):
            # Assume que é o caminho de arquivo CSV
            with open(clubes_source, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Normaliza o nome para facilitar correspondência
                    nome = row['clube'].strip()
                    clubes[nome] = row

                    # Adiciona variações comuns do nome
                    clubes[nome.lower()] = row
        else:
            clubes = clubes_source

        return clubes

    def _fazer_requisicao(self, url):
        """
        Faz requisição HTTP com tratamento de erros e rate limiting.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            time.sleep(self.delay_requisicao)

            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            return BeautifulSoup(response.content, 'html.parser')

        except requests.RequestException as e:
            print(f" ⚠ ERRO ao acessar {url}: {e}")
            return None

    def extrair_dados_partida(self, soup):
        """
        Extrai informações gerais da partida incluindo estádio, placar e data.
        """
        dados = {
            'estadio': None,
            'cidade': None,
            'data': None,
            'mandante': None,
            'visitante': None,
            'placar_mandante': None,
            'placar_visitante': None
        }

        # Extrai informações do cabeçalho da partida
        game_header = soup.find('div', class=_='game_header')
        if game_header:
            # Extrai nomes dos times
            teams = game_header.find_all('a', href=re.compile(r'/equipa/'))
            if len(teams) >= 2:
                dados['mandante'] = teams[0].text.strip()
                dados['visitante'] = teams[1].text.strip()

            # Extrai placar
            score = game_header.find('div', class_='score')
            if score:
                placar_text = score.text.strip()
                match = re.search(r'(\d+)\s*-\s*(\d+)', placar_text)
                if match:
                    dados['placar_mandante'] = int(match.group(1))
                    dados['placar_visitante'] = int(match.group(2))

        # Extrai informações do estádio e local
        game_info = soup.find('div', class_='game_info')
        if game_info:
            # Procura por informações de estádio
            info_lines = game_info.find_all('div', class_='info_line')
            for line in info_lines:
                text = line.text.strip()

                # Estádio
                if 'Estádio' in text or 'Stadium' in text:
                    estadio_match = re.search(r'[Ee]stádio[:\s]+([^, \n]+)', text)
                    if estadio_match:
                        dados['estadio'] = estadio_match.group(1).strip()

                # Cidade
                if any(palavra in text for palavra in ['Cidade', 'City', ',']):
                    # Tenta extrair cidade após vírgula ou palavra-chave
                    cidade_match = re.search(r'(?:Cidade[:\s]+|,\s*)([^,\n]+)', text)
                    if cidade_match:
                        dados['cidade'] = cidade_match.group(1).strip()

                # Data
                if any(c.isdigit() for c in text) and '/' in text:
                    data_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                    if data_match:
                        dados['data'] = data_match.group(1)

        return dados

    def extrair_dados_jogador_detalhado(self, url_jogador, nome_jogador);
        """
        Navega até a página do jogador e extrai dados detalhados.
        Usa cache para evitar requisições repetidas.
        """
        # Verifica cache primeiro
        if url_jogador in self.cache_jogadores:
            print(f"    ✓ Dados de {nome_jogador} recuperados do cache")
            return self.cache_jogadores[url_jogador]

        print(f"    → Buscando dados detalhados de {nome_jogador}...")

        # Constrói URL completa
        url_completa = urljoin(self.base_url, url_jogador)

        soup = self._fazer_requisicao(url_completa)
        if not soup:
            return None

        dados = [
            'nome_completo': nome_jodador,
            'nascimento': None,
            'altura': None,
            'posicao': None,
            'pe_preferido': None
        ]

        # Procura pela seção de informações do jogador
        player_info = soup.find('div', class_='player_info')
        if player_info:
            info_items = player_info.find_all('div', class_='info_item')

            for item in info_items:
                label = item.find('div', class_='label')
                value = item.find('div', class_='value')

                if label and value:
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip()

                    if 'nascimento' in label_text or 'data de nascimento' in label_text:
                        # Tenta extrair data de nascimento
                        data_match = re.search(r'(\d{2}/\d{2}/\d{4})', value_text)
                        if data_match:
                            dados['nascimento'] = data_match.group(1)

                    elif 'altura' in label_text:
                        altura_match = re.search(r'(\d+)', value_text)
                        if altura_match:
                            dados['altura'] = int(altura_match.group(1))

                    elif 'posição' in label_text or 'position' in label_text:
                        dados['posicao'] = value_text

                    elif 'pé' in label_text or 'foot' in label_text:
                        dados['pe_preferido'] = value_text

        # Armazena no cache
        self.cache_jogadores[url_jogador] = dados

        return dados

    def identificar_clube_id(self, nome_clube):
        """
        Identifica o ID do clube no banco de dados baseado no nome.
        Tenta várias variações para aumentar taxa de sucesso.
        """
        if not nome_clube:
            return None

        # Tenta correspondência exata primeiro
        if nome_clube in self.clubes_db:
            return int(self.clubes_db[nome_clube]['ID'])

        # Tenta versão normalizada (minúsculas)
        if nome_clube.lower() in self.clubes_db:
            return int(self.clubes_db[nome_clube.lower()]['ID'])

        # Tenta correspondência parcial
        nome_normalizado = nome_clube.lower().strip()
        for clube_nome, clube_dados in self.clubes_db.items():
            if nome_normalizado in clube_nome.lower():
                return int(clube_dados['ID'])

        print(f"  ⚠ Clube '{nome_clube}' não encontrado no banco de dados")
        return None

    def extrair_jogadores_completo(self, soup):
        """
        Extrai informações completas dos jogadores, incluindo dados detalhados
        navegando pelas páginas individuais.
        """
        print("\n📋 Extraindo dados dos jogadores titulares...")
        jogadores_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            print("  ⚠ Seção de relatório não encontrada")
            return []

        # Extrai dados da partida para o contexto
        dados_partida = self.extrair_dados_partida(soup)

        # Processa titulares de ambos os times
        colunas_times = game_report.find_all('div', class_='zz-tpl-col is-6 fl-c')

        for idx_time, coluna_time in enumerate(colunas_times[:2]):
            subtitle = coluna_time.find('div', class_='subtitle')
            if not subtitle:
                continue

            nome_time - subtitle.text.strip()
            clube_id = self.identificar_clube_id(nome_time)

            # Determina se é mandante ou visitante
            tipo_time = 'mandante' if idx_time == 0 else 'visitante'

            print(f"\n 🔵 Time: {nome_time} (ID: {clube_id}) - {tipo_time}")

            jogadores = coluna_time.find_all('div', class_='player')

            for jogador_div in jogadores:
                link_jogador = jogador_div.find('a', href=re.compile(r'/jogador/'))
                if not link_jogador:
                    continue

                nome_jogador = link_jogador.text.strip()
                url_jogador = link_jogador.get('href', '')

                # Extrai nacionalidade
                flag_span = jogador_div.find('span', class_=re.compile(r'flag:'))
                nacionalidade = None
                if flag_span:
                    classes = flag_span.get('class', [])
                    for cls in classes:
                        if cls.startswith('flag:'):
                            nacionalidade = cls.spilt(':')[1]
                            break

                # Busca dados detalhados do jogador
                dados_detalhados = self.extrair_dados_jogador_detalhado(
                    url_jogador,
                    nome_jogador
                )

                # Verifica se foi substituido
                events_div = jogador_div.find('div', class_='events')
                foi_substituido = bool(events_div and events_div.find('span', class_='icn_zerozero'))

                # Monta registro completo
                registro = {
                    'jogador_id': self.proximo_jogador_id,
                    'nome': nome_jogador,
                    'nacionalidade': nacionalidade,
                    'clube': nome_time,
                    'clube_id': clube_id,
                    'tipo_time': tipo_time,
                    'titular': True,
                    'foi_substituido': foi_substituido,
                    'url': url_jogador
                }

                # Adiciona dados detalhados se disponíveis
                if dados_detalhados:
                    registro.update({
                        'nascimento': dados_detalhados.get('nascimento'),
                        'altura': dados_detalhados.get('altura'),
                        'posicao': dados_detalhados.get('posicao'),
                        'pe_preferido': dados_detalhados.get('pe_preferido')
                    })

                jogadores_dados.append(registro)
                self.proximo_jogador_id += 1

        print(f"\n  ✓ Total de jogadores titulares extraídos: {len(jogadores_dados)}")
        return jogadores_dados

    def extrair_treinadores(self, soup):
        """
        Extrai informações dos treinadores.
        """
        print("\n📋 Extraindo dados dos treinadores...")
        treinadores_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        dados_partida = self.extrair_dados_partida(soup)
        rows = game_report.find_all('div', class_='zz-tpl-row game_report')

        for row in rows:
            subtitle = row.find('div', class_='subtitle')
            if subtitle and 'Treinadores' in subtitle.text:
                colunas = row.find_all('div', class_='zz-tpl-col is-6 fl-c')

                for idx_time, coluna in enumerate(colunas):
                    nome_time = dados_partida['mandante'] if idx_time == 0 else dados_partida['visitante']
                    clube_id = self.identificar_clube_id(nome_time)

                    link_treinador = coluna.find('a', href=re.compile(r'/treinador/'))
                    if link_treinador:
                        nome_treinador = link_treinador.text.strip()

                        flag_span = coluna.find('span', class_=re.compile(r'flag:'))
                        nacionalidade = None
                        if flag_span:
                            classes = flag_span.get('class', [])
                            for cls in classes:
                                if cls.startswith('flag:'):
                                    nacionalidade = cls.split(':')[1]
                                    break

                        treinadores_dados.append({
                            'treinador_id': self.proximo_treinador_id,
                            'nome': nome_treinador,
                            'nacionalidade': nacionalidade,
                            'clube': nome_time,
                            'clube_id': clube_id
                        })

                        self.proximo_treinador_id += 1

        print(f"  ✓ Total de treinadores extraídos: {len(treinadores_dados)}")
        return treinadores_dados

    def exportar_para_csv(self, dados_partidas, jogadores, reservas, treinadores, partida_id):
        """
        Exporta todos os dados para arquivos CSV organizados.
        """
        print("\n💾 Exportando dados para CSV...")

        # Arquivo para tabela jogadores (dados mestres)
        with open('jogadores.csv')






