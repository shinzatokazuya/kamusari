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
            info_items = player_info.find_all()

