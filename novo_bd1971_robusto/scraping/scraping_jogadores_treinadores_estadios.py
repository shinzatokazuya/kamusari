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
                dados['visitante'] = teams[0].text.strip()

            # Extrai

