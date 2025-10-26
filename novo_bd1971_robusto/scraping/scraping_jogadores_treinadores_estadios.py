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

        # Cache para evitar requisições repetidas
        self.cache_jogadores = {}

        # Contadores para IDs quando necessário criar novos registros
        self.proximo_jogador_id = 1
        self.proximo_treinador_id = 1

        # Delay entre requisições para não sobrecarregar o servidor
        self.delay_requisicao = 2  # segundos

    def _carregar_clubes(self, clubes_source):
        """
        Carrega dados dos clubes do CSV fornecido.
        Retorna dicionário {nome_clube: dados_completos}
        """
        clubes = {}

        if isinstance(clubes_source, str):
            # Assume que é um caminho de arquivo CSV
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

            time.sleep(self.delay_requisicao)  # Respeita rate limit

            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            return BeautifulSoup(response.content, 'html.parser')

        except requests.RequestException as e:
            print(f"  ⚠ Erro ao acessar {url}: {e}")
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
        game_header = soup.find('div', class_='game_header')
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
                    estadio_match = re.search(r'[Ee]stádio[:\s]+([^,\n]+)', text)
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

    def extrair_dados_jogador_detalhado(self, url_jogador, nome_jogador):
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

        dados = {
            'nome_completo': nome_jogador,
            'nascimento': None,
            'altura': None,
            'posicao': None,
            'pe_preferido': None
        }

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

        # Extrai dados da partida para contexto
        dados_partida = self.extrair_dados_partida(soup)

        # Processa titulares de ambos os times
        colunas_times = game_report.find_all('div', class_='zz-tpl-col is-6 fl-c')

        for idx_time, coluna_time in enumerate(colunas_times[:2]):
            subtitle = coluna_time.find('div', class_='subtitle')
            if not subtitle:
                continue

            nome_time = subtitle.text.strip()
            clube_id = self.identificar_clube_id(nome_time)

            # Determina se é mandante ou visitante
            tipo_time = 'mandante' if idx_time == 0 else 'visitante'

            print(f"\n  🔵 Time: {nome_time} (ID: {clube_id}) - {tipo_time}")

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
                            nacionalidade = cls.split(':')[1]
                            break

                # Busca dados detalhados do jogador
                dados_detalhados = self.extrair_dados_jogador_detalhado(
                    url_jogador,
                    nome_jogador
                )

                # Verifica se foi substituído
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

    def extrair_reservas_completo(self, soup):
        """
        Extrai informações completas dos reservas que entraram no jogo.
        """
        print("\n📋 Extraindo dados dos reservas...")
        reservas_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        rows = game_report.find_all('div', class_='zz-tpl-row game_report')

        for row in rows:
            subtitle = row.find('div', class_='subtitle')
            if subtitle and 'Reservas' in subtitle.text:
                colunas = row.find_all('div', class_='zz-tpl-col is-6 fl-c')

                for idx_time, coluna in enumerate(colunas):
                    # Identifica o time baseado na ordem
                    dados_partida = self.extrair_dados_partida(soup)
                    nome_time = dados_partida['mandante'] if idx_time == 0 else dados_partida['visitante']
                    clube_id = self.identificar_clube_id(nome_time)

                    jogadores = coluna.find_all('div', class_='player')

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
                                    nacionalidade = cls.split(':')[1]
                                    break

                        # Verifica se entrou no jogo
                        events_div = jogador_div.find('div', class_='events')
                        entrou_jogo = bool(events_div and events_div.find('span', title='Entrou'))

                        # Busca dados detalhados
                        dados_detalhados = self.extrair_dados_jogador_detalhado(
                            url_jogador,
                            nome_jogador
                        )

                        registro = {
                            'jogador_id': self.proximo_jogador_id,
                            'nome': nome_jogador,
                            'nacionalidade': nacionalidade,
                            'clube': nome_time,
                            'clube_id': clube_id,
                            'titular': False,
                            'entrou_jogo': entrou_jogo,
                            'url': url_jogador
                        }

                        if dados_detalhados:
                            registro.update({
                                'nascimento': dados_detalhados.get('nascimento'),
                                'altura': dados_detalhados.get('altura'),
                                'posicao': dados_detalhados.get('posicao'),
                                'pe_preferido': dados_detalhados.get('pe_preferido')
                            })

                        reservas_dados.append(registro)
                        self.proximo_jogador_id += 1

        print(f"  ✓ Total de reservas extraídos: {len(reservas_dados)}")
        return reservas_dados

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

    def exportar_para_csv(self, dados_partida, jogadores, reservas, treinadores, partida_id):
        """
        Exporta todos os dados para arquivos CSV organizados.
        """
        print("\n💾 Exportando dados para CSV...")

        # Arquivo para tabela jogadores (dados mestres)
        with open('novo_bd1971_robusto/csv/jogadores.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nascimento', 'nacionalidade', 'clube_id',
                     'altura', 'posicao', 'pe_preferido']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            todos_jogadores = jogadores + reservas
            for jog in todos_jogadores:
                writer.writerow({
                    'ID': jog['jogador_id'],
                    'nome': jog['nome'],
                    'nascimento': jog.get('nascimento', ''),
                    'nacionalidade': jog.get('nacionalidade', ''),
                    'clube_id': jog.get('clube_id', ''),
                    'altura': jog.get('altura', ''),
                    'posicao': jog.get('posicao', ''),
                    'pe_preferido': jog.get('pe_preferido', '')
                })

        print(f"  ✓ jogadores.csv criado com {len(todos_jogadores)} registros")

        # Arquivo para tabela jogadores_em_partida
        with open('novo_bd1971_robusto/csv/jogadores_em_partida.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'jogador_id', 'titular', 'minutos_jogados',
                     'gols', 'assistencias']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for jog in todos_jogadores:
                # Só inclui se foi titular ou se entrou no jogo
                if jog['titular'] or jog.get('entrou_jogo', False):
                    writer.writerow({
                        'partida_id': partida_id,
                        'jogador_id': jog['jogador_id'],
                        'titular': 1 if jog['titular'] else 0,
                        'minutos_jogados': '',  # Não disponível na página
                        'gols': '',
                        'assistencias': ''
                    })

        print(f"  ✓ jogadores_em_partida.csv criado para partida {partida_id}")

        # Arquivo para tabela treinadores
        with open('novo_bd1971_robusto/csv/treinadores.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nascimento', 'nacionalidade']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for trein in treinadores:
                writer.writerow({
                    'ID': trein['treinador_id'],
                    'nome': trein['nome'],
                    'nascimento': '',  # Não disponível nesta página
                    'nacionalidade': trein.get('nacionalidade', '')
                })

        print(f"  ✓ treinadores.csv criado com {len(treinadores)} registros")

        # Arquivo para relacionamento treinadores_em_partida
        with open('novo_bd1971_robusto/csv/treinadores_em_partida.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'treinador_id', 'clube_id']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for trein in treinadores:
                writer.writerow({
                    'partida_id': partida_id,
                    'treinador_id': trein['treinador_id'],
                    'clube_id': trein.get('clube_id', '')
                })

        print(f"  ✓ treinadores_em_partida.csv criado")

        # Arquivo com dados complementares da partida (estádio, etc)
        with open('novo_bd1971_robusto/csv/partida_detalhes.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'estadio', 'cidade', 'data', 'placar_mandante',
                     'placar_visitante']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            writer.writerow({
                'partida_id': partida_id,
                'estadio': dados_partida.get('estadio', ''),
                'cidade': dados_partida.get('cidade', ''),
                'data': dados_partida.get('data', ''),
                'placar_mandante': dados_partida.get('placar_mandante', ''),
                'placar_visitante': dados_partida.get('placar_visitante', '')
            })

        print(f"  ✓ partida_detalhes.csv criado")

    def executar(self, partida_id):
        """
        Executa o scraping completo da partida.

        Args:
            partida_id: ID da partida no seu banco de dados
        """
        print("="*70)
        print(f"🔍 INICIANDO SCRAPING AVANÇADO")
        print("="*70)
        print(f"URL: {self.url_partida}")
        print(f"Partida ID (seu banco): {partida_id}")
        print(f"Delay entre requisições: {self.delay_requisicao}s")
        print("="*70)

        # Busca página principal
        soup = self._fazer_requisicao(self.url_partida)
        if not soup:
            print("\n❌ Erro: Não foi possível acessar a página da partida")
            return None

        # Extrai dados gerais da partida
        print("\n📊 Extraindo informações gerais da partida...")
        dados_partida = self.extrair_dados_partida(soup)

        print(f"\n  Mandante: {dados_partida['mandante']}")
        print(f"  Visitante: {dados_partida['visitante']}")
        print(f"  Placar: {dados_partida['placar_mandante']} x {dados_partida['placar_visitante']}")
        print(f"  Estádio: {dados_partida['estadio']}")
        print(f"  Cidade: {dados_partida['cidade']}")
        print(f"  Data: {dados_partida['data']}")

        # Extrai todos os dados
        jogadores = self.extrair_jogadores_completo(soup)
        reservas = self.extrair_reservas_completo(soup)
        treinadores = self.extrair_treinadores(soup)

        # Exporta tudo
        self.exportar_para_csv(dados_partida, jogadores, reservas, treinadores, partida_id)

        print("\n" + "="*70)
        print("✅ SCRAPING CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print(f"\nResumo:")
        print(f"  • Jogadores titulares: {len(jogadores)}")
        print(f"  • Reservas: {len(reservas)}")
        print(f"  • Treinadores: {len(treinadores)}")
        print(f"  • Requisições realizadas: {len(self.cache_jogadores) + 1}")
        print("="*70)

        return {
            'partida': dados_partida,
            'jogadores': jogadores,
            'reservas': reservas,
            'treinadores': treinadores
        }


# ===== EXEMPLO DE USO =====

if __name__ == "__main__":
    # URL da partida que você quer extrair
    url_partida = "https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100"

    # Caminho para o CSV de clubes
    caminho_clubes = "csv/clubes.csv"

    # ID da partida no seu banco de dados
    # Você deve buscar isso do seu banco antes de rodar o scraper
    partida_id = 1  # Exemplo - substitua pelo ID real

    # Cria e executa o scraper
    scraper = OGolScraperAvancado(url_partida, caminho_clubes)

    # Executa o scraping
    resultado = scraper.executar(partida_id)

    if resultado:
        print("\n🎯 Dados prontos para importação no banco de dados!")
