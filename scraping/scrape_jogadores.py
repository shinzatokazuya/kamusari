import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re

class OGolScraper:
    """
    Classe para fazer scraping de dados de partidas do site OGol.
    Extrai informações de jogadores, reservas e treinadores.
    """

    def __init__(self, url):
        self.url = url
        self.partida_id = self._extrair_partida_id(url)
        self.data_partida = self._extrair_data_partida(url)

    def _extrair_partida_id(self, url):
        """Extrai o ID da partida da URL"""
        match = re.search(r'/(\d+)$', url)
        return int(match.group(1)) if match else None

    def _extrair_data_partida(self, url):
        """Extrai a data da partida da URL"""
        match = re.search(r'/(\d{4}-\d{2}-\d{2})-', url)
        return match.group(1) if match else None

    def buscar_pagina(self):
        """Faz a requisição HTTP e retorna o conteúdo da página"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            print(f"Erro ao buscar a página: {e}")
            return None

    def extrair_jogadores(self, soup):
        """
        Extrai informações dos jogadores titulares de ambos os times.
        Retorna uma lista de dicionários com os dados dos jogadores.
        """
        jogadores_dados = []
        jogador_id_counter = 1

        # Encontra as seções dos dois times (Bahia e Santos)
        game_report = soup.find('div', id='game_report')
        if not game_report:
            print("Seção de relatório do jogo não encontrada")
            return []

        # Procura pelas colunas dos times
        colunas_times = game_report.find_all('div', class_='zz-tpl-col is-6 fl-c')

        for idx_time, coluna_time in enumerate(colunas_times[:2]):  # Apenas os dois primeiros (titulares)
            # Extrai o nome do time
            subtitle = coluna_time.find('div', class_='subtitle')
            if not subtitle:
                continue

            nome_time = subtitle.text.strip()
            clube_id = idx_time + 1  # ID simples baseado na ordem (1 para Bahia, 2 para Santos)

            # Encontra todos os jogadores
            jogadores = coluna_time.find_all('div', class_='player')

            for jogador_div in jogadores:
                # Extrai o nome do jogador
                link_jogador = jogador_div.find('a', href=re.compile(r'/jogador/'))
                if not link_jogador:
                    continue

                nome_jogador = link_jogador.text.strip()
                url_jogador = link_jogador.get('href', '')

                # Extrai a nacionalidade
                flag_span = jogador_div.find('span', class_=re.compile(r'flag:'))
                nacionalidade = None
                if flag_span:
                    classes = flag_span.get('class', [])
                    for cls in classes:
                        if cls.startswith('flag:'):
                            nacionalidade = cls.split(':')[1]
                            break

                # Verifica se foi substituído (saiu do jogo)
                events_div = jogador_div.find('div', class_='events')
                saiu_jogo = False
                if events_div and events_div.find('span', class_='icn_zerozero'):
                    saiu_jogo = True

                jogadores_dados.append({
                    'jogador_id': jogador_id_counter,
                    'nome': nome_jogador,
                    'nacionalidade': nacionalidade,
                    'clube': nome_time,
                    'clube_id': clube_id,
                    'titular': True,
                    'saiu_jogo': saiu_jogo,
                    'url': url_jogador
                })

                jogador_id_counter += 1

        return jogadores_dados, jogador_id_counter

    def extrair_reservas(self, soup, jogador_id_counter):
        """
        Extrai informações dos jogadores reservas que entraram no jogo.
        """
        reservas_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        # Procura pela seção de reservas
        rows = game_report.find_all('div', class_='zz-tpl-row game_report')

        for row in rows:
            # Verifica se é a linha de reservas
            subtitle = row.find('div', class_='subtitle')
            if subtitle and 'Reservas' in subtitle.text:
                colunas = row.find_all('div', class_='zz-tpl-col is-6 fl-c')

                for idx_time, coluna in enumerate(colunas):
                    clube_id = idx_time + 1

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
                        entrou_jogo = False
                        if events_div and events_div.find('span', title='Entrou'):
                            entrou_jogo = True

                        reservas_dados.append({
                            'jogador_id': jogador_id_counter,
                            'nome': nome_jogador,
                            'nacionalidade': nacionalidade,
                            'clube_id': clube_id,
                            'titular': False,
                            'entrou_jogo': entrou_jogo,
                            'url': url_jogador
                        })

                        jogador_id_counter += 1

        return reservas_dados, jogador_id_counter

    def extrair_treinadores(self, soup):
        """
        Extrai informações dos treinadores de ambos os times.
        """
        treinadores_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        # Procura pela seção de treinadores
        rows = game_report.find_all('div', class_='zz-tpl-row game_report')

        for row in rows:
            subtitle = row.find('div', class_='subtitle')
            if subtitle and 'Treinadores' in subtitle.text:
                colunas = row.find_all('div', class_='zz-tpl-col is-6 fl-c')

                for idx_time, coluna in enumerate(colunas):
                    clube_id = idx_time + 1

                    link_treinador = coluna.find('a', href=re.compile(r'/treinador/'))
                    if link_treinador:
                        nome_treinador = link_treinador.text.strip()

                        # Extrai nacionalidade
                        flag_span = coluna.find('span', class_=re.compile(r'flag:'))
                        nacionalidade = None
                        if flag_span:
                            classes = flag_span.get('class', [])
                            for cls in classes:
                                if cls.startswith('flag:'):
                                    nacionalidade = cls.split(':')[1]
                                    break

                        treinadores_dados.append({
                            'nome': nome_treinador,
                            'nacionalidade': nacionalidade,
                            'clube_id': clube_id
                        })

        return treinadores_dados

    def exportar_para_csv(self, jogadores, reservas, treinadores):
        """
        Exporta os dados para arquivos CSV seguindo o schema do banco de dados.
        """
        # CSV para a tabela 'jogadores'
        with open('jogadores.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'nome', 'nascimento', 'nacionalidade', 'clube_id'])

            # Escreve jogadores titulares
            for jogador in jogadores:
                writer.writerow([
                    jogador['jogador_id'],
                    jogador['nome'],
                    '',  # nascimento não disponível nesta página
                    jogador['nacionalidade'],
                    jogador['clube_id']
                ])

            # Escreve reservas
            for reserva in reservas:
                writer.writerow([
                    reserva['jogador_id'],
                    reserva['nome'],
                    '',
                    reserva['nacionalidade'],
                    reserva['clube_id']
                ])

        print(f"✓ Arquivo 'jogadores.csv' criado com {len(jogadores) + len(reservas)} jogadores")

        # CSV para a tabela 'jogadores_em_partida'
        with open('jogadores_em_partida.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['partida_id', 'jogador_id', 'titular', 'minutos_jogados', 'gols', 'assistencias'])

            # Escreve jogadores titulares
            for jogador in jogadores:
                writer.writerow([
                    self.partida_id,
                    jogador['jogador_id'],
                    1,  # titular = True
                    '',  # minutos não disponíveis nesta página
                    '',  # gols não disponíveis
                    ''   # assistências não disponíveis
                ])

            # Escreve reservas que entraram
            for reserva in reservas:
                if reserva.get('entrou_jogo', False):
                    writer.writerow([
                        self.partida_id,
                        reserva['jogador_id'],
                        0,  # titular = False
                        '',
                        '',
                        ''
                    ])

        print(f"✓ Arquivo 'jogadores_em_partida.csv' criado para partida ID {self.partida_id}")

        # CSV adicional para treinadores (não estava no schema original)
        with open('treinadores_partida.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['nome', 'nacionalidade', 'clube_id', 'partida_id'])

            for treinador in treinadores:
                writer.writerow([
                    treinador['nome'],
                    treinador['nacionalidade'],
                    treinador['clube_id'],
                    self.partida_id
                ])

        print(f"✓ Arquivo 'treinadores_partida.csv' criado com {len(treinadores)} treinadores")

    def executar(self):
        """Executa o processo completo de scraping e exportação"""
        print(f"Iniciando scraping da URL: {self.url}")
        print(f"Data da partida: {self.data_partida}")
        print(f"ID da partida: {self.partida_id}\n")

        soup = self.buscar_pagina()
        if not soup:
            print("Erro: Não foi possível buscar a página")
            return

        # Extrai os dados
        print("Extraindo jogadores titulares...")
        jogadores, counter = self.extrair_jogadores(soup)
        print(f"  → {len(jogadores)} jogadores titulares encontrados")

        print("Extraindo reservas...")
        reservas, _ = self.extrair_reservas(soup, counter)
        print(f"  → {len(reservas)} reservas encontrados")

        print("Extraindo treinadores...")
        treinadores = self.extrair_treinadores(soup)
        print(f"  → {len(treinadores)} treinadores encontrados\n")

        # Exporta para CSV
        print("Exportando dados para CSV...")
        self.exportar_para_csv(jogadores, reservas, treinadores)

        print("\n✓ Scraping concluído com sucesso!")
        return jogadores, reservas, treinadores


# Exemplo de uso
if __name__ == "__main__":
    url = "https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100"

    scraper = OGolScraper(url)
    jogadores, reservas, treinadores = scraper.executar()

    # Exibe um resumo dos dados extraídos
    print("\n" + "="*60)
    print("RESUMO DOS DADOS EXTRAÍDOS")
    print("="*60)
    print(f"\nJogadores titulares: {len(jogadores)}")
    print(f"Reservas: {len(reservas)}")
    print(f"Treinadores: {len(treinadores)}")
