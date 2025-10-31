import requests
from bs4 import BeautifulSoup
import csv
import re
import time
from datetime import datetime
from urllib.parse import urljoin
import json

class OGolScraperRobusto:
    """
    Scraper ultra robusto que usa múltiplas estratégias para extrair dados.
    Extrai informações completas de partidas, jogadores, treinadores e estatísticas.
    """

    def __init__(self, url_partida, clubes_db):
        self.url_partida = url_partida
        self.base_url = "https://www.ogol.com.br"
        self.clubes_db = self._carregar_clubes(clubes_db)

        # Cache
        self.cache_jogadores = {}

        # Contadores
        self.proximo_jogador_id = 1
        self.proximo_treinador_id = 1

        # Configurações
        self.delay_requisicao = 2
        self.timeout = 15

        # Dados extraídos
        self.soup_principal = None

    def _carregar_clubes(self, clubes_source):
        """Carrega dados dos clubes."""
        clubes = {}

        if isinstance(clubes_source, str):
            with open(clubes_source, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    nome = row['clube'].strip()
                    clubes[nome] = row
                    clubes[nome.lower()] = row
        else:
            clubes = clubes_source

        return clubes

    def _fazer_requisicao(self, url, tentativas=3):
        """Faz requisição HTTP com retry automático."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }

        for tentativa in range(tentativas):
            try:
                time.sleep(self.delay_requisicao)

                response = requests.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = 'utf-8'

                return BeautifulSoup(response.content, 'html.parser')

            except requests.Timeout:
                print(f"  ⏱️  Timeout na tentativa {tentativa + 1}/{tentativas}")
                if tentativa < tentativas - 1:
                    time.sleep(5)  # Espera mais tempo antes de tentar novamente
                continue

            except requests.RequestException as e:
                print(f"  ⚠ Erro na tentativa {tentativa + 1}/{tentativas}: {e}")
                if tentativa < tentativas - 1:
                    time.sleep(3)
                continue

        return None

    def extrair_dados_partida_completo(self, soup):
        """
        Extrai TODOS os dados da partida usando múltiplas estratégias.
        Tenta diferentes seletores CSS para máxima robustez.
        """
        dados = {
            'estadio': None,
            'cidade': None,
            'data': None,
            'hora': None,
            'mandante': None,
            'visitante': None,
            'placar_mandante': None,
            'placar_visitante': None,
            'publico': None,
            'arbitro': None,
            'assistente1': None,
            'assistente2': None
        }

        # === ESTRATÉGIA 1: Busca por texto completo ===
        texto_pagina = soup.get_text()

        # Extrai nomes dos times da URL como fallback
        url_match = re.search(r'/(\d{4}-\d{2}-\d{2})-([\w-]+)-([\w-]+)/', self.url_partida)
        if url_match:
            if not dados['data']:
                dados['data'] = url_match.group(1)
            if not dados['mandante']:
                dados['mandante'] = url_match.group(2).replace('-', ' ').title()
            if not dados['visitante']:
                dados['visitante'] = url_match.group(3).replace('-', ' ').title()

        # === ESTRATÉGIA 2: Busca por classes e IDs comuns ===
        # Procura pelo placar
        placar_elementos = soup.find_all(['div', 'span'], class_=re.compile(r'(score|placar|resultado)', re.I))
        for elem in placar_elementos:
            texto = elem.get_text(strip=True)
            placar_match = re.search(r'(\d+)\s*[-x:]\s*(\d+)', texto)
            if placar_match:
                dados['placar_mandante'] = int(placar_match.group(1))
                dados['placar_visitante'] = int(placar_match.group(2))
                print(f"  ✓ Placar encontrado: {dados['placar_mandante']} x {dados['placar_visitante']}")
                break

        # Procura pelos nomes dos times
        if not dados['mandante'] or not dados['visitante']:
            times_links = soup.find_all('a', href=re.compile(r'/equip[ae]/'))
            if len(times_links) >= 2:
                dados['mandante'] = times_links[0].get_text(strip=True)
                dados['visitante'] = times_links[1].get_text(strip=True)
                print(f"  ✓ Times encontrados: {dados['mandante']} vs {dados['visitante']}")

        # === ESTRATÉGIA 3: Busca por padrões de texto ===
        # Estádio
        estadio_patterns = [
            r'Estádio[:\s]+([^,\n\r]+)',
            r'Stadium[:\s]+([^,\n\r]+)',
            r'Local[:\s]+([^,\n\r]+)',
        ]
        for pattern in estadio_patterns:
            match = re.search(pattern, texto_pagina, re.I)
            if match:
                dados['estadio'] = match.group(1).strip()
                print(f"  ✓ Estádio encontrado: {dados['estadio']}")
                break

        # Data e hora
        if not dados['data']:
            data_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', texto_pagina)
            if data_match:
                dados['data'] = data_match.group(1)
                print(f"  ✓ Data encontrada: {dados['data']}")

        hora_match = re.search(r'(\d{1,2}:\d{2})', texto_pagina)
        if hora_match:
            dados['hora'] = hora_match.group(1)
            print(f"  ✓ Hora encontrada: {dados['hora']}")

        # Público
        publico_match = re.search(r'P[úu]blico[:\s]+(\d+[\.\s]*\d*)', texto_pagina, re.I)
        if publico_match:
            publico_str = publico_match.group(1).replace('.', '').replace(' ', '')
            dados['publico'] = int(publico_str)
            print(f"  ✓ Público encontrado: {dados['publico']}")

        # Árbitro
        arbitro_match = re.search(r'[ÁA]rbitro[:\s]+([^\n\r,]+)', texto_pagina, re.I)
        if arbitro_match:
            dados['arbitro'] = arbitro_match.group(1).strip()
            print(f"  ✓ Árbitro encontrado: {dados['arbitro']}")

        # === ESTRATÉGIA 4: Busca em divs de informações ===
        info_divs = soup.find_all(['div', 'section', 'article'], class_=re.compile(r'info', re.I))
        for div in info_divs:
            texto_div = div.get_text()

            # Cidade
            if not dados['cidade']:
                cidade_match = re.search(r'Cidade[:\s]+([^\n\r,]+)', texto_div, re.I)
                if cidade_match:
                    dados['cidade'] = cidade_match.group(1).strip()

        # === ESTRATÉGIA 5: Analisa tabelas HTML ===
        tabelas = soup.find_all('table')
        for tabela in tabelas:
            linhas = tabela.find_all('tr')
            for linha in linhas:
                texto_linha = linha.get_text()

                if 'estádio' in texto_linha.lower() and not dados['estadio']:
                    colunas = linha.find_all(['td', 'th'])
                    if len(colunas) >= 2:
                        dados['estadio'] = colunas[1].get_text(strip=True)

                if 'público' in texto_linha.lower() and not dados['publico']:
                    num_match = re.search(r'(\d+)', texto_linha)
                    if num_match:
                        dados['publico'] = int(num_match.group(1))

        return dados

    def extrair_estatisticas_partida(self, soup):
        """Extrai estatísticas detalhadas da partida (posse, chutes, etc)."""
        estatisticas = {
            'mandante': {},
            'visitante': {}
        }

        # Procura por seção de estatísticas
        stats_section = soup.find(['div', 'section'], class_=re.compile(r'(stat|estatistica)', re.I))

        if not stats_section:
            # Tenta encontrar por texto
            for div in soup.find_all('div'):
                if 'estatística' in div.get_text().lower() or 'posse de bola' in div.get_text().lower():
                    stats_section = div
                    break

        if stats_section:
            texto_stats = stats_section.get_text()

            # Posse de bola
            posse_match = re.search(r'(\d+)%?\s*[-x]\s*(\d+)%?', texto_stats)
            if posse_match:
                estatisticas['mandante']['posse_de_bola'] = posse_match.group(1) + '%'
                estatisticas['visitante']['posse_de_bola'] = posse_match.group(2) + '%'

            # Chutes
            chutes_patterns = [
                r'Chutes[:\s]+(\d+)\s*[-x]\s*(\d+)',
                r'Finalizações[:\s]+(\d+)\s*[-x]\s*(\d+)',
            ]
            for pattern in chutes_patterns:
                match = re.search(pattern, texto_stats, re.I)
                if match:
                    estatisticas['mandante']['chutes'] = int(match.group(1))
                    estatisticas['visitante']['chutes'] = int(match.group(2))
                    break

            # Escanteios
            escanteios_match = re.search(r'Escanteios[:\s]+(\d+)\s*[-x]\s*(\d+)', texto_stats, re.I)
            if escanteios_match:
                estatisticas['mandante']['escanteios'] = int(escanteios_match.group(1))
                estatisticas['visitante']['escanteios'] = int(escanteios_match.group(2))

        return estatisticas

    def extrair_eventos_partida(self, soup):
        """Extrai eventos cronológicos (gols, cartões, substituições)."""
        eventos = []

        # Procura pela timeline/eventos
        eventos_section = soup.find(['div', 'section'], class_=re.compile(r'(event|timeline|cronolog)', re.I))

        if eventos_section:
            # Procura por cada evento individual
            evento_divs = eventos_section.find_all(['div', 'li'], recursive=True)

            for div in evento_divs:
                texto = div.get_text()

                # Tenta extrair minuto
                minuto_match = re.search(r"(\d+)'", texto)
                if not minuto_match:
                    continue

                minuto = int(minuto_match.group(1))

                # Identifica tipo de evento
                tipo_evento = None
                if '⚽' in texto or 'gol' in texto.lower():
                    tipo_evento = 'gol'
                elif '🟨' in texto or 'amarelo' in texto.lower():
                    tipo_evento = 'cartao_amarelo'
                elif '🟥' in texto or 'vermelho' in texto.lower():
                    tipo_evento = 'cartao_vermelho'
                elif '🔄' in texto or 'substituição' in texto.lower() or 'saiu' in texto.lower():
                    tipo_evento = 'substituicao'

                if tipo_evento:
                    # Tenta extrair nome do jogador
                    jogador_link = div.find('a', href=re.compile(r'/jogador/'))
                    if jogador_link:
                        nome_jogador = jogador_link.get_text(strip=True)

                        eventos.append({
                            'minuto': minuto,
                            'tipo': tipo_evento,
                            'jogador': nome_jogador,
                            'descricao': texto.strip()
                        })

        return eventos

    def extrair_dados_jogador_detalhado(self, url_jogador, nome_jogador):
        """Navega até página do jogador para extrair dados completos."""
        if url_jogador in self.cache_jogadores:
            print(f"    ✓ Dados de {nome_jogador} do cache")
            return self.cache_jogadores[url_jogador]

        print(f"    → Buscando {nome_jogador}...")

        url_completa = urljoin(self.base_url, url_jogador)
        soup = self._fazer_requisicao(url_completa)

        if not soup:
            return None

        dados = {
            'nome_completo': nome_jogador,
            'nascimento': None,
            'altura': None,
            'peso': None,
            'posicao': None,
            'pe_preferido': None,
            'foto_url': None
        }

        # Extrai foto
        foto_img = soup.find('div', class_=re.compile(r'(zz-enthdr-media)', re.I))
        if foto_img and foto_img.get('src'):
            dados['foto_url'] = urljoin(self.base_url, foto_img['src'])

        # Busca informações em todo o texto
        texto_pagina = soup.get_text()

        # Data de nascimento
        nascimento_patterns = [
            r'Nascimento[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            r'Data de nascimento[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            r'Nascido[:\s]+em[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
        ]
        for pattern in nascimento_patterns:
            match = re.search(pattern, texto_pagina, re.I)
            if match:
                dados['nascimento'] = match.group(1)
                break

        # Altura
        altura_match = re.search(r'Altura[:\s]+(\d+)\s*cm', texto_pagina, re.I)
        if altura_match:
            dados['altura'] = int(altura_match.group(1))

        # Peso
        peso_match = re.search(r'Peso[:\s]+(\d+)\s*kg', texto_pagina, re.I)
        if peso_match:
            dados['peso'] = int(peso_match.group(1))

        # Posição
        posicao_match = re.search(r'Posição[:\s]+([\w\s]+?)(?:\n|\r|<)', texto_pagina, re.I)
        if posicao_match:
            dados['posicao'] = posicao_match.group(1).strip()

        # Pé preferido
        pe_match = re.search(r'P[ée] preferido[:\s]+([\w\s]+?)(?:\n|\r|<)', texto_pagina, re.I)
        if pe_match:
            dados['pe_preferido'] = pe_match.group(1).strip()

        self.cache_jogadores[url_jogador] = dados
        return dados

    def identificar_clube_id(self, nome_clube):
        """Identifica ID do clube no banco."""
        if not nome_clube:
            return None

        # Tenta correspondência exata
        if nome_clube in self.clubes_db:
            return int(self.clubes_db[nome_clube]['ID'])

        # Tenta normalizada
        if nome_clube.lower() in self.clubes_db:
            return int(self.clubes_db[nome_clube.lower()]['ID'])

        # Tenta correspondência parcial
        nome_normalizado = nome_clube.lower().strip()
        for clube_nome, clube_dados in self.clubes_db.items():
            if nome_normalizado in clube_nome.lower() or clube_nome.lower() in nome_normalizado:
                print(f"  ℹ️  '{nome_clube}' → '{clube_dados['clube']}' (ID: {clube_dados['ID']})")
                return int(clube_dados['ID'])

        print(f"  ⚠ Clube '{nome_clube}' não encontrado no BD")
        return None

    def extrair_jogadores_completo(self, soup):
        """Extrai informações completas dos jogadores titulares."""
        print("\n📋 Extraindo jogadores titulares...")
        jogadores_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            print("  ⚠ Seção game_report não encontrada, tentando alternativa...")
            # Tenta encontrar por outras formas
            game_report = soup.find(['div', 'section'], class_=re.compile(r'(lineup|escalacao|team)', re.I))

        if not game_report:
            print("  ❌ Não foi possível encontrar escalações")
            return []

        dados_partida = self.extrair_dados_partida_completo(soup)

        # Processa ambos os times
        colunas_times = game_report.find_all('div', class_=re.compile(r'(col|team|time)', re.I))[:2]

        for idx_time, coluna_time in enumerate(colunas_times):
            # Identifica nome do time
            nome_time = None
            subtitle = coluna_time.find(['div', 'h2', 'h3'], class_=re.compile(r'(subtitle|title|team-name)', re.I))
            if subtitle:
                nome_time = subtitle.get_text(strip=True)

            if not nome_time:
                nome_time = dados_partida['mandante'] if idx_time == 0 else dados_partida['visitante']

            clube_id = self.identificar_clube_id(nome_time)
            tipo_time = 'mandante' if idx_time == 0 else 'visitante'

            print(f"\n  🔵 {nome_time} (ID: {clube_id}) - {tipo_time}")

            # Encontra jogadores
            jogadores = coluna_time.find_all('div', class_=re.compile(r'player', re.I))

            for jogador_div in jogadores:
                link_jogador = jogador_div.find('a', href=re.compile(r'/jogador/'))
                if not link_jogador:
                    continue

                nome_jogador = link_jogador.get_text(strip=True)
                url_jogador = link_jogador.get('href', '')

                # Nacionalidade
                flag_span = jogador_div.find('span', class_=re.compile(r'flag:', re.I))
                nacionalidade = None
                if flag_span:
                    classes = flag_span.get('class', [])
                    for cls in classes:
                        if 'flag:' in str(cls).lower():
                            nacionalidade = str(cls).split(':')[1].upper()
                            break

                # Número da camisa (se disponível)
                numero_div = jogador_div.find('div', class_=re.compile(r'number', re.I))
                numero_camisa = None
                if numero_div:
                    num_match = re.search(r'\d+', numero_div.get_text())
                    if num_match:
                        numero_camisa = int(num_match.group())

                # Verifica eventos (substituição, cartões)
                events_div = jogador_div.find('div', class_=re.compile(r'event', re.I))
                foi_substituido = bool(events_div and events_div.find('span'))

                # Dados detalhados
                dados_detalhados = self.extrair_dados_jogador_detalhado(url_jogador, nome_jogador)

                registro = {
                    'jogador_id': self.proximo_jogador_id,
                    'nome': nome_jogador,
                    'nacionalidade': nacionalidade,
                    'clube': nome_time,
                    'clube_id': clube_id,
                    'tipo_time': tipo_time,
                    'titular': True,
                    'foi_substituido': foi_substituido,
                    'numero_camisa': numero_camisa,
                    'url': url_jogador
                }

                if dados_detalhados:
                    registro.update(dados_detalhados)

                jogadores_dados.append(registro)
                self.proximo_jogador_id += 1

        print(f"\n  ✓ Total: {len(jogadores_dados)} jogadores")
        return jogadores_dados

    def extrair_reservas_completo(self, soup):
        """Extrai reservas que entraram."""
        print("\n📋 Extraindo reservas...")
        reservas_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        rows = game_report.find_all('div', class_=re.compile(r'row', re.I))

        for row in rows:
            subtitle = row.find(['div', 'h3'], class_=re.compile(r'subtitle', re.I))
            if subtitle and 'reserva' in subtitle.get_text().lower():
                colunas = row.find_all('div', class_=re.compile(r'col', re.I))[:2]

                dados_partida = self.extrair_dados_partida_completo(soup)

                for idx_time, coluna in enumerate(colunas):
                    nome_time = dados_partida['mandante'] if idx_time == 0 else dados_partida['visitante']
                    clube_id = self.identificar_clube_id(nome_time)

                    jogadores = coluna.find_all('div', class_=re.compile(r'player', re.I))

                    for jogador_div in jogadores:
                        link_jogador = jogador_div.find('a', href=re.compile(r'/jogador/'))
                        if not link_jogador:
                            continue

                        nome_jogador = link_jogador.get_text(strip=True)
                        url_jogador = link_jogador.get('href', '')

                        # Nacionalidade
                        flag_span = jogador_div.find('span', class_=re.compile(r'flag:', re.I))
                        nacionalidade = None
                        if flag_span:
                            classes = flag_span.get('class', [])
                            for cls in classes:
                                if 'flag:' in str(cls).lower():
                                    nacionalidade = str(cls).split(':')[1].upper()
                                    break

                        # Verifica se entrou
                        events_div = jogador_div.find('div', class_=re.compile(r'event', re.I))
                        entrou_jogo = bool(events_div)

                        dados_detalhados = self.extrair_dados_jogador_detalhado(url_jogador, nome_jogador)

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
                            registro.update(dados_detalhados)

                        reservas_dados.append(registro)
                        self.proximo_jogador_id += 1

        print(f"  ✓ Total: {len(reservas_dados)} reservas")
        return reservas_dados

    def extrair_treinadores(self, soup):
        """Extrai treinadores."""
        print("\n📋 Extraindo treinadores...")
        treinadores_dados = []

        game_report = soup.find('div', id='game_report')
        if not game_report:
            return []

        dados_partida = self.extrair_dados_partida_completo(soup)
        rows = game_report.find_all('div', class_=re.compile(r'row', re.I))

        for row in rows:
            subtitle = row.find(['div', 'h3'], class_=re.compile(r'subtitle', re.I))
            if subtitle and 'treinador' in subtitle.get_text().lower():
                colunas = row.find_all('div', class_=re.compile(r'col', re.I))[:2]

                for idx_time, coluna in enumerate(colunas):
                    nome_time = dados_partida['mandante'] if idx_time == 0 else dados_partida['visitante']
                    clube_id = self.identificar_clube_id(nome_time)

                    link_treinador = coluna.find('a', href=re.compile(r'/treinador/'))
                    if link_treinador:
                        nome_treinador = link_treinador.get_text(strip=True)

                        flag_span = coluna.find('span', class_=re.compile(r'flag:', re.I))
                        nacionalidade = None
                        if flag_span:
                            classes = flag_span.get('class', [])
                            for cls in classes:
                                if 'flag:' in str(cls).lower():
                                    nacionalidade = str(cls).split(':')[1].upper()
                                    break

                        treinadores_dados.append({
                            'treinador_id': self.proximo_treinador_id,
                            'nome': nome_treinador,
                            'nacionalidade': nacionalidade,
                            'clube': nome_time,
                            'clube_id': clube_id
                        })

                        self.proximo_treinador_id += 1

        print(f"  ✓ Total: {len(treinadores_dados)} treinadores")
        return treinadores_dados

    def exportar_para_csv(self, dados_partida, jogadores, reservas, treinadores,
                          estatisticas, eventos, partida_id):
        """Exporta TODOS os dados para CSVs."""
        print("\n💾 Exportando dados...")

        import os
        os.makedirs('csv_extraidos', exist_ok=True)

        # Jogadores
        with open('csv_extraidos/jogadores.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nome_completo', 'nascimento', 'nacionalidade',
                     'altura', 'peso', 'posicao', 'pe_preferido', 'clube_id', 'url_foto']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            todos_jogadores = jogadores + reservas
            for jog in todos_jogadores:
                writer.writerow({
                    'ID': jog['jogador_id'],
                    'nome': jog['nome'],
                    'nome_completo': jog.get('nome_completo', ''),
                    'nascimento': jog.get('nascimento', ''),
                    'nacionalidade': jog.get('nacionalidade', ''),
                    'altura': jog.get('altura', ''),
                    'peso': jog.get('peso', ''),
                    'posicao': jog.get('posicao', ''),
                    'pe_preferido': jog.get('pe_preferido', ''),
                    'clube_id': jog.get('clube_id', ''),
                    'url_foto': jog.get('foto_url', '')
                })

        print(f"  ✓ jogadores.csv ({len(todos_jogadores)} registros)")

        # Jogadores em partida
        with open('csv_extraidos/jogadores_em_partida.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'jogador_id', 'clube_id', 'titular',
                     'numero_camisa', 'minutos_jogados', 'gols', 'assistencias']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for jog in todos_jogadores:
                if jog['titular'] or jog.get('entrou_jogo', False):
                    writer.writerow({
                        'partida_id': partida_id,
                        'jogador_id': jog['jogador_id'],
                        'clube_id': jog.get('clube_id', ''),
                        'titular': 1 if jog['titular'] else 0,
                        'numero_camisa': jog.get('numero_camisa', ''),
                        'minutos_jogados': '',
                        'gols': '',
                        'assistencias': ''
                    })

        print(f"  ✓ jogadores_em_partida.csv")

        # Treinadores
        with open('csv_extraidos/treinadores.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nascimento', 'nacionalidade']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for trein in treinadores:
                writer.writerow({
                    'ID': trein['treinador_id'],
                    'nome': trein['nome'],
                    'nascimento': '',
                    'nacionalidade': trein.get('nacionalidade', '')
                })

        print(f"  ✓ treinadores.csv ({len(treinadores)} registros)")

        # Treinadores em partida
        with open('csv_extraidos/treinadores_em_partida.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'treinador_id', 'clube_id']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for trein in treinadores:
                writer.writerow({
                    'partida_id': partida_id,
                    'treinador_id': trein['treinador_id'],
                    'clube_id': trein.get('clube_id', '')
                })

        print(f"  ✓ treinadores_em_partida.csv")

        # Detalhes da partida
        with open('csv_extraidos/partida_detalhes.csv', 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'estadio', 'cidade', 'data', 'hora',
                     'placar_mandante', 'placar_visitante', 'publico', 'arbitro']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            writer.writerow({
                'partida_id': partida_id,
                'estadio': dados_partida.get('estadio', ''),
                'cidade': dados_partida.get('cidade', ''),
                'data': dados_partida.get('data', ''),
                'hora': dados_partida.get('hora', ''),
                'placar_mandante': dados_partida.get('placar_mandante', ''),
                'placar_visitante': dados_partida.get('placar_visitante', ''),
                'publico': dados_partida.get('publico', ''),
                'arbitro': dados_partida.get('arbitro', '')
            })

        print(f"  ✓ partida_detalhes.csv")

        # Estatísticas da partida (NOVO!)
        if estatisticas and (estatisticas.get('mandante') or estatisticas.get('visitante')):
            with open('csv_extraidos/estatisticas_partida.csv', 'w', newline='', encoding='utf-8') as f:
                campos = ['partida_id', 'clube_id', 'tipo_time', 'posse_de_bola',
                         'chutes', 'escanteios']
                writer = csv.DictWriter(f, fieldnames=campos)
                writer.writeheader()

                # Mandante
                if estatisticas.get('mandante'):
                    clube_id_mand = self.identificar_clube_id(dados_partida.get('mandante'))
                    writer.writerow({
                        'partida_id': partida_id,
                        'clube_id': clube_id_mand,
                        'tipo_time': 'mandante',
                        'posse_de_bola': estatisticas['mandante'].get('posse_de_bola', ''),
                        'chutes': estatisticas['mandante'].get('chutes', ''),
                        'escanteios': estatisticas['mandante'].get('escanteios', '')
                    })

                # Visitante
                if estatisticas.get('visitante'):
                    clube_id_vis = self.identificar_clube_id(dados_partida.get('visitante'))
                    writer.writerow({
                        'partida_id': partida_id,
                        'clube_id': clube_id_vis,
                        'tipo_time': 'visitante',
                        'posse_de_bola': estatisticas['visitante'].get('posse_de_bola', ''),
                        'chutes': estatisticas['visitante'].get('chutes', ''),
                        'escanteios': estatisticas['visitante'].get('escanteios', '')
                    })

            print(f"  ✓ estatisticas_partida.csv")

        # Eventos da partida (NOVO!)
        if eventos:
            with open('csv_extraidos/eventos_partida.csv', 'w', newline='', encoding='utf-8') as f:
                campos = ['partida_id', 'minuto', 'tipo_evento', 'jogador', 'descricao']
                writer = csv.DictWriter(f, fieldnames=campos)
                writer.writeheader()

                for evento in eventos:
                    writer.writerow({
                        'partida_id': partida_id,
                        'minuto': evento['minuto'],
                        'tipo_evento': evento['tipo'],
                        'jogador': evento['jogador'],
                        'descricao': evento.get('descricao', '')
                    })

            print(f"  ✓ eventos_partida.csv ({len(eventos)} eventos)")

    def executar(self, partida_id):
        """Executa scraping completo."""
        print("="*70)
        print(f"🔍 SCRAPER ULTRA ROBUSTO - EXTRAÇÃO COMPLETA")
        print("="*70)
        print(f"URL: {self.url_partida}")
        print(f"Partida ID: {partida_id}")
        print("="*70)

        # Busca página principal
        self.soup_principal = self._fazer_requisicao(self.url_partida)

        if not self.soup_principal:
            print("\n❌ Erro: Não foi possível acessar a página")
            return None

        # Extrai TODOS os dados
        print("\n📊 Extraindo informações da partida...")
        dados_partida = self.extrair_dados_partida_completo(self.soup_principal)

        print(f"\n  Mandante: {dados_partida['mandante']}")
        print(f"  Visitante: {dados_partida['visitante']}")
        print(f"  Placar: {dados_partida['placar_mandante']} x {dados_partida['placar_visitante']}")
        print(f"  Estádio: {dados_partida['estadio']}")
        print(f"  Cidade: {dados_partida['cidade']}")
        print(f"  Data: {dados_partida['data']}")
        print(f"  Hora: {dados_partida['hora']}")
        print(f"  Público: {dados_partida['publico']}")
        print(f"  Árbitro: {dados_partida['arbitro']}")

        # Extrai jogadores
        jogadores = self.extrair_jogadores_completo(self.soup_principal)
        reservas = self.extrair_reservas_completo(self.soup_principal)
        treinadores = self.extrair_treinadores(self.soup_principal)

        # Extrai estatísticas e eventos
        print("\n📈 Extraindo estatísticas...")
        estatisticas = self.extrair_estatisticas_partida(self.soup_principal)

        print("\n⚡ Extraindo eventos...")
        eventos = self.extrair_eventos_partida(self.soup_principal)

        # Exporta tudo
        self.exportar_para_csv(
            dados_partida, jogadores, reservas, treinadores,
            estatisticas, eventos, partida_id
        )

        print("\n" + "="*70)
        print("✅ SCRAPING CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print(f"\n📊 Resumo:")
        print(f"  • Jogadores titulares: {len(jogadores)}")
        print(f"  • Reservas: {len(reservas)}")
        print(f"  • Treinadores: {len(treinadores)}")
        print(f"  • Eventos extraídos: {len(eventos)}")
        print(f"  • Requisições: {len(self.cache_jogadores) + 1}")
        print("="*70)

        return {
            'partida': dados_partida,
            'jogadores': jogadores,
            'reservas': reservas,
            'treinadores': treinadores,
            'estatisticas': estatisticas,
            'eventos': eventos
        }


# ===== EXEMPLO DE USO =====
if __name__ == "__main__":
    url_partida = "https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100"
    caminho_clubes = "C:/Users/enryk/Documents/Estudos/kamusari/novo_bd1971_robusto/csv_bd/clubes.csv"
    partida_id = 1

    scraper = OGolScraperRobusto(url_partida, caminho_clubes)
    resultado = scraper.executar(partida_id)

    if resultado:
        print("\n🎯 Dados prontos para importação no banco de dados!")
        print("\nArquivos gerados na pasta 'csv_extraidos/':")
        print("  • jogadores.csv")
        print("  • jogadores_em_partida.csv")
        print("  • treinadores.csv")
        print("  • treinadores_em_partida.csv")
        print("  • partida_detalhes.csv")
        print("  • estatisticas_partida.csv")
        print("  • eventos_partida.csv")
