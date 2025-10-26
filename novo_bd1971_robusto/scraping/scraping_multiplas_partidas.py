import csv
import time
from datetime import datetime
import json
import os
from ogol_scraper_avancado import OGolScraperAvancado

class ProcessadorPartidas:
    """
    Processa múltiplas partidas do OGol e gera CSVs consolidados
    para importação no banco de dados.
    """

    def __init__(self, arquivo_clubes, arquivo_partidas=None):
        """
        Inicializa o processador.

        Args:
            arquivo_clubes: Caminho para CSV com dados dos clubes
            arquivo_partidas: Caminho para CSV com lista de partidas a processar
        """
        self.arquivo_clubes = arquivo_clubes
        self.arquivo_partidas = arquivo_partidas

        # Armazena todos os dados extraídos
        self.todos_jogadores = {}  # {nome_jogador: dados}
        self.todos_treinadores = {}  # {nome_treinador: dados}
        self.todas_participacoes = []
        self.todos_tecnicos_partidas = []
        self.detalhes_partidas = []
        self.estadios = {}  # {nome_estadio: dados}

        # Contadores globais de IDs
        self.proximo_jogador_id = 1
        self.proximo_treinador_id = 1
        self.proximo_estadio_id = 1

        # Estatísticas
        self.stats = {
            'total_partidas': 0,
            'partidas_sucesso': 0,
            'partidas_erro': 0,
            'total_requisicoes': 0,
            'tempo_total': 0
        }

    def carregar_lista_partidas(self):
        """
        Carrega lista de partidas a processar de um CSV.

        Formato esperado do CSV:
        partida_id,url_ogol,mandante,visitante,data
        1,https://www.ogol.com.br/jogo/...,Bahia,Santos,1971-08-07
        """
        partidas = []

        if not self.arquivo_partidas:
            return partidas

        with open(self.arquivo_partidas, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                partidas.append({
                    'partida_id': int(row['partida_id']),
                    'url_ogol': row['url_ogol'],
                    'mandante': row.get('mandante', ''),
                    'visitante': row.get('visitante', ''),
                    'data': row.get('data', '')
                })

        return partidas

    def criar_partida_manual(self, partida_id, url_ogol):
        """
        Cria entrada manual de partida para processar.
        """
        return {
            'partida_id': partida_id,
            'url_ogol': url_ogol,
            'mandante': '',
            'visitante': '',
            'data': ''
        }

    def processar_jogador(self, dados_jogador):
        """
        Adiciona ou atualiza jogador no registro global.
        Evita duplicatas baseado no nome.
        """
        nome = dados_jogador['nome']

        if nome not in self.todos_jogadores:
            # Novo jogador - adiciona com ID único
            self.todos_jogadores[nome] = {
                'id': self.proximo_jogador_id,
                'nome': nome,
                'nascimento': dados_jogador.get('nascimento'),
                'nacionalidade': dados_jogador.get('nacionalidade'),
                'altura': dados_jogador.get('altura'),
                'posicao': dados_jogador.get('posicao'),
                'pe_preferido': dados_jogador.get('pe_preferido'),
                'clube_atual_id': dados_jogador.get('clube_id')
            }
            self.proximo_jogador_id += 1
        else:
            # Jogador existente - atualiza campos vazios se novos dados disponíveis
            jogador_existente = self.todos_jogadores[nome]

            for campo in ['nascimento', 'nacionalidade', 'altura', 'posicao', 'pe_preferido']:
                if not jogador_existente.get(campo) and dados_jogador.get(campo):
                    jogador_existente[campo] = dados_jogador[campo]

        return self.todos_jogadores[nome]['id']

    def processar_treinador(self, dados_treinador):
        """
        Adiciona ou atualiza treinador no registro global.
        """
        nome = dados_treinador['nome']

        if nome not in self.todos_treinadores:
            self.todos_treinadores[nome] = {
                'id': self.proximo_treinador_id,
                'nome': nome,
                'nacionalidade': dados_treinador.get('nacionalidade')
            }
            self.proximo_treinador_id += 1

        return self.todos_treinadores[nome]['id']

    def processar_estadio(self, nome_estadio, cidade=None):
        """
        Adiciona estádio se ainda não existir.
        """
        if not nome_estadio:
            return None

        if nome_estadio not in self.estadios:
            self.estadios[nome_estadio] = {
                'id': self.proximo_estadio_id,
                'nome': nome_estadio,
                'cidade': cidade,
                'capacidade': None  # Não disponível no scraper atual
            }
            self.proximo_estadio_id += 1

        return self.estadios[nome_estadio]['id']

    def processar_partida(self, partida_info):
        """
        Processa uma partida individual.
        """
        partida_id = partida_info['partida_id']
        url = partida_info['url_ogol']

        print(f"\n{'='*70}")
        print(f"Processando Partida ID: {partida_id}")
        print(f"URL: {url}")
        print(f"{'='*70}")

        try:
            # Cria scraper para esta partida
            scraper = OGolScraperAvancado(url, self.arquivo_clubes)

            # Ajusta IDs iniciais para continuar sequência global
            scraper.proximo_jogador_id = self.proximo_jogador_id
            scraper.proximo_treinador_id = self.proximo_treinador_id

            # Faz o scraping
            inicio = time.time()
            resultado = scraper.executar(partida_id)
            tempo_decorrido = time.time() - inicio

            if not resultado:
                self.stats['partidas_erro'] += 1
                print(f"❌ Erro ao processar partida {partida_id}")
                return False

            # Processa dados extraídos
            dados_partida = resultado['partida']

            # Processa jogadores e registra participações
            for jogador_dados in resultado['jogadores'] + resultado['reservas']:
                jogador_id_global = self.processar_jogador(jogador_dados)

                # Registra participação na partida
                if jogador_dados['titular'] or jogador_dados.get('entrou_jogo', False):
                    self.todas_participacoes.append({
                        'partida_id': partida_id,
                        'jogador_id': jogador_id_global,
                        'clube_id': jogador_dados.get('clube_id'),
                        'titular': jogador_dados['titular'],
                        'minutos_jogados': None,
                        'gols': None,
                        'assistencias': None
                    })

            # Processa treinadores
            for treinador_dados in resultado['treinadores']:
                treinador_id_global = self.processar_treinador(treinador_dados)

                self.todos_tecnicos_partidas.append({
                    'partida_id': partida_id,
                    'treinador_id': treinador_id_global,
                    'clube_id': treinador_dados.get('clube_id')
                })

            # Processa estádio
            estadio_id = self.processar_estadio(
                dados_partida.get('estadio'),
                dados_partida.get('cidade')
            )

            # Registra detalhes da partida
            self.detalhes_partidas.append({
                'partida_id': partida_id,
                'estadio_id': estadio_id,
                'estadio_nome': dados_partida.get('estadio'),
                'cidade': dados_partida.get('cidade'),
                'data': dados_partida.get('data'),
                'placar_mandante': dados_partida.get('placar_mandante'),
                'placar_visitante': dados_partida.get('placar_visitante')
            })

            # Atualiza estatísticas
            self.stats['partidas_sucesso'] += 1
            self.stats['total_requisicoes'] += len(scraper.cache_jogadores) + 1
            self.stats['tempo_total'] += tempo_decorrido

            # Atualiza contadores globais
            self.proximo_jogador_id = scraper.proximo_jogador_id
            self.proximo_treinador_id = scraper.proximo_treinador_id

            print(f"✅ Partida {partida_id} processada com sucesso em {tempo_decorrido:.1f}s")
            return True

        except Exception as e:
            print(f"❌ Erro ao processar partida {partida_id}: {e}")
            self.stats['partidas_erro'] += 1
            return False

    def exportar_consolidado(self, prefixo='consolidado'):
        """
        Exporta todos os dados consolidados para CSVs.
        """
        print("\n" + "="*70)
        print("📊 EXPORTANDO DADOS CONSOLIDADOS")
        print("="*70)

        # Cria diretório de saída se não existir
        os.makedirs('output', exist_ok=True)

        # Exporta jogadores únicos
        arquivo_jogadores = f'output/{prefixo}_jogadores.csv'
        with open(arquivo_jogadores, 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nascimento', 'nacionalidade', 'altura',
                     'posicao', 'pe_preferido', 'clube_atual_id']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for jogador in self.todos_jogadores.values():
                writer.writerow({
                    'ID': jogador['id'],
                    'nome': jogador['nome'],
                    'nascimento': jogador.get('nascimento', ''),
                    'nacionalidade': jogador.get('nacionalidade', ''),
                    'altura': jogador.get('altura', ''),
                    'posicao': jogador.get('posicao', ''),
                    'pe_preferido': jogador.get('pe_preferido', ''),
                    'clube_atual_id': jogador.get('clube_atual_id', '')
                })

        print(f"✓ {arquivo_jogadores} - {len(self.todos_jogadores)} jogadores únicos")

        # Exporta participações em partidas
        arquivo_participacoes = f'output/{prefixo}_jogadores_em_partida.csv'
        with open(arquivo_participacoes, 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'jogador_id', 'clube_id', 'titular',
                     'minutos_jogados', 'gols', 'assistencias']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for participacao in self.todas_participacoes:
                writer.writerow(participacao)

        print(f"✓ {arquivo_participacoes} - {len(self.todas_participacoes)} participações")

        # Exporta treinadores únicos
        arquivo_treinadores = f'output/{prefixo}_treinadores.csv'
        with open(arquivo_treinadores, 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'nascimento', 'nacionalidade']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for treinador in self.todos_treinadores.values():
                writer.writerow({
                    'ID': treinador['id'],
                    'nome': treinador['nome'],
                    'nascimento': '',
                    'nacionalidade': treinador.get('nacionalidade', '')
                })

        print(f"✓ {arquivo_treinadores} - {len(self.todos_treinadores)} treinadores únicos")

        # Exporta técnicos em partidas
        arquivo_tecnicos = f'output/{prefixo}_treinadores_em_partida.csv'
        with open(arquivo_tecnicos, 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'treinador_id', 'clube_id']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for registro in self.todos_tecnicos_partidas:
                writer.writerow(registro)

        print(f"✓ {arquivo_tecnicos} - {len(self.todos_tecnicos_partidas)} registros")

        # Exporta estádios
        arquivo_estadios = f'output/{prefixo}_estadios.csv'
        with open(arquivo_estadios, 'w', newline='', encoding='utf-8') as f:
            campos = ['ID', 'nome', 'cidade', 'capacidade']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for estadio in self.estadios.values():
                writer.writerow({
                    'ID': estadio['id'],
                    'nome': estadio['nome'],
                    'cidade': estadio.get('cidade', ''),
                    'capacidade': estadio.get('capacidade', '')
                })

        print(f"✓ {arquivo_estadios} - {len(self.estadios)} estádios únicos")

        # Exporta detalhes das partidas
        arquivo_detalhes = f'output/{prefixo}_partidas_detalhes.csv'
        with open(arquivo_detalhes, 'w', newline='', encoding='utf-8') as f:
            campos = ['partida_id', 'estadio_id', 'estadio_nome', 'cidade',
                     'data', 'placar_mandante', 'placar_visitante']
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()

            for detalhe in self.detalhes_partidas:
                writer.writerow(detalhe)

        print(f"✓ {arquivo_detalhes} - {len(self.detalhes_partidas)} partidas")

        # Exporta relatório de estatísticas
        arquivo_stats = f'output/{prefixo}_relatorio.txt'
        with open(arquivo_stats, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RELATÓRIO DE PROCESSAMENTO\n")
            f.write("="*70 + "\n\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
            f.write(f"Total de partidas processadas: {self.stats['total_partidas']}\n")
            f.write(f"  • Sucesso: {self.stats['partidas_sucesso']}\n")
            f.write(f"  • Erro: {self.stats['partidas_erro']}\n\n")
            f.write(f"Dados extraídos:\n")
            f.write(f"  • Jogadores únicos: {len(self.todos_jogadores)}\n")
            f.write(f"  • Participações em partidas: {len(self.todas_participacoes)}\n")
            f.write(f"  • Treinadores únicos: {len(self.todos_treinadores)}\n")
            f.write(f"  • Estádios únicos: {len(self.estadios)}\n\n")
            f.write(f"Performance:\n")
            f.write(f"  • Total de requisições HTTP: {self.stats['total_requisicoes']}\n")
            f.write(f"  • Tempo total: {self.stats['tempo_total']:.1f}s\n")
            if self.stats['partidas_sucesso'] > 0:
                tempo_medio = self.stats['tempo_total'] / self.stats['partidas_sucesso']
                f.write(f"  • Tempo médio por partida: {tempo_medio:.1f}s\n")

        print(f"✓ {arquivo_stats} - Relatório detalhado")

        print("\n" + "="*70)
        print("✅ EXPORTAÇÃO CONCLUÍDA!")
        print("="*70)

    def executar_lote(self, lista_partidas=None):
        """
        Executa processamento de um lote de partidas.

        Args:
            lista_partidas: Lista de dicts com partida_id e url_ogol
                           Se None, carrega do arquivo configurado
        """
        if lista_partidas is None:
            lista_partidas = self.carregar_lista_partidas()

        if not lista_partidas:
            print("⚠ Nenhuma partida para processar")
            return

        self.stats['total_partidas'] = len(lista_partidas)

        print(f"\n🚀 Iniciando processamento de {len(lista_partidas)} partidas")
        print(f"Arquivo de clubes: {self.arquivo_clubes}")
        print(f"Diretório de saída: output/\n")

        inicio_total = time.time()

        for i, partida in enumerate(lista_partidas, 1):
            print(f"\n[{i}/{len(lista_partidas)}] ", end='')
            self.processar_partida(partida)

            # Pausa entre partidas para não sobrecarregar o servidor
            if i < len(lista_partidas):
                time.sleep(3)

        tempo_total = time.time() - inicio_total
        self.stats['tempo_total'] = tempo_total

        # Exporta dados consolidados
        self.exportar_consolidado()

        # Exibe resumo final
        print(f"\n{'='*70}")
        print("📈 RESUMO FINAL")
        print(f"{'='*70}")
        print(f"Partidas processadas: {self.stats['partidas_sucesso']}/{self.stats['total_partidas']}")
        print(f"Jogadores únicos encontrados: {len(self.todos_jogadores)}")
        print(f"Treinadores únicos encontrados: {len(self.todos_treinadores)}")
        print(f"Estádios únicos encontrados: {len(self.estadios)}")
        print(f"Tempo total: {tempo_total/60:.1f} minutos")
        print(f"{'='*70}\n")


# ===== EXEMPLOS DE USO =====

def exemplo_partida_unica():
    """Exemplo: processar uma única partida"""
    processador = ProcessadorPartidas('csv_br_1971/clubes.csv')

    partida = processador.criar_partida_manual(
        partida_id=1,
        url_ogol='https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100'
    )

    processador.executar_lote([partida])


def exemplo_multiplas_partidas():
    """Exemplo: processar várias partidas de uma vez"""
    processador = ProcessadorPartidas('csv_br_1971/clubes.csv')

    # Lista de partidas a processar
    partidas = [
        processador.criar_partida_manual(1, 'https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100'),
        processador.criar_partida_manual(2, 'https://www.ogol.com.br/jogo/...'),  # adicione mais URLs
        processador.criar_partida_manual(3, 'https://www.ogol.com.br/jogo/...'),
    ]

    processador.executar_lote(partidas)


def exemplo_com_arquivo_csv():
    """
    Exemplo: processar partidas listadas em um CSV

    Crie um arquivo 'partidas_para_processar.csv' com o formato:
    partida_id,url_ogol,mandante,visitante,data
    1,https://www.ogol.com.br/jogo/1971-08-07-bahia-santos/500100,Bahia,Santos,1971-08-07
    2,https://www.ogol.com.br/jogo/...,Palmeiras,Corinthians,1971-08-14
    """
    processador = ProcessadorPartidas(
        arquivo_clubes='csv_bd/clubes.csv',
        arquivo_partidas='csv_bd/partidas_para_processar.csv'
    )

    processador.executar_lote()


if __name__ == "__main__":
    # Descomente o exemplo que você quer usar

    # exemplo_partida_unica()
    # exemplo_multiplas_partidas()
    # exemplo_com_arquivo_csv()

    print("Execute um dos exemplos descomentando a função correspondente")
