import sqlite3
import csv
import os
from datetime import datetime

class MigradorCSVParaSQLite:
    def __init__(self, db_path, csv_dir):
        """
        Inicializa o migrador com o caminho do banco SQLite e diretório dos CSVs

        Args:
            db_path: Caminho para o arquivo .db do SQLite
            csv_dir: Diretório onde estão os arquivos CSV
        """
        self.db_path = db_path
        self.csv_dir = csv_dir
        self.conn = None
        self.cursor = None

    def conectar(self):
        """Estabelece conexão com o banco SQLite"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"✅ Conectado ao banco: {self.db_path}")

    def desconectar(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✅ Conexão encerrada")

    def limpar_valor(self, valor):
        """
        Limpa valores vazios ou inválidos dos CSVs
        Converte strings vazias, '-' ou 'None' em NULL do SQLite
        """
        if valor in ['', '-', 'None', 'NULL']:
            return None
        return valor.strip() if isinstance(valor, str) else valor

    def migrar_locais(self):
        """Migra dados da tabela locais"""
        csv_path = os.path.join(self.csv_dir, 'locais - locais.csv')
        if not os.path.exists(csv_path):
            print("⚠️  locais.csv não encontrado")
            return

        print("\n📍 Migrando LOCAIS...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO locais (ID, cidade, estado, UF, regiao, pais)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['cidade']),
                        self.limpar_valor(row['estado']),
                        self.limpar_valor(row['uf']),
                        self.limpar_valor(row['regiao']),
                        self.limpar_valor(row['pais'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir local {row.get('id')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} locais migrados")

    def migrar_clubes(self):
        """Migra dados da tabela clubes"""
        csv_path = os.path.join(self.csv_dir, 'clubes - clubes.csv')
        if not os.path.exists(csv_path):
            print("⚠️  clubes.csv não encontrado")
            return

        print("\n⚽ Migrando CLUBES...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO clubes (ID, clube, apelido, local_id, fundacao, ativo)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['clube']),
                        self.limpar_valor(row['apelido']),
                        self.limpar_valor(row['local_id']),
                        self.limpar_valor(row['fundacao']),
                        self.limpar_valor(row.get('ativo', 1))
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir clube {row.get('clube')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} clubes migrados")

    def migrar_estadios(self):
        """Migra dados da tabela estadios"""
        csv_path = os.path.join(self.csv_dir, 'estadios - estadios.csv')
        if not os.path.exists(csv_path):
            print("⚠️  estadios.csv não encontrado")
            return

        print("\n🏟️  Migrando ESTÁDIOS...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO estadios (ID, estadio, capacidade, local_id, inauguracao, ativo)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['estadio']),
                        self.limpar_valor(row['capacidade']),
                        self.limpar_valor(row['local_id']),
                        self.limpar_valor(row['inauguracao']),
                        self.limpar_valor(row.get('ativo', 1))
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir estádio {row.get('estadio')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} estádios migrados")

    def migrar_jogadores(self):
        """Migra dados da tabela jogadores"""
        csv_path = os.path.join(self.csv_dir, 'jogadores - jogadores.csv')
        if not os.path.exists(csv_path):
            print("⚠️  jogadores.csv não encontrado")
            return

        print("\n👟 Migrando JOGADORES...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO jogadores
                        (ID, nome, nascimento, falecimento, nacionalidade, naturalidade,
                         altura, peso, posicao, posicao_detalhada, pe_preferido, aposentado)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['nome']),
                        self.limpar_valor(row['nascimento']),
                        self.limpar_valor(row['falecimento']),
                        self.limpar_valor(row['nacionalidade']),
                        self.limpar_valor(row['naturalidade']),
                        self.limpar_valor(row['altura']),
                        self.limpar_valor(row['peso']),
                        self.limpar_valor(row['posicao']),
                        self.limpar_valor(row['posicao_detalhada']),
                        self.limpar_valor(row['pe_preferido']),
                        self.limpar_valor(row.get('aposentado', 0))
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir jogador {row.get('nome')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} jogadores migrados")

    def migrar_treinadores(self):
        """Migra dados da tabela treinadores"""
        csv_path = os.path.join(self.csv_dir, 'treinadores - treinadores.csv')
        if not os.path.exists(csv_path):
            print("⚠️  treinadores.csv não encontrado")
            return

        print("\n👔 Migrando TREINADORES...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO treinadores
                        (ID, nome, nascimento, falecimento, nacionalidade, naturalidade, aposentado)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['nome']),
                        self.limpar_valor(row['nascimento']),
                        self.limpar_valor(row['falecimento']),
                        self.limpar_valor(row['nacionalidade']),
                        self.limpar_valor(row['naturalidade']),
                        self.limpar_valor(row['aposentado'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir treinador {row.get('nome')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} treinadores migrados")

    def migrar_arbitros(self):
        """Migra dados da tabela arbitros"""
        csv_path = os.path.join(self.csv_dir, 'arbitros - arbitros.csv')
        if not os.path.exists(csv_path):
            print("⚠️  arbitros.csv não encontrado")
            return

        print("\n🧑‍⚖️  Migrando ÁRBITROS...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO arbitros
                        (ID, nome, nascimento, falecimento, nacionalidade, naturalidade, aposentado)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['nome']),
                        self.limpar_valor(row['nascimento']),
                        self.limpar_valor(row['falecimento']),
                        self.limpar_valor(row['nacionalidade']),
                        self.limpar_valor(row['naturalidade']),
                        self.limpar_valor(row['aposentado'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir árbitro {row.get('nome')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} árbitros migrados")

    def migrar_campeonatos(self):
        """Migra dados da tabela campeonatos"""
        csv_path = os.path.join(self.csv_dir, 'campeonatos - campeonatos.csv')
        if not os.path.exists(csv_path):
            print("⚠️  campeonatos.csv não encontrado")
            return

        print("\n🏆 Migrando CAMPEONATOS...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO campeonatos (ID, campeonato, pais, entidade, tipo, criado_em)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['ID']),
                        self.limpar_valor(row['campeonato']),
                        self.limpar_valor(row['pais']),
                        self.limpar_valor(row['entidade']),
                        self.limpar_valor(row['tipo']),
                        self.limpar_valor(row['criado_em'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir campeonato: {e}")

            self.conn.commit()
            print(f"✅ {contador} campeonatos migrados")

    def migrar_edicoes(self):
        """Migra dados da tabela edicoes"""
        csv_path = os.path.join(self.csv_dir, 'edicoes - edicoes.csv')
        if not os.path.exists(csv_path):
            print("⚠️  edicoes.csv não encontrado")
            return

        print("\n📅 Migrando EDIÇÕES...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO edicoes
                        (ID, campeonato_id, ano, data_inicio, data_fim, campeao_id, vice_id, criado_em)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['ID']),
                        self.limpar_valor(row['campeonato_id']),
                        self.limpar_valor(row['ano']),
                        self.limpar_valor(row['data_inicio']),
                        self.limpar_valor(row['data_fim']),
                        self.limpar_valor(row['campeao_id']),
                        self.limpar_valor(row['vice_id']),
                        self.limpar_valor(row['criado_em'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir edição: {e}")

            self.conn.commit()
            print(f"✅ {contador} edições migradas")

    def migrar_partidas(self):
        """Migra dados da tabela partidas"""
        csv_path = os.path.join(self.csv_dir, 'partidas - partidas.csv')
        if not os.path.exists(csv_path):
            print("⚠️  partidas.csv não encontrado")
            return

        print("\n⚽ Migrando PARTIDAS...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO partidas
                        (ID, edicao_id, campeonato_id, data, hora, fase, grupo, rodada,
                         estadio_id, mandante_id, visitante_id, mandante_placar,
                         visitante_placar, mandante_grupo, visitante_grupo, mandante_penalti, visitante_penalti, prorrogacao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['edicao_id']),
                        self.limpar_valor(row['campeonato_id']),
                        self.limpar_valor(row['data']),
                        self.limpar_valor(row['hora']),
                        self.limpar_valor(row['fase']),
                        self.limpar_valor(row['grupo']),
                        self.limpar_valor(row['rodada']),
                        self.limpar_valor(row['estadio_id']),
                        self.limpar_valor(row['mandante_id']),
                        self.limpar_valor(row['visitante_id']),
                        self.limpar_valor(row['mandante_placar']),
                        self.limpar_valor(row['visitante_placar']),
                        self.limpar_valor(row['mandante_grupo']),
                        self.limpar_valor(row['visitante_grupo']),
                        self.limpar_valor(row['mandante_penalti']),
                        self.limpar_valor(row['visitante_penalti']),
                        self.limpar_valor(row['prorrogacao'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir partida {row.get('id')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} partidas migradas")

    def migrar_jogadores_em_partida(self):
        """Migra dados da tabela jogadores_em_partida"""
        csv_path = os.path.join(self.csv_dir, 'jogadores_em_partida - jogadores_em_partida.csv')
        if not os.path.exists(csv_path):
            print("⚠️  jogadores_em_partida.csv não encontrado")
            return

        print("\n👥 Migrando JOGADORES EM PARTIDA...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO jogadores_em_partida
                        (partida_id, jogador_id, clube_id, titular, posicao_jogada, numero_camisa)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['partida_id']),
                        self.limpar_valor(row['jogador_id']),
                        self.limpar_valor(row['clube_id']),
                        self.limpar_valor(row['titular']),
                        self.limpar_valor(row['posicao_jogada']),
                        self.limpar_valor(row['numero_camisa'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir jogador em partida: {e}")

            self.conn.commit()
            print(f"✅ {contador} registros de jogadores em partidas migrados")

    def migrar_treinadores_em_partida(self):
        """Migra dados da tabela treinadores_em_partida"""
        csv_path = os.path.join(self.csv_dir, 'treinadores_em_partida - treinadores_em_partida.csv')
        if not os.path.exists(csv_path):
            print("⚠️  treinadores_em_partida.csv não encontrado")
            return

        print("\n👔 Migrando TREINADORES EM PARTIDA...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO treinadores_em_partida
                        (partida_id, treinador_id, clube_id, tipo)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['partida_id']),
                        self.limpar_valor(row['treinador_id']),
                        self.limpar_valor(row['clube_id']),
                        self.limpar_valor(row['tipo'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir treinador em partida: {e}")

            self.conn.commit()
            print(f"✅ {contador} registros de treinadores em partidas migrados")

    def migrar_arbitros_em_partida(self):
        """Migra dados da tabela arbitros_em_partida"""
        csv_path = os.path.join(self.csv_dir, 'arbitros_em_partida - arbitros_em_partida.csv')
        if not os.path.exists(csv_path):
            print("⚠️  arbitros_em_partida.csv não encontrado")
            return

        print("\n🧑‍⚖️  Migrando ÁRBITROS EM PARTIDA...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO arbitros_em_partida (partida_id, arbitro_id)
                        VALUES (?, ?)
                    ''', (
                        self.limpar_valor(row['partida_id']),
                        self.limpar_valor(row['arbitro_id'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir árbitro em partida: {e}")

            self.conn.commit()
            print(f"✅ {contador} registros de árbitros em partidas migrados")

    def migrar_eventos_partida(self):
        """Migra dados da tabela eventos_partida"""
        csv_path = os.path.join(self.csv_dir, 'eventos_partida - eventos_partida.csv')
        if not os.path.exists(csv_path):
            print("⚠️  eventos_partida.csv não encontrado")
            return

        print("\n📝 Migrando EVENTOS DE PARTIDA...")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contador = 0

            for row in reader:
                try:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO eventos_partida
                        (ID, partida_id, jogador_id, clube_id, tipo_evento, tipo_gol, minuto)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.limpar_valor(row['id']),
                        self.limpar_valor(row['partida_id']),
                        self.limpar_valor(row['jogador_id']),
                        self.limpar_valor(row['clube_id']),
                        self.limpar_valor(row['tipo_evento']),
                        self.limpar_valor(row['tipo_gol']),
                        self.limpar_valor(row['minuto'])
                    ))
                    contador += 1
                except Exception as e:
                    print(f"❌ Erro ao inserir evento {row.get('id')}: {e}")

            self.conn.commit()
            print(f"✅ {contador} eventos migrados")

    def executar_migracao_completa(self):
        """
        Executa a migração completa de todos os CSVs para o SQLite
        na ordem correta para respeitar as chaves estrangeiras
        """
        print("\n" + "="*60)
        print("🚀 INICIANDO MIGRAÇÃO CSV → SQLite")
        print("="*60)

        try:
            self.conectar()

            # Migra na ordem correta (respeitando dependências)
            self.migrar_locais()
            self.migrar_clubes()
            self.migrar_estadios()
            self.migrar_jogadores()
            self.migrar_treinadores()
            self.migrar_arbitros()
            self.migrar_campeonatos()
            self.migrar_edicoes()
            self.migrar_partidas()
            self.migrar_jogadores_em_partida()
            self.migrar_treinadores_em_partida()
            self.migrar_arbitros_em_partida()
            self.migrar_eventos_partida()

            print("\n" + "="*60)
            print("✅ MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
            print("="*60)

        except Exception as e:
            print(f"\n❌ ERRO NA MIGRAÇÃO: {e}")
            self.conn.rollback()
        finally:
            self.desconectar()


# ============================================
# EXEMPLO DE USO
# ============================================

if __name__ == "__main__":
    # Configure os caminhos dos seus arquivos
    CAMINHO_BANCO_SQLITE = "bd/estruturado_bd_1971.db"  # Seu arquivo .db
    DIRETORIO_CSVS = "csv_atualizados"  # Pasta com os CSVs

    # Cria o migrador e executa
    migrador = MigradorCSVParaSQLite(CAMINHO_BANCO_SQLITE, DIRETORIO_CSVS)
    migrador.executar_migracao_completa()
