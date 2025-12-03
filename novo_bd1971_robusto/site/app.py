# app.py - Versão Completa
from flask import Flask, render_template, jsonify, request, redirect, g, url_for
from datetime import datetime
import sqlite3
import json

app = Flask(__name__)

# ==================== CONFIGURAÇÕES ====================
DATABASE = "../bd/estruturado_bd_1971.db"

# Função para descobrir automaticamente o ano mais recente
def obter_ano_mais_recente():
    """
    Descobre qual é o ano mais recente com dados no banco.
    Isso evita erros quando tentamos acessar anos sem dados.
    """
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        resultado = cursor.execute("SELECT MAX(CAST(ano AS INTEGER)) FROM edicoes").fetchone()
        conn.close()
        return resultado[0] if resultado[0] else 1971
    except Exception as e:
        print(f"Erro ao obter ano mais recente: {e}")
        return 1971

ANO_ATUAL = obter_ano_mais_recente()
ANOS_DISPONIVEIS = list(range(1971, ANO_ATUAL + 1))

# ==================== DATABASE MANAGEMENT ====================

def get_db():
    """
    Obtém conexão com banco de dados.
    Usamos g (contexto global do Flask) para manter uma conexão por requisição.
    """
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # Permite acessar colunas por nome
    return g.db

@app.teardown_appcontext
def close_db(error):
    """
    Fecha a conexão ao fim da requisição.
    Isso é importante para não deixar conexões abertas.
    """
    db = g.pop('db', None)
    if db is not None:
        db.close()

def dict_from_row(row):
    """
    Converte sqlite3.Row para dict.
    Isso facilita passar dados para os templates.
    """
    return dict(zip(row.keys(), row)) if row else None

# ==================== FUNÇÕES AUXILIARES ====================

def calcular_pontos_vitoria(ano):
    """
    IMPORTANTE: Até 1994, vitória valia 2 pontos.
    A partir de 1995, passou a valer 3 pontos.

    Esta função é usada em TODOS os cálculos de classificação.
    """
    return 2 if ano <= 1994 else 3

def slugify(text):
    """
    Transforma texto em URL amigável.
    Exemplo: 'São Paulo' -> 'sao-paulo'
    """
    import unicodedata
    import re

    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')

def get_formato_campeonato(ano):
    """
    Determina o formato do campeonato baseado no ano.
    Isso ajuda a saber se devemos mostrar grupos ou não.

    Formatos históricos do Brasileirão:
    - Até 1991: Grupos + Fases eliminatórias
    - 1992-2002: Misto (grupos + mata-mata)
    - 2003 em diante: Pontos corridos puro
    """
    if ano >= 2003:
        return 'pontos_corridos'
    elif ano >= 1992:
        return 'misto'
    else:
        return 'grupos_fases'

# ==================== BEFORE REQUEST ====================

@app.before_request
def setup_globals():
    """
    Configura variáveis globais disponíveis em todos os templates.
    Isso evita ter que passar essas variáveis em cada render_template.
    """
    g.ANO_ATUAL = ANO_ATUAL
    g.ANOS_DISPONIVEIS = ANOS_DISPONIVEIS

# ==================== ROTAS PRINCIPAIS ====================

@app.route("/")
def index():
    """Homepage com resumo da temporada atual e dados históricos"""
    db = get_db()

    # Classificação do ano atual (top 10)
    pontos_vitoria = calcular_pontos_vitoria(ANO_ATUAL)
    classificacao_atual = db.execute(f"""
        WITH jogos_mandante AS (
            SELECT
                c.clube,
                p.mandante_placar AS gols_pro,
                p.visitante_placar AS gols_sofrido,
                CASE
                    WHEN p.mandante_placar > p.visitante_placar THEN {pontos_vitoria}
                    WHEN p.mandante_placar = p.visitante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END AS derrotas
            FROM partidas p
            JOIN clubes c ON p.mandante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ?
        ),
        jogos_visitante AS (
            SELECT
                c.clube,
                p.visitante_placar AS gols_pro,
                p.mandante_placar AS gols_sofrido,
                CASE
                    WHEN p.visitante_placar > p.mandante_placar THEN {pontos_vitoria}
                    WHEN p.visitante_placar = p.mandante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.visitante_placar = p.mandante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END AS derrotas
            FROM partidas p
            JOIN clubes c ON p.visitante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ?
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pontos) DESC, SUM(vitorias) DESC,
                (SUM(gols_pro) - SUM(gols_sofrido)) DESC, SUM(gols_pro) DESC
            ) AS posicao,
            clube,
            COUNT(*) AS jogos,
            SUM(pontos) AS pts,
            SUM(vitorias) AS v,
            SUM(empates) AS e,
            SUM(derrotas) AS d,
            SUM(gols_pro) AS gp,
            SUM(gols_sofrido) AS gc,
            (SUM(gols_pro) - SUM(gols_sofrido)) AS sg
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pts DESC, v DESC, sg DESC, gp DESC
        LIMIT 10
    """, (ANO_ATUAL, ANO_ATUAL)).fetchall()

    # Últimos 10 jogos
    ultimos_jogos = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cv.clube AS visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.data,
            p.fase,
            ed.ano,
            est.estadio
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        LEFT JOIN estadios est ON p.estadio_id = est.ID
        WHERE ed.ano = ?
            AND p.mandante_placar IS NOT NULL
            AND p.visitante_placar IS NOT NULL
        ORDER BY p.data DESC
        LIMIT 10
    """, (ANO_ATUAL,)).fetchall()

    # Top 5 histórico
    top_historico = db.execute("""
        WITH jogos_mandante AS (
            SELECT
                c.clube,
                ed.ano,
                p.mandante_placar AS gols_pro,
                p.visitante_placar AS gols_sofrido,
                CASE
                    WHEN p.mandante_placar > p.visitante_placar THEN
                        CASE WHEN ed.ano <= 1994 THEN 2 ELSE 3 END
                    WHEN p.mandante_placar = p.visitante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS vitorias
            FROM partidas p
            JOIN clubes c ON p.mandante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
        ),
        jogos_visitante AS (
            SELECT
                c.clube,
                ed.ano,
                p.visitante_placar AS gols_pro,
                p.mandante_placar AS gols_sofrido,
                CASE
                    WHEN p.visitante_placar > p.mandante_placar THEN
                        CASE WHEN ed.ano <= 1994 THEN 2 ELSE 3 END
                    WHEN p.visitante_placar = p.mandante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS vitorias
            FROM partidas p
            JOIN clubes c ON p.visitante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            clube,
            SUM(pontos) AS pontos_total,
            COUNT(*) AS jogos_total,
            SUM(vitorias) AS vitorias_total
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pontos_total DESC, vitorias_total DESC
        LIMIT 5
    """).fetchall()

    # Estatísticas gerais
    stats_gerais = db.execute("""
        SELECT
            COUNT(DISTINCT c.ID) as total_clubes,
            COUNT(DISTINCT p.ID) as total_partidas,
            COUNT(DISTINCT j.ID) as total_jogadores,
            COUNT(DISTINCT ed.ano) as total_edicoes
        FROM clubes c
        LEFT JOIN partidas p ON c.ID = p.mandante_id OR c.ID = p.visitante_id
        LEFT JOIN jogadores_em_partida jp ON p.ID = jp.partida_id
        LEFT JOIN jogadores j ON jp.jogador_id = j.ID
        LEFT JOIN edicoes ed ON p.edicao_id = ed.ID
    """).fetchone()

    return render_template('index.html',
                         classificacao_atual=[dict_from_row(r) for r in classificacao_atual],
                         ultimos_jogos=[dict_from_row(r) for r in ultimos_jogos],
                         top_historico=[dict_from_row(r) for r in top_historico],
                         stats_gerais=dict_from_row(stats_gerais))

@app.route("/explorar")
def explorar():
    """Página de exploração por temporadas, clubes e campeões"""
    db = get_db()

    clubes = db.execute("""
        SELECT DISTINCT c.clube, c.ID, l.UF, l.cidade
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        JOIN partidas p ON c.ID = p.mandante_id OR c.ID = p.visitante_id
        ORDER BY c.clube
    """).fetchall()

    campeoes = db.execute("""
        SELECT ed.ano, c.clube as campeao, cv.clube as vice
        FROM edicoes ed
        LEFT JOIN clubes c ON ed.campeao_id = c.ID
        LEFT JOIN clubes cv ON ed.vice_id = cv.ID
        WHERE ed.campeao_id IS NOT NULL
        ORDER BY ed.ano DESC
    """).fetchall()

    return render_template('explorar.html',
                         clubes=[dict_from_row(r) for r in clubes],
                         campeoes=[dict_from_row(r) for r in campeoes])

@app.route("/buscar")
def buscar():
    """Sistema de busca unificado"""
    query = request.args.get('q', '').strip()

    if not query:
        return render_template('buscar.html', query=None, resultados=None)

    db = get_db()
    resultados = {
        'clubes': [],
        'jogadores': [],
        'treinadores': [],
        'arbitros': [],
        'estadios': []
    }

    # Buscar clubes
    clubes = db.execute("""
        SELECT c.ID, c.clube, c.apelido, l.cidade, l.UF
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        WHERE c.clube LIKE ? OR c.apelido LIKE ?
        LIMIT 10
    """, (f'%{query}%', f'%{query}%')).fetchall()
    resultados['clubes'] = [dict_from_row(r) for r in clubes]

    # Buscar jogadores
    jogadores = db.execute("""
        SELECT ID, nome, apelido, posicao, nascimento
        FROM jogadores
        WHERE nome LIKE ? OR apelido LIKE ?
        LIMIT 10
    """, (f'%{query}%', f'%{query}%')).fetchall()
    resultados['jogadores'] = [dict_from_row(r) for r in jogadores]

    # Buscar treinadores
    treinadores = db.execute("""
        SELECT ID, nome, apelido, nacionalidade
        FROM treinadores
        WHERE nome LIKE ? OR apelido LIKE ?
        LIMIT 10
    """, (f'%{query}%', f'%{query}%')).fetchall()
    resultados['treinadores'] = [dict_from_row(r) for r in treinadores]

    # Buscar árbitros
    arbitros = db.execute("""
        SELECT ID, nome, naturalidade
        FROM arbitros
        WHERE nome LIKE ?
        LIMIT 10
    """, (f'%{query}%',)).fetchall()
    resultados['arbitros'] = [dict_from_row(r) for r in arbitros]

    # Buscar estádios
    estadios = db.execute("""
        SELECT e.ID, e.estadio, l.cidade, l.UF, e.capacidade
        FROM estadios e
        LEFT JOIN locais l ON e.local_id = l.ID
        WHERE e.estadio LIKE ?
        LIMIT 10
    """, (f'%{query}%',)).fetchall()
    resultados['estadios'] = [dict_from_row(r) for r in estadios]

    return render_template('buscar.html', query=query, resultados=resultados)

@app.route("/temporada/<int:ano>")
def temporada(ano):
    """
    Página da temporada com classificação.
    IMPORTANTE: Aqui tratamos grupos quando existem.
    """
    db = get_db()

    # Informações da edição
    edicao = db.execute("""
        SELECT ed.*, c.clube as campeao, cv.clube as vice
        FROM edicoes ed
        LEFT JOIN clubes c ON ed.campeao_id = c.ID
        LEFT JOIN clubes cv ON ed.vice_id = cv.ID
        WHERE ed.ano = ?
    """, (ano,)).fetchone()

    if not edicao:
        return render_template('error.html',
                             mensagem=f"Temporada {ano} não encontrada."), 404

    # Descobrir formato do campeonato
    formato = get_formato_campeonato(ano)

    # Verificar se tem grupos neste ano
    grupos_existentes = db.execute("""
        SELECT DISTINCT mandante_grupo
        FROM partidas p
        JOIN edicoes ed ON p.edicao_id = ed.ID
        WHERE ed.ano = ? AND mandante_grupo IS NOT NULL
        ORDER BY mandante_grupo
    """, (ano,)).fetchall()

    classificacoes_por_grupo = {}

    if grupos_existentes:
        # Tem grupos! Calcular classificação de cada grupo
        for grupo_row in grupos_existentes:
            grupo = grupo_row['mandante_grupo']
            class_grupo = calcular_classificacao_grupo(ano, grupo)
            classificacoes_por_grupo[grupo] = class_grupo

        # Para temporada.html, vamos passar classificação geral também
        classificacao = calcular_classificacao_geral(ano)
    else:
        # Não tem grupos, classificação simples
        classificacao = calcular_classificacao_geral(ano)

    # Artilheiros
    artilheiros = db.execute("""
        SELECT
            j.nome,
            j.apelido,
            j.ID as jogador_id,
            SUM(jp.gols) as total_gols,
            COUNT(DISTINCT jp.partida_id) as jogos
        FROM jogadores_em_partida jp
        JOIN jogadores j ON jp.jogador_id = j.ID
        JOIN partidas p ON jp.partida_id = p.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        WHERE ed.ano = ? AND jp.gols > 0
        GROUP BY j.ID
        ORDER BY total_gols DESC, jogos ASC
        LIMIT 10
    """, (ano,)).fetchall()

    return render_template('temporada.html',
                         ano=ano,
                         edicao=dict_from_row(edicao),
                         classificacao=[dict_from_row(r) for r in classificacao],
                         classificacoes_por_grupo={g: [dict_from_row(r) for r in c]
                                                   for g, c in classificacoes_por_grupo.items()},
                         artilheiros=[dict_from_row(r) for r in artilheiros],
                         formato=formato)

def calcular_classificacao_geral(ano):
    """
    Calcula classificação geral do ano, respeitando sistema de pontos.
    Esta função é o coração do sistema de classificação!
    """
    db = get_db()
    pontos_vitoria = calcular_pontos_vitoria(ano)

    return db.execute(f"""
        WITH jogos_mandante AS (
            SELECT
                c.clube, c.ID as clube_id,
                p.mandante_placar AS gols_pro,
                p.visitante_placar AS gols_sofrido,
                CASE
                    WHEN p.mandante_placar > p.visitante_placar THEN {pontos_vitoria}
                    WHEN p.mandante_placar = p.visitante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS v,
                CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END AS e,
                CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END AS d
            FROM partidas p
            JOIN clubes c ON p.mandante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ?
        ),
        jogos_visitante AS (
            SELECT
                c.clube, c.ID as clube_id,
                p.visitante_placar AS gols_pro,
                p.mandante_placar AS gols_sofrido,
                CASE
                    WHEN p.visitante_placar > p.mandante_placar THEN {pontos_vitoria}
                    WHEN p.visitante_placar = p.mandante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS v,
                CASE WHEN p.visitante_placar = p.mandante_placar THEN 1 ELSE 0 END AS e,
                CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END AS d
            FROM partidas p
            JOIN clubes c ON p.visitante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ?
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pontos) DESC, SUM(v) DESC,
                (SUM(gols_pro) - SUM(gols_sofrido)) DESC, SUM(gols_pro) DESC
            ) AS pos,
            clube,
            clube_id,
            COUNT(*) AS j,
            SUM(pontos) AS pts,
            SUM(v) AS v,
            SUM(e) AS e,
            SUM(d) AS d,
            SUM(gols_pro) AS gp,
            SUM(gols_sofrido) AS gc,
            (SUM(gols_pro) - SUM(gols_sofrido)) AS sg
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pts DESC, v DESC, sg DESC, gp DESC
    """, (ano, ano)).fetchall()

def calcular_classificacao_grupo(ano, grupo):
    """
    Calcula classificação de um grupo específico.
    Usado para campeonatos com fase de grupos.
    """
    db = get_db()
    pontos_vitoria = calcular_pontos_vitoria(ano)

    return db.execute(f"""
        WITH jogos_mandante AS (
            SELECT
                c.clube,
                p.mandante_placar AS gols_pro,
                p.visitante_placar AS gols_sofrido,
                CASE
                    WHEN p.mandante_placar > p.visitante_placar THEN {pontos_vitoria}
                    WHEN p.mandante_placar = p.visitante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS v,
                CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END AS e,
                CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END AS d
            FROM partidas p
            JOIN clubes c ON p.mandante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ? AND p.mandante_grupo = ?
        ),
        jogos_visitante AS (
            SELECT
                c.clube,
                p.visitante_placar AS gols_pro,
                p.mandante_placar AS gols_sofrido,
                CASE
                    WHEN p.visitante_placar > p.mandante_placar THEN {pontos_vitoria}
                    WHEN p.visitante_placar = p.mandante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS v,
                CASE WHEN p.visitante_placar = p.mandante_placar THEN 1 ELSE 0 END AS e,
                CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END AS d
            FROM partidas p
            JOIN clubes c ON p.visitante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ? AND p.visitante_grupo = ?
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pontos) DESC, SUM(v) DESC,
                (SUM(gols_pro) - SUM(gols_sofrido)) DESC, SUM(gols_pro) DESC
            ) AS pos,
            clube,
            COUNT(*) AS j,
            SUM(pontos) AS pts,
            SUM(v) AS v,
            SUM(e) AS e,
            SUM(d) AS d,
            SUM(gols_pro) AS gp,
            SUM(gols_sofrido) AS gc,
            (SUM(gols_pro) - SUM(gols_sofrido)) AS sg
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pts DESC, v DESC, sg DESC, gp DESC
    """, (ano, grupo, ano, grupo)).fetchall()

@app.route("/clube/<string:nome>")
def clube(nome):
    """Página detalhada de um clube"""
    db = get_db()

    info = db.execute("""
        SELECT c.*, l.cidade, l.estado, l.UF
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        WHERE c.clube = ?
    """, (nome,)).fetchone()

    if not info:
        return render_template('error.html',
                             mensagem=f"Clube '{nome}' não encontrado."), 404

    stats = db.execute("""
        SELECT
            COUNT(DISTINCT p.ID) as total_jogos,
            SUM(CASE
                WHEN (cm.clube = ? AND p.mandante_placar > p.visitante_placar) OR
                     (cv.clube = ? AND p.visitante_placar > p.mandante_placar)
                THEN 1 ELSE 0 END) as vitorias,
            SUM(CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END) as empates,
            SUM(CASE
                WHEN (cm.clube = ? AND p.mandante_placar < p.visitante_placar) OR
                     (cv.clube = ? AND p.visitante_placar < p.mandante_placar)
                THEN 1 ELSE 0 END) as derrotas
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        WHERE cm.clube = ? OR cv.clube = ?
    """, (nome, nome, nome, nome, nome, nome)).fetchone()

    ultimos_jogos = db.execute("""
        SELECT
            p.ID,
            cm.clube as mandante,
            cv.clube as visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.data,
            ed.ano,
            p.fase
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        WHERE cm.clube = ? OR cv.clube = ?
        ORDER BY ed.ano DESC, p.data DESC
        LIMIT 20
    """, (nome, nome)).fetchall()

    return render_template('clube.html',
                         clube=dict_from_row(info),
                         stats=dict_from_row(stats),
                         ultimos_jogos=[dict_from_row(r) for r in ultimos_jogos])

# Continuarei com as rotas restantes (jogo, jogador, etc.) na próxima mensagem
# por questão de tamanho...

@app.route("/jogo/<int:jogo_id>")
def jogo(jogo_id):
    """
    NOVA ROTA: Página completa do jogo com TODAS as estatísticas.
    Esta é a página mais complexa, pois mostra:
    - Placar e informações básicas
    - Escalações completas
    - Gols e assists
    - Cartões
    - Estatísticas do jogo (finalizações, posse, etc.)
    - Árbitros e treinadores
    """
    db = get_db()

    # Buscar dados completos da partida
    partida = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cm.ID as mandante_id,
            cv.clube AS visitante,
            cv.ID as visitante_id,
            p.mandante_placar AS gols_mandante,
            p.visitante_placar AS gols_visitante,
            p.mandante_penalti,
            p.visitante_penalti,
            p.prorrogacao,
            p.data,
            p.hora,
            e_estadio.estadio AS arena,
            e_estadio.ID as estadio_id,
            p.fase AS rodada,
            l.cidade as estadio_cidade,
            l.UF AS estadio_uf,
            ed.ano,
            camp.campeonato,
            p.publico,
            p.renda
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        JOIN campeonatos camp ON ed.campeonato_id = camp.ID
        LEFT JOIN estadios e_estadio ON p.estadio_id = e_estadio.ID
        LEFT JOIN locais l ON e_estadio.local_id = l.ID
        WHERE p.ID = ?
    """, (jogo_id,)).fetchone()

    if not partida:
        return render_template('error.html', mensagem="Partida não encontrada."), 404

    partida = dict_from_row(partida)

    # Buscar eventos da partida (gols com assists)
    eventos = db.execute("""
        SELECT
            ep.tipo_evento,
            ep.tipo_gol,
            ep.minuto,
            j.nome as jogador_nome,
            j.apelido as jogador_apelido,
            j.ID as jogador_id,
            c.clube
        FROM eventos_partida ep
        JOIN jogadores j ON ep.jogador_id = j.ID
        JOIN clubes c ON ep.clube_id = c.ID
        WHERE ep.partida_id = ?
        ORDER BY ep.minuto
    """, (jogo_id,)).fetchall()

    # Separar gols por time
    gols_mandante = [dict_from_row(e) for e in eventos if e['tipo_evento'] == 'Gol' and e['clube'] == partida['mandante']]
    gols_visitante = [dict_from_row(e) for e in eventos if e['tipo_evento'] == 'Gol' and e['clube'] == partida['visitante']]
    cartoes_amarelos = [dict_from_row(e) for e in eventos if e['tipo_evento'] == 'cartao_amarelo']
    cartoes_vermelhos = [dict_from_row(e) for e in eventos if e['tipo_evento'] == 'cartao_vermelho']

    # Buscar jogadores que participaram
    jogadores_partida = db.execute("""
        SELECT
            j.nome,
            j.apelido,
            j.ID as jogador_id,
            c.clube,
            jp.titular,
            jp.posicao_jogada,
            jp.numero_camisa,
            jp.minutos_jogados,
            jp.gols,
            jp.assistencias,
            jp.cartao_amarelo,
            jp.cartao_vermelho
        FROM jogadores_em_partida jp
        JOIN jogadores j ON jp.jogador_id = j.ID
        JOIN clubes c ON jp.clube_id = c.ID
        WHERE jp.partida_id = ?
        ORDER BY c.clube, jp.titular DESC, jp.numero_camisa
    """, (jogo_id,)).fetchall()

    # Separar jogadores por time
    jogadores_mandante = [dict_from_row(j) for j in jogadores_partida if j['clube'] == partida['mandante']]
    jogadores_visitante = [dict_from_row(j) for j in jogadores_partida if j['clube'] == partida['visitante']]

    # Buscar estatísticas do jogo (finalizações, posse de bola, etc.)
    estatisticas_jogo = db.execute("""
        SELECT
            c.clube,
            ep.chutes,
            ep.chutes_no_alvo,
            ep.posse_de_bola,
            ep.passes,
            ep.precisao_passes,
            ep.faltas,
            ep.cartao_amarelo,
            ep.cartao_vermelho,
            ep.impedimentos,
            ep.escanteios
        FROM estatisticas_partida ep
        JOIN clubes c ON ep.clube_id = c.ID
        WHERE ep.partida_id = ?
    """, (jogo_id,)).fetchall()

    # Buscar árbitros
    arbitros = db.execute("""
        SELECT
            a.nome,
            a.ID as arbitro_id,
            a.naturalidade
        FROM arbitros_em_partida ap
        JOIN arbitros a ON ap.arbitro_id = a.ID
        WHERE ap.partida_id = ?
    """, (jogo_id,)).fetchall()

    # Buscar treinadores
    treinadores = db.execute("""
        SELECT
            t.nome,
            t.ID as treinador_id,
            c.clube,
            tp.tipo
        FROM treinadores_em_partida tp
        JOIN treinadores t ON tp.treinador_id = t.ID
        JOIN clubes c ON tp.clube_id = c.ID
        WHERE tp.partida_id = ?
    """, (jogo_id,)).fetchall()

    return render_template(
        "jogo.html",
        partida=partida,
        gols_mandante=gols_mandante,
        gols_visitante=gols_visitante,
        cartoes_amarelos=cartoes_amarelos,
        cartoes_vermelhos=cartoes_vermelhos,
        jogadores_mandante=jogadores_mandante,
        jogadores_visitante=jogadores_visitante,
        estatisticas=[dict_from_row(e) for e in estatisticas_jogo],
        arbitros=[dict_from_row(a) for a in arbitros],
        treinadores=[dict_from_row(t) for t in treinadores]
    )

# ROTAS SIMPLES para páginas individuais

@app.route("/jogador/<int:jogador_id>")
def jogador(jogador_id):
    """Página do jogador"""
    db = get_db()

    info_jogador = db.execute("""
        SELECT *
        FROM jogadores
        WHERE ID = ?
    """, (jogador_id,)).fetchone()

    if not info_jogador:
        return render_template('error.html', mensagem="Jogador não encontrado."), 404

    partidas = db.execute("""
        SELECT
            p.ID as partida_id,
            p.data,
            ed.ano,
            cm.clube as mandante,
            cv.clube as visitante,
            p.mandante_placar,
            p.visitante_placar,
            c.clube as clube_jogador,
            jp.gols,
            jp.assistencias,
            jp.cartao_amarelo,
            jp.cartao_vermelho,
            jp.titular
        FROM jogadores_em_partida jp
        JOIN partidas p ON jp.partida_id = p.ID
        JOIN clubes c ON jp.clube_id = c.ID
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        WHERE jp.jogador_id = ?
        ORDER BY ed.ano DESC, p.data DESC
    """, (jogador_id,)).fetchall()

    return render_template("jogador.html",
                          jogador=dict_from_row(info_jogador),
                          partidas=[dict_from_row(p) for p in partidas])

@app.route("/arbitro/<int:arbitro_id>")
def arbitro(arbitro_id):
    """Página do árbitro"""
    db = get_db()

    info_arbitro = db.execute("""
        SELECT *
        FROM arbitros
        WHERE ID = ?
    """, (arbitro_id,)).fetchone()

    if not info_arbitro:
        return render_template('error.html', mensagem="Árbitro não encontrado."), 404

    partidas = db.execute("""
        SELECT
            p.ID as partida_id,
            p.data,
            ed.ano,
            cm.clube as mandante,
            cv.clube as visitante,
            p.mandante_placar,
            p.visitante_placar,
            camp.campeonato
        FROM arbitros_em_partida ap
        JOIN partidas p ON ap.partida_id = p.ID
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        JOIN campeonatos camp ON ed.campeonato_id = camp.ID
        WHERE ap.arbitro_id = ?
        ORDER BY ed.ano DESC, p.data DESC
    """, (arbitro_id,)).fetchall()

    return render_template("arbitro.html",
                          arbitro=dict_from_row(info_arbitro),
                          partidas=[dict_from_row(p) for p in partidas])

@app.route("/treinador/<int:treinador_id>")
def treinador(treinador_id):
    """Página do treinador"""
    db = get_db()

    info_treinador = db.execute("""
        SELECT *
        FROM treinadores
        WHERE ID = ?
    """, (treinador_id,)).fetchone()

    if not info_treinador:
        return render_template('error.html', mensagem="Treinador não encontrado."), 404

    partidas = db.execute("""
        SELECT
            p.ID as partida_id,
            p.data,
            ed.ano,
            cm.clube as mandante,
            cv.clube as visitante,
            p.mandante_placar,
            p.visitante_placar,
            c.clube as clube_treinador,
            tp.tipo
        FROM treinadores_em_partida tp
        JOIN partidas p ON tp.partida_id = p.ID
        JOIN clubes c ON tp.clube_id = c.ID
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        WHERE tp.treinador_id = ?
        ORDER BY ed.ano DESC, p.data DESC
    """, (treinador_id,)).fetchall()

    return render_template("treinador.html",
                          treinador=dict_from_row(info_treinador),
                          partidas=[dict_from_row(p) for p in partidas])

@app.route("/estadio/<int:estadio_id>")
def estadio(estadio_id):
    """Página do estádio"""
    db = get_db()

    info_estadio = db.execute("""
        SELECT
            e.*,
            l.cidade,
            l.estado,
            l.UF
        FROM estadios e
        LEFT JOIN locais l ON e.local_id = l.ID
        WHERE e.ID = ?
    """, (estadio_id,)).fetchone()

    if not info_estadio:
        return render_template('error.html', mensagem="Estádio não encontrado."), 404

    partidas = db.execute("""
        SELECT
            p.ID as partida_id,
            p.data,
            ed.ano,
            cm.clube as mandante,
            cv.clube as visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.publico,
            camp.campeonato
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        JOIN campeonatos camp ON ed.campeonato_id = camp.ID
        WHERE p.estadio_id = ?
        ORDER BY ed.ano DESC, p.data DESC
        LIMIT 50
    """, (estadio_id,)).fetchall()

    return render_template("estadio.html",
                          estadio=dict_from_row(info_estadio),
                          partidas=[dict_from_row(p) for p in partidas])

# ==================== APIs PARA GRÁFICOS ====================

@app.route("/api/evolucao_clube/<string:nome>")
def api_evolucao_clube(nome):
    """Retorna dados de evolução de pontos de um clube ao longo dos anos"""
    db = get_db()

    evolucao = db.execute("""
        WITH jogos_clube AS (
            SELECT
                ed.ano,
                CASE
                    WHEN cm.clube = ? THEN
                        CASE
                            WHEN p.mandante_placar > p.visitante_placar THEN
                                CASE WHEN ed.ano <= 1994 THEN 2 ELSE 3 END
                            WHEN p.mandante_placar = p.visitante_placar THEN 1
                            ELSE 0
                        END
                    ELSE
                        CASE
                            WHEN p.visitante_placar > p.mandante_placar THEN
                                CASE WHEN ed.ano <= 1994 THEN 2 ELSE 3 END
                            WHEN p.visitante_placar = p.mandante_placar THEN 1
                            ELSE 0
                        END
                END AS pontos
            FROM partidas p
            JOIN clubes cm ON p.mandante_id = cm.ID
            JOIN clubes cv ON p.visitante_id = cv.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE cm.clube = ? OR cv.clube = ?
        )
        SELECT ano, SUM(pontos) as total_pontos
        FROM jogos_clube
        GROUP BY ano
        ORDER BY ano
    """, (nome, nome, nome)).fetchall()

    return jsonify([dict_from_row(r) for r in evolucao])

# ==================== FILTROS JINJA ====================

@app.template_filter('slugify')
def slugify_filter(text):
    return slugify(text)

