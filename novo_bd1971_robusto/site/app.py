# app.py
from flask import Flask, render_template, jsonify, request, redirect, g
from datetime import datetime
import sqlite3
import json

app = Flask(__name__)

# ==================== CONFIGURAÇÕES ====================
DATABASE = "../bd/estruturado_bd_1971.db"
ANO_ATUAL = 2025  # Atualizar conforme necessário
ANOS_DISPONIVEIS = list(range(1971, ANO_ATUAL + 1))

# ==================== DATABASE MANAGEMENT ====================

def get_db():
    """Obtém conexão com banco de dados com row_factory para facilitar acesso"""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    """Fecha conexão ao fim da requisição"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def dict_from_row(row):
    """Converte sqlite3.Row para dict"""
    return dict(zip(row.keys(), row)) if row else None

# ==================== FUNÇÕES AUXILIARES ====================

def calcular_pontos_vitoria(ano):
    """
    Calcula pontos por vitória baseado no ano.
    Regra histórica: até 1994 = 2 pontos, depois = 3 pontos
    """
    return 2 if ano <= 1994 else 3

def slugify(text):
    """Transforma texto em URL amigável (ex: 'São Paulo' -> 'sao-paulo')"""
    import unicodedata
    import re

    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')

# ==================== BEFORE REQUEST ====================

@app.before_request
def setup_globals():
    """Configura variáveis globais disponíveis em todos os templates"""
    g.ANO_ATUAL = ANO_ATUAL
    g.ANOS_DISPONIVEIS = ANOS_DISPONIVEIS

# ==================== ROTAS PRINCIPAIS ====================

@app.route("/")
def index():
    """
    Homepage redesenhada com:
    - Resumo da temporada atual
    - Últimos jogos
    - Top 5 classificação histórica
    - Links rápidos para exploração
    """
    db = get_db()

    # 1. Classificação do ano atual (top 10)
    classificacao_atual = db.execute("""
        WITH jogos_mandante AS (
            SELECT
                c.clube,
                p.mandante_placar AS gols_pro,
                p.visitante_placar AS gols_sofrido,
                CASE
                    WHEN p.mandante_placar > p.visitante_placar THEN 3
                    WHEN p.mandante_placar = p.visitante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END AS derrotas
            FROM partidas p
            JOIN clubes c ON p.mandante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ? AND p.fase LIKE 'R%'
        ),
        jogos_visitante AS (
            SELECT
                c.clube,
                p.visitante_placar AS gols_pro,
                p.mandante_placar AS gols_sofrido,
                CASE
                    WHEN p.visitante_placar > p.mandante_placar THEN 3
                    WHEN p.visitante_placar = p.mandante_placar THEN 1
                    ELSE 0
                END AS pontos,
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.visitante_placar = p.mandante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END AS derrotas
            FROM partidas p
            JOIN clubes c ON p.visitante_id = c.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE ed.ano = ? AND p.fase LIKE 'R%'
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

    # 2. Últimos 10 jogos realizados
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

    # 3. Top 5 classificação histórica
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

    # 4. Estatísticas gerais do banco
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
    """
    Página de exploração onde usuário pode navegar por:
    - Anos e edições
    - Clubes
    - Estatísticas gerais
    """
    db = get_db()

    # Lista todos os clubes que já participaram
    clubes = db.execute("""
        SELECT DISTINCT c.clube, c.ID, l.UF, l.cidade
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        JOIN partidas p ON c.ID = p.mandante_id OR c.ID = p.visitante_id
        ORDER BY c.clube
    """).fetchall()

    # Campeões por ano
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
    """
    Sistema de busca unificado que procura em:
    - Clubes
    - Jogadores
    - Treinadores
    - Árbitros
    - Estádios
    """
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
    Página completa de uma temporada com:
    - Classificação final ou por rodada
    - Estatísticas da temporada
    - Artilharia
    - Gráfico de evolução
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

    # Classificação final
    pontos_vitoria = calcular_pontos_vitoria(ano)
    classificacao = db.execute(f"""
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
                         artilheiros=[dict_from_row(r) for r in artilheiros])

@app.route("/clube/<string:nome>")
def clube(nome):
    """Página detalhada de um clube"""
    db = get_db()

    # Informações básicas
    info = db.execute("""
        SELECT c.*, l.cidade, l.estado, l.UF
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        WHERE c.clube = ?
    """, (nome,)).fetchone()

    if not info:
        return render_template('error.html',
                             mensagem=f"Clube '{nome}' não encontrado."), 404

    # Estatísticas gerais
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

    # Últimos 20 jogos
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

@app.route("/api/comparacao_clubes")
def api_comparacao_clubes():
    """Compara estatísticas de múltiplos clubes"""
    clubes = request.args.getlist('clubes[]')

    if not clubes:
        return jsonify({"error": "Nenhum clube especificado"}), 400

    db = get_db()
    dados = []

    for clube in clubes:
        stats = db.execute("""
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
            SELECT SUM(pontos) as total_pontos, COUNT(*) as total_jogos
            FROM jogos_clube
        """, (clube, clube, clube)).fetchone()

        dados.append({
            'clube': clube,
            'pontos': stats['total_pontos'] or 0,
            'jogos': stats['total_jogos'] or 0
        })

    return jsonify(dados)

# ==================== FILTROS JINJA ====================

@app.template_filter('slugify')
def slugify_filter(text):
    return slugify(text)

# ==================== EXECUTAR APP ====================

if __name__ == "__main__":
    app.run(debug=True, port=5000)
