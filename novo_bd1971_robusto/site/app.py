# app.py
import functools
from flask import Flask, render_template, jsonify, request, redirect, g
from collections import defaultdict
from datetime import datetime
import sqlite3

app = Flask(__name__)

# Configurações
DATABASE = "../bd/estruturado_bd_1971.db"
DATAS = list(range(1971, 2026))

# ==================== DATABASE MANAGEMENT ====================

def get_db():
    """" Obtém uma conexão com o banco de dados. """
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row # Permite acessar colunas por nome
    return g.db

@app.teardown_appcontext
def close_db(error):
    """ Fecha a conexão com o banco ao final do request. """
    db = g.pop('db', None)
    if db is not None:
        db.close()

def get_clubes():
    """ Carrega a lista de clubes do banco. """
    db = get_db()
    clubes_query = db.execute("SELECT clube FROM clubes").fetchall()
    return [row['clube'] for row in clubes_query]

# ==================== BEFORE REQUEST ====================

@app.before_request
def pass_global_data():
    """ Passa dados globais como CLUBES e DATAS para todos os templates. """
    clubes = get_clubes()

    # Converte a lista Python de clubes para STRING JSON para ser usada diretamente no JS do template
    g.json_clubes = jsonify(clubes).get_data(as_text=True)
    g.DATAS = DATAS # Anos disponíveis
    g.CLUBES = clubes # Lista de clubes para uso no backend, se necessário

# ==================== ROUTES ====================

@app.route("/")
def index():
    """Renderiza a página inicial com a classificação geral."""
    db = get_db()

    # Query para classificação geral usando o schema correto
    rankings_geral = db.execute("""
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
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pontos) DESC,
                         SUM(vitorias) DESC,
                         (SUM(gols_pro) - SUM(gols_sofrido)) DESC,
                         SUM(gols_pro) DESC
            ) AS posicao,
            clube,
            COUNT(*) AS total_jogos,
            SUM(pontos) AS pontos,
            SUM(vitorias) AS vitorias,
            SUM(empates) AS empates,
            SUM(derrotas) AS derrotas,
            SUM(gols_pro) AS gm,
            SUM(gols_sofrido) AS gs,
            (SUM(gols_pro) - SUM(gols_sofrido)) AS sg
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pontos DESC, vitorias DESC, sg DESC, gm DESC
    """).fetchall()

    return render_template("index.html", rankings=rankings_geral, datas=g.DATAS, clubes_json=g.json_clubes)

@app.route("/search")
def search():
    """ Permite buscar por clubes, anos ou rodadas. """
    db = get_db()
    q = request.args.get("q")
    ano = request.args.get("data")
    rodada_param = request.args.get("rodada")

    classificacoes = []
    jogos = []
    current_year = None
    current_round = None
    max_round = 0

    if q:
        # Busca por clube
        jogos_clube, jogos_por_ano = get_jogos_por_clube(q)
        return render_template("clube.html", clubes=q, jogos_por_ano=jogos_por_ano)

    elif ano:
        current_year = int(ano)

        # Buscar a rodada máxima para o ano
        max_round_result = db.execute("""
            SELECT MAX(CAST(p.fase AS INTEGER)) AS max_r
            FROM partidas p
            JOIN edicoes e ON p.edicao_id = e.ID
            WHERE e.ano = ?
                AND p.fase LIKE 'R%'
                AND LENGTH(p.fase) > 1
                AND SUBSTR(p.fase, 2) GLOB '[0-9]*'
        """, (current_year,)).fetchone()

        if max_round_result and max_round_result['max_r'] is not None:
            max_round = max_round_result['max_r']
        else:
            max_round = 0

        if rodada_param and rodada_param.isdigit():
            current_round = int(rodada_param)
            if current_round == 0:
                classificacoes = get_classificacao_por_ano_e_rodada(current_year, 0)
                jogos = get_jogos_por_ano_e_rodada(current_year, None)
            else:
                classificacoes = get_classificacao_por_ano_e_rodada(current_year, current_round)
                jogos = get_jogos_por_ano_e_rodada(current_year, current_round)
        else:
            current_round = max_round
            classificacoes = get_classificacao_por_ano_e_rodada(current_year, current_round)
            jogos = get_jogos_por_ano_e_rodada(current_year, current_round)

    return render_template("search.html",
                           classificacoes=classificacoes,
                           jogos=jogos,
                           q=q,
                           ano_selecionado=current_year,
                           rodada_selecionada=current_round,
                           max_rodada=max_round,
                           datas=g.DATAS,
                           clubes_json=g.json_clubes)

@app.route("/clube/<string:nome>")
def clube(nome):
    """ Página de um clube específico. """
    jogos_clube, jogos_por_ano = get_jogos_por_clube(nome)
    return render_template("clube.html", clube=nome, jogos_por_ano=jogos_por_ano)

@app.route("/estatisticas/<int:jogo_id>")
def estatisticas(jogo_id):
    """ Página de estatísticas de uma partida. """
    db = get_db()

    # Buscar dados do confronto
    confronto = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cv.clube AS visitante,
            p.mandante_placar AS gols_mandante,
            p.visitante_placar AS gols_visitante,
            p.data,
            p.hora,
            e_estadio.estadio AS arena,
            p.fase AS rodada,
            l.UF AS mandante_Estado,
            ed.ano
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        LEFT JOIN estadios e_estadio ON p.estadio_id = e_estadio.ID
        LEFT JOIN locais l ON cm.local_id = l.ID
        WHERE p.ID = ?
    """, (jogo_id)).fetchone()

    if confronto:
        confronto = dict(confronto)
    else:
        confronto = {}

    # Como não temos AINDA dados de estatisticas, gols, jogadores e cartões
    estatisticas_jogo = []
    gols_jogo = []
    cartoes_jogo = []

    return render_template(
        "estatisticas.html",
        estatisticas=estatisticas_jogo,
        gols=gols_jogo,
        cartoes=cartoes_jogo,
        confronto=confronto,
    )

# ==================== API ROUTES ====================

@app.route("/api/classificacao/<int:ano>/<int:rodada>")
def api_classificacao(ano, rodada):
    """ Retorna a classificação para um dado ano e rodada. """
    classificacoes = get_classificacao_por_ano_e_rodada(ano, rodada)
    return jsonify([dict(row) for row in classificacoes])

@app.route("/api/jogos/<int:ano>/<int:rodada>")
def api_jogos(ano, rodada):
    """ Retorna os jogos para um dado ano e rodada. """
    jogos = get_jogos_por_ano_e_rodada(ano, rodada if rodada != 0 else None)
    return jsonify([dict(row) for row in jogos])

@app.route("/api/max_rodada/<int:ano>")
def api_max_rodada(ano):
    """ Retorna o máximo de rodadas para um dado ano. """
    db = get_db()

    result = db.execute("""
        SELECT MAX(CAST(SUBSTR(p.fase, 2) AS INTEGER)) AS max_r
        FROM partidas p
        JOIN edicoes e ON p.edicao_id = e.ID
        WHERE e.ano = ?
            AND p.fase LIKE 'R%'
            AND LENGTH(p.fase) > 1
            AND SUBSTR(p.fase, 2) GLOB '[0-9]*'
    """, (ano,)).fetchone()

    if result and result['max_r'] is not None:
        return jsonify(result['max_r'])

    return jsonify(0)

# ==================== HELPER FUNCTIONS ====================

def get_jogos_por_clube(nome):
    """ Retorna todos os jogos de um clube, separados por ano. """
    db = get_db()

    jogos_clube = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cv.clube AS visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.data,
            p.fase AS rodada,
            ed.ano,
            est.estadio AS arena
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        LEFT JOIN estadios est ON p.estadio_id = est.ID
        WHERE cm.clube = ? OR cv.clube = ?
        ORDER BY ed.ano DESC, p.data, p.fase
    """, (nome, nome)).fetchall()

    jogos_por_ano = {}
    jogos_adicionados = set()

    for jogo in jogos_clube:
        jogo_dict = dict(jogo)
        ano_jogo = jogo_dict['ano']
        jogo_id = jogo_dict['ID']

        if jogo_id in jogos_adicionados:
            continue
        jogos_adicionados.add(jogo_id)

        if ano_jogo not in jogos_por_ano:
            jogos_por_ano[ano_jogo] = []
        jogos_por_ano[ano_jogo].append(jogo_dict)

    # Ordena os anos de forma decrescente
    jogos_por_ano = dict(sorted(jogos_por_ano.items(), reverse=True))

    # Ordena cada ano por rodada
    for ano in jogos_por_ano:
        jogos_por_ano[ano] = sorted(jogos_por_ano[ano], key=lambda x: str(x["rodada"]))

    return jogos_clube, jogos_por_ano

def get_jogos_por_ano_e_rodada(ano, rodada_num=None):
    """ Retorna os jogos de um ano e rodada específicos. """
    db = get_db()

    if rodada_num is None:
        # Todos os jogos do ano
        jogos = db.execute("""
            SELECT
                p.ID,
                cm.clube AS mandante,
                cv.clube AS visitante,
                p.mandante_placar,
                p.visitante_placar,
                p.data,
                p.fase AS rodada,
                est.estadio AS arena
            FROM partidas p
            JOIN clubes cm ON p.mandante_id = cm.ID
            JOIN clubes cv ON p.visitante_id = cv.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            LEFT JOIN estadios est ON p.estadio_id = est.ID
            WHERE ed.ano = ?
            ORDER BY p.data, p.fase
        """, (ano,)).fetchall()
    else:
        # Jogos de uma rodada específica
        # IMPORTANTE: Monta a string da fase exatamente como está no banco (R1, R2, etc.)
        fase_busca = f"R{rodada_num}"
        print(f"DEBUG: Buscando jogos para ano={ano}, fase={fase_busca}")

        jogos = db.execute("""
            SELECT
                p.ID,
                cm.clube AS mandante,
                cv.clube AS visitante,
                p.mandante_placar,
                p.visitante_placar,
                p.data,
                p.fase AS rodada,
                est.estadio AS arena
            FROM partidas p
            JOIN clubes cm ON p.mandante_id = cm.ID
            JOIN clubes cv ON p.visitante_id = cv.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            LEFT JOIN estadios est ON p.estadio_id = est.ID
            WHERE ed.ano = ? AND p.fase = ?
            ORDER BY p.data
        """, (ano, fase_busca)).fetchall()

        print(f"DEBUG: Encontrados {len(jogos)} jogos")

    return jogos

def get_classificacao_por_ano_e_rodada(ano, rodada_num=None):
    """ Calcula a classificação do campeonato até uma rodada específica ou a classificação final. """
    db = get_db()

    if rodada_num is None or rodada_num == 0:
        # Classificação final do ano
        where_clause = """WHERE ed.ano = ?
                        AND p.fase LIKE 'R%'
                        AND LENGTH(p.fase) > 1
                        AND SUBSTR(p.fase, 2) GLOB '[0-9]*'"""
        params = (ano,)
    else:
        # Classificação até uma rodada específica
        where_clause = """WHERE ed.ano = ?
                        AND p.fase LIKE 'R%'
                        AND LENGTH(p.fase) > 1
                        AND SUBSTR(p.fase, 2) GLOB '[0-9]*'
                        AND CAST(SUBSTR(p.fase, 2) AS INTEGER) <= ?"""
        params = (ano, rodada_num)

    rankings = db.execute(f"""
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
            {where_clause}
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
            {where_clause}
        ),
        todos_jogos AS (
            SELECT * FROM jogos_mandante
            UNION ALL
            SELECT * FROM jogos_visitante
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pontos) DESC,
                         SUM(vitorias) DESC,
                         (SUM(gols_pro) - SUM(gols_sofrido)) DESC,
                         SUM(gols_pro) DESC
            ) AS posicao,
            clube,
            COUNT(*) AS total_jogos,
            SUM(pontos) AS pontos,
            SUM(vitorias) AS vitorias,
            SUM(empates) AS empates,
            SUM(derrotas) AS derrotas,
            SUM(gols_pro) AS gm,
            SUM(gols_sofrido) AS gs,
            (SUM(gols_pro) - SUM(gols_sofrido)) AS sg
        FROM todos_jogos
        GROUP BY clube
        ORDER BY pontos DESC, vitorias DESC, sg DESC, gm DESC
    """, params * 2).fetchall()  # Multiplica por 2 pois usamos where_clause em 2 CTEs

    return rankings
