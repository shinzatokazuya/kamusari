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
    """Obtém uma conexão com o banco de dados."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    """Fecha a conexão com o banco ao final do request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def get_clubes():
    """Carrega a lista de clubes do banco."""
    db = get_db()
    clubes_query = db.execute("SELECT clube FROM clubes ORDER BY clube").fetchall()
    return [row['clube'] for row in clubes_query]

# ==================== HELPER: SISTEMA DE PONTUAÇÃO ====================

def calcular_pontos_vitoria(ano):
    """
    Retorna quantos pontos vale uma vitória baseado no ano.
    Até 1994: 2 pontos
    A partir de 1995: 3 pontos
    """
    return 2 if ano <= 1994 else 3

# ==================== BEFORE REQUEST ====================

@app.before_request
def pass_global_data():
    """Passa dados globais como CLUBES e DATAS para todos os templates."""
    clubes = get_clubes()
    g.json_clubes = jsonify(clubes).get_data(as_text=True)
    g.DATAS = DATAS
    g.CLUBES = clubes

# ==================== ROUTES ====================

@app.route("/")
def index():
    """Renderiza a página inicial com a classificação geral histórica."""
    db = get_db()

    rankings_geral = db.execute("""
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
                CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END AS derrotas
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
                CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END AS vitorias,
                CASE WHEN p.visitante_placar = p.mandante_placar THEN 1 ELSE 0 END AS empates,
                CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END AS derrotas
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
    """Permite buscar por clubes, anos, fases ou rodadas."""
    db = get_db()
    q = request.args.get("q")
    ano = request.args.get("data")
    fase_param = request.args.get("fase")
    rodada_param = request.args.get("rodada")

    classificacoes = []
    classificacoes_por_grupo = {}
    jogos = []
    current_year = None
    current_fase = None
    current_round = None
    max_round = 0
    formato_campeonato = None
    fases_disponiveis = []

    if q:
        return redirect(f"/clube/{q}")

    elif ano:
        current_year = int(ano)
        formato_campeonato = get_formato_campeonato(current_year)

        # Buscar fases disponíveis para o ano
        fases_disponiveis = get_fases_disponiveis(current_year)

        # Se o usuário selecionou uma fase específica
        if fase_param:
            current_fase = fase_param
        elif fases_disponiveis:
            # Por padrão, seleciona a primeira fase disponível
            current_fase = fases_disponiveis[0]

        if formato_campeonato == 'pontos_corridos':
            max_round_result = db.execute("""
                SELECT MAX(p.rodada) AS max_r
                FROM partidas p
                JOIN edicoes e ON p.edicao_id = e.ID
                WHERE e.ano = ?
                    AND p.rodada IS NOT NULL
            """, (current_year,)).fetchone()

            if max_round_result and max_round_result['max_r'] is not None:
                max_round = max_round_result['max_r']

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
        else:
            # Para formatos com grupos e fases
            if current_fase:
                # Verificar se há grupos na fase
                grupos = get_grupos_por_fase(current_year, current_fase)

                if grupos:
                    # Buscar classificação por grupo
                    for grupo in grupos:
                        classificacoes_por_grupo[grupo] = get_classificacao_por_grupo(current_year, current_fase, grupo)
                else:
                    # Classificação geral da fase
                    classificacoes = get_classificacao_por_fase(current_year, current_fase)

                # Buscar jogos da fase
                jogos = get_jogos_por_fase(current_year, current_fase)

    return render_template("search.html",
                           classificacoes=classificacoes,
                           classificacoes_por_grupo=classificacoes_por_grupo,
                           jogos=jogos,
                           q=q,
                           ano_selecionado=current_year,
                           fase_selecionada=current_fase,
                           rodada_selecionada=current_round,
                           max_rodada=max_round,
                           formato_campeonato=formato_campeonato,
                           fases_disponiveis=fases_disponiveis,
                           datas=g.DATAS,
                           clubes_json=g.json_clubes)

@app.route("/clube/<string:nome>")
def clube(nome):
    """Página de um clube específico com informações e últimos jogos."""
    db = get_db()

    # Buscar informações do clube
    info_clube = db.execute("""
        SELECT
            c.clube,
            c.apelido,
            c.fundacao,
            c.ativo,
            l.cidade,
            l.estado,
            l.UF,
            l.regiao
        FROM clubes c
        LEFT JOIN locais l ON c.local_id = l.ID
        WHERE c.clube = ?
    """, (nome,)).fetchone()

    if not info_clube:
        return render_template("error.html", mensagem=f"Clube '{nome}' não encontrado."), 404

    # Buscar últimos 5 jogos por campeonato
    ultimos_jogos = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cv.clube AS visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.data,
            p.fase AS rodada,
            ed.ano,
            camp.campeonato,
            est.estadio AS arena
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        JOIN campeonatos camp ON ed.campeonato_id = camp.ID
        LEFT JOIN estadios est ON p.estadio_id = est.ID
        WHERE (cm.clube = ? OR cv.clube = ?)
        ORDER BY ed.ano DESC, p.data DESC
        LIMIT 100
    """, (nome, nome)).fetchall()

    # Organizar jogos por campeonato (últimos 5 de cada)
    jogos_por_campeonato = {}
    for jogo in ultimos_jogos:
        campeonato = jogo['campeonato']
        if campeonato not in jogos_por_campeonato:
            jogos_por_campeonato[campeonato] = []
        if len(jogos_por_campeonato[campeonato]) < 5:
            jogos_por_campeonato[campeonato].append(dict(jogo))

    # Estatísticas gerais do clube
    estatisticas = db.execute("""
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
                END AS pontos,
                CASE
                    WHEN cm.clube = ? THEN
                        CASE WHEN p.mandante_placar > p.visitante_placar THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN p.visitante_placar > p.mandante_placar THEN 1 ELSE 0 END
                END AS vitorias,
                CASE
                    WHEN p.mandante_placar = p.visitante_placar THEN 1 ELSE 0
                END AS empates,
                CASE
                    WHEN cm.clube = ? THEN
                        CASE WHEN p.mandante_placar < p.visitante_placar THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN p.visitante_placar < p.mandante_placar THEN 1 ELSE 0 END
                END AS derrotas
            FROM partidas p
            JOIN clubes cm ON p.mandante_id = cm.ID
            JOIN clubes cv ON p.visitante_id = cv.ID
            JOIN edicoes ed ON p.edicao_id = ed.ID
            WHERE cm.clube = ? OR cv.clube = ?
        )
        SELECT
            COUNT(*) as total_jogos,
            SUM(pontos) as total_pontos,
            SUM(vitorias) as total_vitorias,
            SUM(empates) as total_empates,
            SUM(derrotas) as total_derrotas
        FROM jogos_clube
    """, (nome, nome, nome, nome, nome)).fetchone()

    return render_template("clube.html",
                          clube_info=dict(info_clube),
                          jogos_por_campeonato=jogos_por_campeonato,
                          estatisticas=dict(estatisticas))

@app.route("/jogo/<int:jogo_id>")
@app.route("/jogo/<int:jogo_id>/<path:descricao>")
def jogo(jogo_id, descricao=None):
    """Página completa de uma partida com todos os detalhes."""
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
        return render_template("error.html", mensagem="Partida não encontrada."), 404

    partida = dict(partida)

    # Buscar estatísticas da partida (se disponíveis)
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

    # Buscar eventos da partida (gols, cartões)
    eventos = db.execute("""
        SELECT
            ep.tipo_evento,
            ep.tipo_gol,
            ep.minuto,
            j.nome as jogador_nome,
            j.apelido as jogador_apelido,
            c.clube
        FROM eventos_partida ep
        JOIN jogadores j ON ep.jogador_id = j.ID
        JOIN clubes c ON ep.clube_id = c.ID
        WHERE ep.partida_id = ?
        ORDER BY ep.minuto
    """, (jogo_id,)).fetchall()

    # Separar eventos por tipo
    gols = [dict(e) for e in eventos if e['tipo_evento'] == 'gol']
    cartoes_amarelos = [dict(e) for e in eventos if e['tipo_evento'] == 'cartao_amarelo']
    cartoes_vermelhos = [dict(e) for e in eventos if e['tipo_evento'] == 'cartao_vermelho']

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
    jogadores_mandante = [dict(j) for j in jogadores_partida if j['clube'] == partida['mandante']]
    jogadores_visitante = [dict(j) for j in jogadores_partida if j['clube'] == partida['visitante']]

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
        estatisticas=[dict(e) for e in estatisticas_jogo],
        gols=gols,
        cartoes_amarelos=cartoes_amarelos,
        cartoes_vermelhos=cartoes_vermelhos,
        jogadores_mandante=jogadores_mandante,
        jogadores_visitante=jogadores_visitante,
        arbitros=[dict(a) for a in arbitros],
        treinadores=[dict(t) for t in treinadores]
    )

@app.route("/jogador/<int:jogador_id>")
@app.route("/jogador/<int:jogador_id>/<path:nome>")
def jogador(jogador_id, nome=None):
    """Página de um jogador específico."""
    db = get_db()

    # Informações do jogador
    info_jogador = db.execute("""
        SELECT *
        FROM jogadores
        WHERE ID = ?
    """, (jogador_id,)).fetchone()

    if not info_jogador:
        return render_template("error.html", mensagem="Jogador não encontrado."), 404

    # Partidas do jogador
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
                          jogador=dict(info_jogador),
                          partidas=[dict(p) for p in partidas])

@app.route("/arbitro/<int:arbitro_id>")
@app.route("/arbitro/<int:arbitro_id>/<path:nome>")
def arbitro(arbitro_id, nome=None):
    """Página de um árbitro específico."""
    db = get_db()

    info_arbitro = db.execute("""
        SELECT *
        FROM arbitros
        WHERE ID = ?
    """, (arbitro_id,)).fetchone()

    if not info_arbitro:
        return render_template("error.html", mensagem="Árbitro não encontrado."), 404

    # Partidas apitadas
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
                          arbitro=dict(info_arbitro),
                          partidas=[dict(p) for p in partidas])

@app.route("/treinador/<int:treinador_id>")
@app.route("/treinador/<int:treinador_id>/<path:nome>")
def treinador(treinador_id, nome=None):
    """Página de um treinador específico."""
    db = get_db()

    info_treinador = db.execute("""
        SELECT *
        FROM treinadores
        WHERE ID = ?
    """, (treinador_id,)).fetchone()

    if not info_treinador:
        return render_template("error.html", mensagem="Treinador não encontrado."), 404

    # Partidas como treinador
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
                          treinador=dict(info_treinador),
                          partidas=[dict(p) for p in partidas])

@app.route("/estadio/<int:estadio_id>")
@app.route("/estadio/<int:estadio_id>/<path:nome>")
def estadio(estadio_id, nome=None):
    """Página de um estádio específico."""
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
        return render_template("error.html", mensagem="Estádio não encontrado."), 404

    # Partidas no estádio
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
                          estadio=dict(info_estadio),
                          partidas=[dict(p) for p in partidas])

# ==================== API ROUTES ====================

@app.route("/api/classificacao/<int:ano>/<int:rodada>")
def api_classificacao(ano, rodada):
    """Retorna a classificação para um dado ano e rodada."""
    classificacoes = get_classificacao_por_ano_e_rodada(ano, rodada)
    return jsonify([dict(row) for row in classificacoes])

@app.route("/api/jogos/<int:ano>/<int:rodada>")
def api_jogos(ano, rodada):
    """Retorna os jogos para um dado ano e rodada."""
    jogos = get_jogos_por_ano_e_rodada(ano, rodada if rodada != 0 else None)
    return jsonify([dict(row) for row in jogos])

@app.route("/api/max_rodada/<int:ano>")
def api_max_rodada(ano):
    """Retorna o máximo de rodadas para um dado ano."""
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

@app.route("/api/formato_campeonato/<int:ano>")
def api_formato_campeonato(ano):
    """Retorna o formato do campeonato para um dado ano."""
    formato = get_formato_campeonato(ano)
    return jsonify({"formato": formato})

@app.route("/api/fases/<int:ano>")
def api_fases(ano):
    """Retorna as fases disponíveis para um ano."""
    fases = get_fases_disponiveis(ano)
    return jsonify(fases)

@app.route("/api/grupos/<int:ano>/<string:fase>")
def api_grupos(ano, fase):
    """Retorna os grupos disponíveis para uma fase específica."""
    grupos = get_grupos_por_fase(ano, fase)
    return jsonify(grupos)

@app.route("/api/classificacao_grupo/<int:ano>/<string:fase>/<string:grupo>")
def api_classificacao_grupo(ano, fase, grupo):
    """Retorna a classificação de um grupo específico."""
    classificacao = get_classificacao_por_grupo(ano, fase, grupo)
    return jsonify([dict(row) for row in classificacao])

# ==================== HELPER FUNCTIONS ====================

def slugify(text):
    """
    Converte texto para formato URL amigável.
    Ex: "São Paulo" -> "sao-paulo"
    """
    import unicodedata
    import re

    # Normaliza caracteres unicode
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Converte para minúsculas e substitui espaços por hífens
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)

    return text.strip('-')

def make_url_jogador(jogador_id, nome):
    """Cria URL amigável para jogador."""
    return f"/jogador/{jogador_id}/{slugify(nome)}"

def make_url_arbitro(arbitro_id, nome):
    """Cria URL amigável para árbitro."""
    return f"/arbitro/{arbitro_id}/{slugify(nome)}"

def make_url_treinador(treinador_id, nome):
    """Cria URL amigável para treinador."""
    return f"/treinador/{treinador_id}/{slugify(nome)}"

def make_url_estadio(estadio_id, nome):
    """Cria URL amigável para estádio."""
    return f"/estadio/{estadio_id}/{slugify(nome)}"

def make_url_jogo(jogo_id, mandante, visitante):
    """Cria URL amigável para jogo."""
    slug = f"{slugify(mandante)}-vs-{slugify(visitante)}"
    return f"/jogo/{jogo_id}/{slug}"

# Adicionar funções ao contexto do Jinja2
@app.context_processor
def utility_processor():
    return dict(
        make_url_jogador=make_url_jogador,
        make_url_arbitro=make_url_arbitro,
        make_url_treinador=make_url_treinador,
        make_url_estadio=make_url_estadio,
        make_url_jogo=make_url_jogo
    )

def get_formato_campeonato(ano):
    """
    Determina o formato do campeonato baseado no ano.
    - pontos_corridos: a partir de 2003
    - mata_mata: 1992-2002
    - grupos_fases: até 1991
    """
    if ano >= 2003:
        return 'pontos_corridos'
    elif ano >= 1992:
        return 'mata_mata'
    else:
        return 'grupos_fases'

def get_jogos_por_ano_e_fase(ano):
    """Retorna todos os jogos de um ano, organizados por fase."""
    db = get_db()

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

    return jogos

def get_jogos_por_ano_e_rodada(ano, rodada_num=None):
    """Retorna os jogos de um ano e rodada específicos."""
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
        fase_busca = f"R{rodada_num}"

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

    return jogos

def get_classificacao_por_ano_e_rodada(ano, rodada_num=None):
    """Calcula a classificação do campeonato até uma rodada específica ou a classificação final."""
    db = get_db()

    # Calcula os pontos por vitória baseado no ano
    pontos_vitoria = calcular_pontos_vitoria(ano)

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
            {where_clause}
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
    """, params * 2).fetchall()

    return rankings

def get_formato_campeonato(ano):
    """Determina o formato do campeonato baseado no ano."""
    if ano >= 2003:
        return 'pontos_corridos'
    elif ano >= 1992:
        return 'mata_mata'
    else:
        return 'grupos_fases'

def get_fases_disponiveis(ano):
    """Retorna lista de fases únicas disponíveis para um ano."""
    db = get_db()

    result = db.execute("""
        SELECT DISTINCT p.fase
        FROM partidas p
        JOIN edicoes e ON p.edicao_id = e.ID
        WHERE e.ano = ?
            AND p.fase IS NOT NULL
            AND p.fase != ''
        ORDER BY
            CASE
                WHEN p.fase LIKE '%Primeira%' OR p.fase LIKE '%1ª%' THEN 1
                WHEN p.fase LIKE '%Segunda%' OR p.fase LIKE '%2ª%' THEN 2
                WHEN p.fase LIKE '%Terceira%' OR p.fase LIKE '%3ª%' THEN 3
                WHEN p.fase LIKE '%Final%' THEN 4
                ELSE 5
            END
    """, (ano,)).fetchall()

    return [row['fase'] for row in result]

def get_grupos_por_fase(ano, fase):
    """Retorna lista de grupos disponíveis para uma fase específica."""
    db = get_db()

    result = db.execute("""
        SELECT DISTINCT p.grupo
        FROM partidas p
        JOIN edicoes e ON p.edicao_id = e.ID
        WHERE e.ano = ?
            AND p.fase = ?
            AND p.grupo IS NOT NULL
            AND p.grupo != ''
        ORDER BY p.grupo
    """, (ano, fase)).fetchall()

    return [row['grupo'] for row in result]

def get_classificacao_por_grupo(ano, fase, grupo):
    """Calcula a classificação de um grupo específico."""
    db = get_db()
    pontos_vitoria = calcular_pontos_vitoria(ano)

    rankings = db.execute(f"""
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
                AND p.fase = ?
                AND p.mandante_grupo = ?
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
                AND p.fase = ?
                AND p.visitante_grupo = ?
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
    """, (ano, fase, grupo, ano, fase, grupo)).fetchall()

    return rankings

def get_classificacao_por_fase(ano, fase):
    """Calcula a classificação geral de uma fase (sem divisão por grupos)."""
    db = get_db()
    pontos_vitoria = calcular_pontos_vitoria(ano)

    rankings = db.execute(f"""
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
            WHERE ed.ano = ? AND p.fase = ?
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
            WHERE ed.ano = ? AND p.fase = ?
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
    """, (ano, fase, ano, fase)).fetchall()

    return rankings

def get_jogos_por_fase(ano, fase):
    """Retorna todos os jogos de uma fase específica."""
    db = get_db()

    jogos = db.execute("""
        SELECT
            p.ID,
            cm.clube AS mandante,
            cv.clube AS visitante,
            p.mandante_placar,
            p.visitante_placar,
            p.data,
            p.fase,
            p.grupo,
            p.rodada,
            est.estadio AS arena
        FROM partidas p
        JOIN clubes cm ON p.mandante_id = cm.ID
        JOIN clubes cv ON p.visitante_id = cv.ID
        JOIN edicoes ed ON p.edicao_id = ed.ID
        LEFT JOIN estadios est ON p.estadio_id = est.ID
        WHERE ed.ano = ? AND p.fase = ?
        ORDER BY p.grupo, p.rodada, p.data
    """, (ano, fase)).fetchall()

    return jogos

if __name__ == "__main__":
    app.run(debug=True)
