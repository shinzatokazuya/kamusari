"""
app.py — Backend Flask do Brasileirão Analytics
Execução: flask run  ou  python app.py

Estrutura de pastas esperada:
  brasileirao/
  ├── app.py
  ├── brasileirao.db
  └── templates/
        ├── base.html
        ├── temporada.html
        ├── jogador.html
        ├── jogo.html
        └── admin.html
"""

import sqlite3
import re
from datetime import datetime
from flask import Flask, render_template, jsonify, request, redirect, url_for, g, flash

app = Flask(__name__)
app.secret_key = "brasileirao_secret_2024"   # troque em produção

DATABASE = "brasileirao.db"   # ajuste o caminho se necessário


# ══════════════════════════════════════════════════════════════════════════════
# BANCO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db:
        db.close()


def q(sql: str, params=()) -> list[dict]:
    """Executa SELECT e devolve lista de dicts."""
    cur = get_db().execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def q1(sql: str, params=()) -> dict | None:
    """Executa SELECT e devolve primeiro resultado como dict ou None."""
    rows = q(sql, params)
    return rows[0] if rows else None


# ══════════════════════════════════════════════════════════════════════════════
# FILTROS JINJA
# ══════════════════════════════════════════════════════════════════════════════

@app.template_filter("data_br")
def data_br(valor: str | None) -> str:
    """
    Converte YYYY-MM-DD → DD/MM/YYYY para exibição.
    Strings que não seguem o padrão ISO são devolvidas como estão.
    """
    if not valor:
        return "–"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", str(valor))
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return valor


@app.template_filter("hora_fmt")
def hora_fmt(valor: str | None) -> str:
    if not valor:
        return "–"
    # Garante HH:MM mesmo que venha HH:MM:SS
    return str(valor)[:5]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def pontos_vitoria(ano: int) -> int:
    """Regra histórica: até 1994 vitória = 2 pts; depois = 3 pts."""
    return 2 if int(ano) <= 1994 else 3


def classificacao(ano: int) -> list[dict]:
    pv = pontos_vitoria(ano)
    return q(f"""
        WITH m AS (
            SELECT c.id AS clube_id, c.clube,
                   p.mandante_placar  AS gp_raw,
                   p.visitante_placar AS gc_raw,
                   CASE WHEN p.mandante_placar > p.visitante_placar THEN {pv}
                        WHEN p.mandante_placar = p.visitante_placar THEN 1
                        ELSE 0 END AS pts,
                   (p.mandante_placar > p.visitante_placar) AS v,
                   (p.mandante_placar = p.visitante_placar) AS e,
                   (p.mandante_placar < p.visitante_placar) AS d
            FROM partidas p
            JOIN clubes c ON c.id = p.mandante_id
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE ed.ano = ?
              AND p.mandante_placar IS NOT NULL
        ),
        a AS (
            SELECT c.id AS clube_id, c.clube,
                   p.visitante_placar AS gp_raw,
                   p.mandante_placar  AS gc_raw,
                   CASE WHEN p.visitante_placar > p.mandante_placar THEN {pv}
                        WHEN p.visitante_placar = p.mandante_placar THEN 1
                        ELSE 0 END AS pts,
                   (p.visitante_placar > p.mandante_placar) AS v,
                   (p.visitante_placar = p.mandante_placar) AS e,
                   (p.visitante_placar < p.mandante_placar) AS d
            FROM partidas p
            JOIN clubes c ON c.id = p.visitante_id
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE ed.ano = ?
              AND p.visitante_placar IS NOT NULL
        ),
        tudo AS (SELECT * FROM m UNION ALL SELECT * FROM a)
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY SUM(pts) DESC, SUM(v) DESC,
                         SUM(gp_raw)-SUM(gc_raw) DESC, SUM(gp_raw) DESC
            ) AS pos,
            clube_id, clube,
            COUNT(*)        AS j,
            SUM(pts)        AS pts,
            SUM(v)          AS v,
            SUM(e)          AS e,
            SUM(d)          AS d,
            SUM(gp_raw)     AS gp,
            SUM(gc_raw)     AS gc,
            SUM(gp_raw)-SUM(gc_raw) AS sg
        FROM tudo
        GROUP BY clube_id
        ORDER BY pts DESC, v DESC, sg DESC, gp DESC
    """, (ano, ano))


def artilheiros(ano: int, limite: int = 15) -> list[dict]:
    """
    Usa eventos_partida (tipo_evento='Gol', tipo_gol != 'Gol Contra')
    porque jogadores_em_partida não tem coluna de gols.
    """
    return q("""
        SELECT j.id AS jogador_id,
               COALESCE(NULLIF(j.apelido,''), j.nome) AS nome_exib,
               j.posicao,
               c.clube,
               COUNT(*) AS gols,
               COUNT(DISTINCT ep.partida_id) AS jogos
        FROM eventos_partida ep
        JOIN jogadores j ON j.id = ep.jogador_id
        JOIN clubes c    ON c.id = ep.clube_id
        JOIN partidas p  ON p.id = ep.partida_id
        JOIN edicoes ed  ON ed.id = p.edicao_id
        WHERE ed.ano = ?
          AND ep.tipo_evento = 'Gol'
          AND (ep.tipo_gol IS NULL OR ep.tipo_gol != 'Gol Contra')
        GROUP BY j.id, c.clube
        ORDER BY gols DESC, jogos ASC
        LIMIT ?
    """, (ano, limite))


def anos_disponiveis() -> list[int]:
    rows = q("SELECT DISTINCT CAST(ano AS INTEGER) AS ano FROM edicoes ORDER BY ano")
    return [r["ano"] for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# ROTAS PÚBLICAS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    anos = anos_disponiveis()
    ano = anos[-1] if anos else 2024
    return redirect(url_for("temporada", ano=ano))


@app.route("/temporada/<int:ano>")
def temporada(ano: int):
    anos = anos_disponiveis()
    if ano not in anos:
        return render_template("erro.html", msg=f"Temporada {ano} não encontrada."), 404

    edicao = q1("""
        SELECT ed.*, ch.clube AS campeao, vc.clube AS vice
        FROM edicoes ed
        LEFT JOIN clubes ch ON ch.id = ed.campeao_id
        LEFT JOIN clubes vc ON vc.id = ed.vice_id
        WHERE ed.ano = ?
    """, (ano,))

    tabela = classificacao(ano)
    top_artilheiros = artilheiros(ano)

    # Últimas 20 partidas com placar registrado
    jogos = q("""
        SELECT p.id, p.data, p.hora, p.fase, p.rodada, p.publico,
               cm.clube AS mandante, p.mandante_placar,
               cv.clube AS visitante, p.visitante_placar,
               p.prorrogacao, p.mandante_penalti, p.visitante_penalti,
               est.estadio
        FROM partidas p
        JOIN clubes cm ON cm.id = p.mandante_id
        JOIN clubes cv ON cv.id = p.visitante_id
        JOIN edicoes ed ON ed.id = p.edicao_id
        LEFT JOIN estadios est ON est.id = p.estadio_id
        WHERE ed.ano = ?
          AND p.mandante_placar IS NOT NULL
        ORDER BY p.data DESC, p.id DESC
        LIMIT 20
    """, (ano,))

    return render_template("temporada.html",
                           ano=ano,
                           anos=anos,
                           edicao=edicao,
                           tabela=tabela,
                           artilheiros=top_artilheiros,
                           jogos=jogos)


@app.route("/jogo/<int:jogo_id>")
def jogo(jogo_id: int):
    partida = q1("""
        SELECT p.*,
               cm.clube AS mandante, cm.id AS mandante_id,
               cv.clube AS visitante, cv.id AS visitante_id,
               est.estadio, lc.cidade AS est_cidade, lc.uf AS est_uf,
               ed.ano, camp.campeonato
        FROM partidas p
        JOIN clubes cm     ON cm.id = p.mandante_id
        JOIN clubes cv     ON cv.id = p.visitante_id
        JOIN edicoes ed    ON ed.id = p.edicao_id
        JOIN campeonatos camp ON camp.id = ed.campeonato_id
        LEFT JOIN estadios est ON est.id = p.estadio_id
        LEFT JOIN locais lc    ON lc.id = est.local_id
        WHERE p.id = ?
    """, (jogo_id,))

    if not partida:
        return render_template("erro.html", msg="Partida não encontrada."), 404

    eventos = q("""
        SELECT ep.tipo_evento, ep.tipo_gol, ep.minuto,
               COALESCE(NULLIF(j.apelido,''), j.nome) AS jogador,
               j.id AS jogador_id, c.clube
        FROM eventos_partida ep
        JOIN jogadores j ON j.id = ep.jogador_id
        JOIN clubes c    ON c.id = ep.clube_id
        WHERE ep.partida_id = ?
        ORDER BY CAST(REPLACE(ep.minuto, '+', '.') AS REAL) NULLS LAST
    """, (jogo_id,))

    # Escalações
    escalacao = q("""
        SELECT COALESCE(NULLIF(j.apelido,''), j.nome) AS jogador,
               j.id AS jogador_id, j.posicao,
               c.clube, jep.titular, jep.numero_camisa, jep.posicao_jogada
        FROM jogadores_em_partida jep
        JOIN jogadores j ON j.id = jep.jogador_id
        JOIN clubes c    ON c.id = jep.clube_id
        WHERE jep.partida_id = ?
        ORDER BY c.clube, jep.titular DESC, jep.numero_camisa NULLS LAST
    """, (jogo_id,))

    arbitros_jogo = q("""
        SELECT a.id AS arbitro_id, a.nome, a.naturalidade
        FROM arbitros_em_partida aep
        JOIN arbitros a ON a.id = aep.arbitro_id
        WHERE aep.partida_id = ?
    """, (jogo_id,))

    treinadores_jogo = q("""
        SELECT COALESCE(NULLIF(t.apelido,''), t.nome) AS treinador,
               t.id AS treinador_id, c.clube, tep.tipo
        FROM treinadores_em_partida tep
        JOIN clubes c ON c.id = tep.clube_id
        LEFT JOIN treinadores t ON t.id = tep.treinador_id
        WHERE tep.partida_id = ?
    """, (jogo_id,))

    return render_template("jogo.html",
                           partida=partida,
                           eventos=eventos,
                           escalacao=escalacao,
                           arbitros=arbitros_jogo,
                           treinadores=treinadores_jogo)


@app.route("/jogador/<int:jogador_id>")
def jogador(jogador_id: int):
    info = q1("SELECT * FROM jogadores WHERE id = ?", (jogador_id,))
    if not info:
        return render_template("erro.html", msg="Jogador não encontrado."), 404

    historico = q("""
        SELECT ed.ano,
               c.clube,
               COUNT(DISTINCT jep.partida_id) AS jogos,
               SUM(jep.titular)               AS titulares,
               COUNT(CASE WHEN ep.tipo_evento='Gol'
                          AND COALESCE(ep.tipo_gol,'')!='Gol Contra'
                     THEN 1 END)              AS gols,
               COUNT(CASE WHEN ep.tipo_evento='Assistência'
                     THEN 1 END)              AS assists,
               COUNT(CASE WHEN ep.tipo_evento='Cartão Amarelo'
                     THEN 1 END)              AS amarelos,
               COUNT(CASE WHEN ep.tipo_evento='Cartão Vermelho'
                     THEN 1 END)              AS vermelhos
        FROM jogadores_em_partida jep
        JOIN partidas p  ON p.id = jep.partida_id
        JOIN edicoes ed  ON ed.id = p.edicao_id
        JOIN clubes c    ON c.id = jep.clube_id
        LEFT JOIN eventos_partida ep
               ON ep.partida_id = jep.partida_id
              AND ep.jogador_id  = jep.jogador_id
        WHERE jep.jogador_id = ?
        GROUP BY ed.ano, c.clube
        ORDER BY ed.ano DESC
    """, (jogador_id,))

    return render_template("jogador.html", jogador=info, historico=historico)


# ══════════════════════════════════════════════════════════════════════════════
# APIs JSON PARA GRÁFICOS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/classificacao/<int:ano>")
def api_classificacao(ano: int):
    return jsonify(classificacao(ano))


@app.route("/api/artilheiros/<int:ano>")
def api_artilheiros(ano: int):
    return jsonify(artilheiros(ano, limite=10))


@app.route("/api/tendencia_clube/<int:clube_id>")
def api_tendencia_clube(clube_id: int):
    """
    Retorna pontos por temporada para o gráfico de linha de tendência.
    Considera apenas anos em que o clube disputou ao menos 5 jogos.
    """
    pv_case = """
        CASE WHEN CAST(ed.ano AS INTEGER) <= 1994
             THEN 2 ELSE 3 END
    """
    rows = q(f"""
        WITH m AS (
            SELECT ed.ano,
                   CASE WHEN p.mandante_placar > p.visitante_placar THEN {pv_case}
                        WHEN p.mandante_placar = p.visitante_placar THEN 1
                        ELSE 0 END AS pts,
                   1 AS jogo
            FROM partidas p
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE p.mandante_id = ?
              AND p.mandante_placar IS NOT NULL
        ),
        a AS (
            SELECT ed.ano,
                   CASE WHEN p.visitante_placar > p.mandante_placar THEN {pv_case}
                        WHEN p.visitante_placar = p.mandante_placar THEN 1
                        ELSE 0 END AS pts,
                   1 AS jogo
            FROM partidas p
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE p.visitante_id = ?
              AND p.visitante_placar IS NOT NULL
        ),
        tudo AS (SELECT * FROM m UNION ALL SELECT * FROM a)
        SELECT ano, SUM(pts) AS pontos, SUM(jogo) AS jogos
        FROM tudo
        GROUP BY ano
        HAVING jogos >= 5
        ORDER BY ano
    """, (clube_id, clube_id))
    return jsonify(rows)


@app.route("/api/radar_jogador/<int:jogador_id>/<int:ano>")
def api_radar_jogador(jogador_id: int, ano: int):
    """
    Retorna métricas normalizadas (0–100) para radar chart do jogador.
    Baseado em eventos_partida, que é o que temos nos CSVs.
    """
    stats = q1("""
        SELECT
            COUNT(CASE WHEN ep.tipo_evento='Gol'
                       AND COALESCE(ep.tipo_gol,'')!='Gol Contra'
                  THEN 1 END) AS gols,
            COUNT(CASE WHEN ep.tipo_evento='Assistência'  THEN 1 END) AS assists,
            COUNT(CASE WHEN ep.tipo_evento='Cartão Amarelo' THEN 1 END) AS amarelos,
            COUNT(CASE WHEN ep.tipo_evento='Cartão Vermelho' THEN 1 END) AS vermelhos,
            COUNT(DISTINCT ep.partida_id) AS partidas
        FROM eventos_partida ep
        JOIN partidas p ON p.id = ep.partida_id
        JOIN edicoes ed ON ed.id = p.edicao_id
        WHERE ep.jogador_id = ? AND ed.ano = ?
    """, (jogador_id, ano))

    if not stats or not stats["partidas"]:
        return jsonify({"erro": "Sem dados para esse jogador/ano"})

    # Métricas por jogo, depois normaliza pelo máximo da temporada
    partidas = stats["partidas"] or 1
    gols_pg    = round(stats["gols"] / partidas, 3)
    assists_pg = round(stats["assists"] / partidas, 3)
    amarelos_pg = round(stats["amarelos"] / partidas, 3)

    # Máximos da temporada para normalização
    max_stat = q1("""
        SELECT
            MAX(gols_pg)    AS max_gols,
            MAX(assists_pg) AS max_assists
        FROM (
            SELECT ep.jogador_id,
                   COUNT(CASE WHEN ep.tipo_evento='Gol'
                              AND COALESCE(ep.tipo_gol,'')!='Gol Contra'
                         THEN 1 END) * 1.0 / COUNT(DISTINCT ep.partida_id) AS gols_pg,
                   COUNT(CASE WHEN ep.tipo_evento='Assistência' THEN 1 END) * 1.0
                        / COUNT(DISTINCT ep.partida_id) AS assists_pg
            FROM eventos_partida ep
            JOIN partidas p ON p.id = ep.partida_id
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE ed.ano = ?
            GROUP BY ep.jogador_id
        )
    """, (ano,))

    def norm(val, maximo):
        return round(val / maximo * 100, 1) if maximo else 0

    return jsonify({
        "jogador_id": jogador_id,
        "ano": ano,
        "labels": ["Gols/Jogo", "Assists/Jogo", "Disc. (inv.)", "Participação"],
        "values": [
            norm(gols_pg, max_stat["max_gols"] or 1),
            norm(assists_pg, max_stat["max_assists"] or 1),
            max(0, 100 - amarelos_pg * 50),   # penalidade por cartões
            min(100, round(partidas * 3, 1))   # presença: 33 jogos = 100
        ]
    })


@app.route("/api/resumo_temporada/<int:ano>")
def api_resumo_temporada(ano: int):
    """Dados para o gráfico de barras da página de temporada."""
    top_gols = q("""
        SELECT c.clube,
               COUNT(*) AS total_gols
        FROM eventos_partida ep
        JOIN partidas p ON p.id = ep.partida_id
        JOIN edicoes ed ON ed.id = p.edicao_id
        JOIN clubes c   ON c.id = ep.clube_id
        WHERE ed.ano = ?
          AND ep.tipo_evento = 'Gol'
          AND COALESCE(ep.tipo_gol,'') != 'Gol Contra'
        GROUP BY c.clube
        ORDER BY total_gols DESC
        LIMIT 10
    """, (ano,))

    return jsonify({
        "top_gols": top_gols,
        "tabela": classificacao(ano)[:10],
    })


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN — CRUD
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/admin")
def admin():
    """Dashboard admin: lista jogadores e partidas recentes para edição."""
    busca = request.args.get("q", "").strip()
    entidade = request.args.get("entidade", "jogador")

    resultados = []
    if entidade == "jogador":
        sql = """
            SELECT id, nome, apelido, posicao, nascimento, nacionalidade, aposentado
            FROM jogadores
            WHERE nome LIKE ? OR apelido LIKE ?
            ORDER BY nome LIMIT 50
        """
        resultados = q(sql, (f"%{busca}%", f"%{busca}%"))
    elif entidade == "partida":
        sql = """
            SELECT p.id, p.data, p.fase, p.rodada,
                   cm.clube AS mandante, p.mandante_placar,
                   cv.clube AS visitante, p.visitante_placar,
                   ed.ano
            FROM partidas p
            JOIN clubes cm ON cm.id = p.mandante_id
            JOIN clubes cv ON cv.id = p.visitante_id
            JOIN edicoes ed ON ed.id = p.edicao_id
            WHERE cm.clube LIKE ? OR cv.clube LIKE ?
               OR CAST(p.id AS TEXT) LIKE ?
            ORDER BY p.data DESC LIMIT 50
        """
        resultados = q(sql, (f"%{busca}%", f"%{busca}%", f"%{busca}%"))

    return render_template("admin.html",
                           busca=busca,
                           entidade=entidade,
                           resultados=resultados)


@app.route("/admin/jogador/<int:jogador_id>", methods=["GET", "POST"])
def admin_jogador(jogador_id: int):
    db = get_db()
    info = q1("SELECT * FROM jogadores WHERE id = ?", (jogador_id,))
    if not info:
        return render_template("erro.html", msg="Jogador não encontrado."), 404

    if request.method == "POST":
        campos_editaveis = ["apelido", "posicao", "pe_preferido", "altura",
                            "peso", "nacionalidade", "naturalidade", "aposentado"]
        updates = {c: request.form.get(c, "").strip() or None
                   for c in campos_editaveis}

        # Converte tipos
        for col in ["altura", "peso", "aposentado"]:
            try:
                updates[col] = int(updates[col]) if updates[col] is not None else None
            except (ValueError, TypeError):
                updates[col] = None

        set_clause = ", ".join(f"{c} = ?" for c in updates)
        db.execute(
            f"UPDATE jogadores SET {set_clause} WHERE id = ?",
            (*updates.values(), jogador_id)
        )
        db.commit()
        flash(f"Jogador #{jogador_id} atualizado com sucesso.", "success")
        return redirect(url_for("admin_jogador", jogador_id=jogador_id))

    return render_template("admin_jogador.html", jogador=info)


@app.route("/admin/partida/<int:partida_id>", methods=["GET", "POST"])
def admin_partida(partida_id: int):
    db = get_db()
    info = q1("""
        SELECT p.*,
               cm.clube AS mandante, cv.clube AS visitante, ed.ano
        FROM partidas p
        JOIN clubes cm ON cm.id = p.mandante_id
        JOIN clubes cv ON cv.id = p.visitante_id
        JOIN edicoes ed ON ed.id = p.edicao_id
        WHERE p.id = ?
    """, (partida_id,))

    if not info:
        return render_template("erro.html", msg="Partida não encontrada."), 404

    if request.method == "POST":
        campos = ["data", "hora", "fase", "rodada", "estadio_id",
                  "mandante_placar", "visitante_placar",
                  "mandante_penalti", "visitante_penalti",
                  "prorrogacao", "publico"]
        updates = {}
        for c in campos:
            val = request.form.get(c, "").strip() or None
            if c in ["rodada", "estadio_id", "mandante_placar", "visitante_placar",
                     "mandante_penalti", "visitante_penalti", "prorrogacao", "publico"]:
                try:
                    val = int(val) if val is not None else None
                except (ValueError, TypeError):
                    val = None
            updates[c] = val

        set_clause = ", ".join(f"{c} = ?" for c in updates)
        db.execute(
            f"UPDATE partidas SET {set_clause} WHERE id = ?",
            (*updates.values(), partida_id)
        )
        db.commit()
        flash(f"Partida #{partida_id} atualizada.", "success")
        return redirect(url_for("admin_partida", partida_id=partida_id))

    estadios_list = q("SELECT id, estadio FROM estadios ORDER BY estadio")
    return render_template("admin_partida.html",
                           partida=info,
                           estadios=estadios_list)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(debug=True, port=5000)
