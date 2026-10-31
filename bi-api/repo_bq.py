"""Consultas ao BigQuery. É o único módulo que conhece tabelas e SQL; o resto da API fala com ele.

Todas as consultas são parametrizadas (nada de f-string com valor vindo da URL) e têm teto de
bytes lidos. O repositório mock (repo_mock.py) implementa a mesma interface.
"""
import logging
import time
from datetime import datetime
from functools import wraps
from typing import Optional

from google.cloud import bigquery

import config as cfg

log = logging.getLogger("bi.bq")


def _cache(metodo):
    """Cache em memória com TTL, só para o histórico. O stream nunca é cacheado."""
    guardado: dict = {}

    @wraps(metodo)
    def envelope(self, *args):
        agora = time.monotonic()
        achado = guardado.get(args)
        if achado and agora - achado[0] < cfg.CACHE_S:
            return achado[1]
        valor = metodo(self, *args)
        guardado[args] = (agora, valor)
        return valor

    return envelope


def _tabela(nome: str) -> str:
    return f"`{cfg.PROJECT}.{nome}`"


def _p(nome: str, tipo: str, valor):
    return bigquery.ScalarQueryParameter(nome, tipo, valor)


# Pega a primeira gravação de cada evento: o Pub/Sub entrega pelo menos uma vez, então o
# Dataflow pode gravar duas. É a mesma regra da vw_voltas_unificada.
def _stream_dedup(where: str) -> str:
    return f"""
      SELECT * FROM {_tabela(cfg.T_STREAM)}
      WHERE {where}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY evento_id ORDER BY _ts_processamento) = 1"""


class RepoBigQuery:
    nome = "bigquery"

    def __init__(self):
        self.client = bigquery.Client(project=cfg.PROJECT, location=cfg.LOCATION)

    def _rodar(self, sql: str, params: Optional[list] = None) -> list[dict]:
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=params or [], maximum_bytes_billed=cfg.BQ_MAX_BYTES))
        return [dict(r) for r in job.result()]

    def ping(self) -> None:
        self._rodar("SELECT 1")

    # ---------- histórico (batch) ----------

    @_cache
    def sessoes(self, year: Optional[int]) -> list[dict]:
        return self._rodar(f"""
            SELECT session_key, year, session_name, nome_evento, circuito, circuit_type, pais,
                   eh_corrida, data_inicio_agendada
            FROM {_tabela(cfg.T_SESSOES)}
            WHERE tem_dado AND NOT eh_teste AND (@year IS NULL OR year = @year)
            ORDER BY data_inicio_agendada DESC""", [_p("year", "INT64", year)])

    @_cache
    def pilotos(self, session_key: int) -> list[dict]:
        return self._rodar(f"""
            SELECT driver_number, sigla, nome_completo, equipe
            FROM {_tabela(cfg.T_PILOTOS)}
            WHERE session_key = @session_key
            ORDER BY driver_number""", [_p("session_key", "INT64", session_key)])

    @_cache
    def voltas(self, session_key: int, driver_number: Optional[int], origem: Optional[str]) -> list[dict]:
        return self._rodar(f"""
            SELECT origem, session_key, driver_number, lap_number, duracao_s,
                   setor_1_s, setor_2_s, setor_3_s, composto, stint_number, idade_pneu,
                   eh_saida_box, eh_entrada_box, gap_medio_a_frente, temp_pista
            FROM {_tabela(cfg.T_UNIFICADA)}
            WHERE session_key = @session_key
              AND (@driver_number IS NULL OR driver_number = @driver_number)
              AND (@origem IS NULL OR origem = @origem)
            ORDER BY driver_number, lap_number, origem
            LIMIT 6000""", [_p("session_key", "INT64", session_key),
                            _p("driver_number", "INT64", driver_number),
                            _p("origem", "STRING", origem)])

    @_cache
    def degradacao(self, session_key: Optional[int], circuito: Optional[str]) -> list[dict]:
        return self._rodar(f"""
            SELECT composto, idade_pneu, ROUND(AVG(alvo_duracao_s), 3) AS tempo_medio_s,
                   COUNT(*) AS voltas
            FROM {_tabela(cfg.T_GOLD_PNEU)}
            WHERE (@session_key IS NULL OR session_key = @session_key)
              AND (@circuito IS NULL OR circuito = @circuito)
            GROUP BY composto, idade_pneu
            HAVING COUNT(*) >= 3
            ORDER BY composto, idade_pneu""", [_p("session_key", "INT64", session_key),
                                               _p("circuito", "STRING", circuito)])

    # ---------- batch × stream ----------

    @_cache
    def comparativo_origem(self) -> list[dict]:
        return self._rodar(f"""
            SELECT origem, COUNT(*) AS voltas, COUNT(DISTINCT session_key) AS sessoes,
                   MIN(data_inicio) AS primeira_volta, MAX(data_inicio) AS ultima_volta
            FROM {_tabela(cfg.T_UNIFICADA)}
            GROUP BY origem ORDER BY origem""")

    # ---------- stream ----------

    def stream_resumo(self) -> dict:
        agg = self._rodar(f"""
            WITH v AS ({_stream_dedup("TRUE")})
            SELECT COUNT(*) AS total_eventos, MAX(ts_evento) AS ultimo_evento,
                   COUNTIF(apto_modelo) AS aptos_modelo,
                   COUNTIF(tempo_previsto_s IS NOT NULL) AS com_predicao,
                   -- Só as últimas 24 h: uma fila acumulada de testes antigos (job parado) daria
                   -- latência de dias e esconderia a do pipeline em operação.
                   APPROX_QUANTILES(IF(ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR),
                                       latencia_ms, NULL), 100)[SAFE_OFFSET(50)] AS latencia_p50_ms,
                   APPROX_QUANTILES(IF(ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR),
                                       latencia_ms, NULL), 100)[SAFE_OFFSET(95)] AS latencia_p95_ms
            FROM v""")[0]
        dlq = self._rodar(f"""
            SELECT motivo, COUNT(*) AS eventos FROM {_tabela(cfg.T_DLQ)}
            GROUP BY motivo ORDER BY eventos DESC""")
        por_minuto = self._rodar(f"""
            WITH v AS ({_stream_dedup("ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)")})
            SELECT TIMESTAMP_TRUNC(ts_evento, MINUTE) AS minuto, COUNT(*) AS eventos
            FROM v GROUP BY minuto ORDER BY minuto""")
        return {**agg, "dlq_total": sum(d["eventos"] for d in dlq),
                "dlq_por_motivo": dlq, "por_minuto": por_minuto}

    def replays(self) -> list[dict]:
        return self._rodar(f"""
            WITH v AS ({_stream_dedup("TRUE")}),
            totais AS (
              SELECT session_key, COUNT(*) AS voltas_na_corrida
              FROM {_tabela(cfg.T_SILVER_VOLTAS)}
              WHERE session_key IN (SELECT DISTINCT session_key FROM v)
              GROUP BY session_key)
            SELECT v.replay_id, v.session_key, ANY_VALUE(v.circuito) AS circuito,
                   COUNT(*) AS eventos, MAX(v.ts_evento) AS ultimo_evento,
                   ANY_VALUE(t.voltas_na_corrida) AS voltas_na_corrida
            FROM v LEFT JOIN totais t USING (session_key)
            GROUP BY v.replay_id, v.session_key
            ORDER BY ultimo_evento DESC LIMIT 20""")

    def eventos_stream(self, desde: Optional[datetime], limite: int) -> list[dict]:
        """Eventos em ordem crescente de processamento.

        Com `desde`: os processados a partir dele (inclusive: o chamador descarta repetidos
        por evento_id). Sem `desde`: os `limite` mais recentes das últimas 24 h.
        O filtro de ts_evento existe só para podar partições; o corte real é _ts_processamento.
        """
        if desde:
            poda = "ts_evento >= TIMESTAMP_SUB(@desde, INTERVAL 1 HOUR)"
            filtro, ordem = "AND v._ts_processamento >= @desde", "ASC"
        else:
            poda = "ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
            filtro, ordem = "", "DESC"
        linhas = self._rodar(f"""
            WITH v AS ({_stream_dedup(poda)})
            SELECT v.evento_id, v.replay_id, v.ts_evento, v._ts_processamento AS ts_processamento,
                   v.session_key, v.driver_number,
                   COALESCE(p.sigla, CAST(v.driver_number AS STRING)) AS piloto,
                   v.lap_number, v.year, v.circuito, v.composto, v.idade_pneu, v.duracao_s,
                   v.setor_1_s, v.setor_2_s, v.setor_3_s, v.eh_saida_box, v.eh_entrada_box,
                   v.apto_modelo, v.latencia_ms, v.tempo_previsto_s, v.modelo_versao
            FROM v
            LEFT JOIN {_tabela(cfg.T_PILOTOS)} p
              ON p.session_key = v.session_key AND p.driver_number = v.driver_number
            WHERE TRUE {filtro}
            ORDER BY v._ts_processamento {ordem}
            LIMIT @limite""", [_p("desde", "TIMESTAMP", desde), _p("limite", "INT64", limite)])
        return linhas if desde else linhas[::-1]

    def previsto_real(self, session_key: Optional[int], replay_id: Optional[str], limite: int) -> dict:
        filtro = ("(@session_key IS NULL OR session_key = @session_key) "
                  "AND (@replay_id IS NULL OR replay_id = @replay_id)")
        params = [_p("session_key", "INT64", session_key), _p("replay_id", "STRING", replay_id),
                  _p("limite", "INT64", limite)]
        metricas = self._rodar(f"""
            WITH v AS ({_stream_dedup(filtro)})
            SELECT COUNTIF(tempo_previsto_s IS NOT NULL AND duracao_s IS NOT NULL) AS voltas,
                   AVG(IF(duracao_s IS NOT NULL, ABS(tempo_previsto_s - duracao_s), NULL)) AS mae_s,
                   AVG(IF(duracao_s IS NOT NULL, tempo_previsto_s - duracao_s, NULL)) AS vies_s,
                   COUNTIF(apto_modelo AND tempo_previsto_s IS NULL) AS aguardando_predicao
            FROM v""", params)[0]
        pares = self._rodar(f"""
            WITH v AS ({_stream_dedup(filtro)})
            SELECT v.evento_id, v.replay_id, v.session_key, v.driver_number,
                   COALESCE(p.sigla, CAST(v.driver_number AS STRING)) AS piloto,
                   v.lap_number, v.composto, v.duracao_s, v.tempo_previsto_s,
                   ROUND(v.tempo_previsto_s - v.duracao_s, 3) AS erro_s, v.modelo_versao
            FROM v
            LEFT JOIN {_tabela(cfg.T_PILOTOS)} p
              ON p.session_key = v.session_key AND p.driver_number = v.driver_number
            WHERE v.tempo_previsto_s IS NOT NULL AND v.duracao_s IS NOT NULL
            ORDER BY v.ts_evento DESC
            LIMIT @limite""", params)
        return {"metricas": {k: metricas[k] for k in ("voltas", "mae_s", "vies_s")},
                "pares": pares[::-1], "aguardando_predicao": metricas["aguardando_predicao"]}
