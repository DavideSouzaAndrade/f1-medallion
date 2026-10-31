"""Leitura de uma corrida na Bronze, que e a fonte do replay.

A Bronze guarda o JSON de cada registro exatamente como a OpenF1 entregou
(coluna `payload`). O gerador publica esse JSON sem mexer em nenhum campo:
e isso que faz o replay ser indistinguivel do dado ao vivo.

Uma consulta por replay. As tabelas sao clusterizadas por session_key, entao
uma corrida (~23 mil registros) le poucos MB.
"""

from __future__ import annotations

import json
from typing import Any

from google.cloud import bigquery

import config

# meetings e sessions sao listas por temporada, sem a coluna session_key: o
# filtro e pelo conteudo do JSON. Os demais sao particionados por sessao.
#
# DISTINCT porque uma sessao reingerida pode ter gravado o mesmo registro
# duas vezes na Bronze; o replay nao deve publicar a duplicata como se fosse
# da origem.
_SQL_REGISTROS = """
WITH sessao AS (
  SELECT ANY_VALUE(payload) AS payload
  FROM `{projeto}.{dataset}.bronze_sessions`
  WHERE SAFE_CAST(JSON_VALUE(payload, '$.session_key') AS INT64) = @session_key
)
SELECT 'sessions' AS topico, payload FROM sessao
UNION ALL
SELECT 'meetings', ANY_VALUE(m.payload)
FROM `{projeto}.{dataset}.bronze_meetings` m, sessao s
WHERE JSON_VALUE(m.payload, '$.meeting_key') = JSON_VALUE(s.payload, '$.meeting_key')
{por_sessao}
"""

_POR_SESSAO = """UNION ALL
SELECT DISTINCT '{nome}', payload FROM `{{projeto}}.{{dataset}}.bronze_{nome}` WHERE session_key = @session_key"""

_SQL_CORRIDAS = """
WITH corridas AS (
  SELECT
    SAFE_CAST(JSON_VALUE(payload, '$.session_key') AS INT64) AS session_key,
    JSON_VALUE(payload, '$.meeting_key')                     AS meeting_key,
    JSON_VALUE(payload, '$.circuit_short_name')              AS circuito,
    DATE(SAFE_CAST(JSON_VALUE(payload, '$.date_start') AS TIMESTAMP)) AS data
  FROM `{projeto}.{dataset}.bronze_sessions`
  WHERE JSON_VALUE(payload, '$.session_name') = 'Race'
),
voltas AS (
  SELECT session_key, COUNT(*) AS voltas
  FROM `{projeto}.{dataset}.bronze_laps`
  GROUP BY 1
)
SELECT
  c.session_key,
  ANY_VALUE(JSON_VALUE(m.payload, '$.meeting_name')) AS nome_evento,
  c.circuito,
  c.data,
  v.voltas
FROM corridas c
JOIN voltas v USING (session_key)
LEFT JOIN `{projeto}.{dataset}.bronze_meetings` m
  ON JSON_VALUE(m.payload, '$.meeting_key') = c.meeting_key
GROUP BY c.session_key, c.circuito, c.data, v.voltas
ORDER BY c.data DESC
LIMIT @limite
"""


class FonteBronze:
    def __init__(self, cliente: bigquery.Client | None = None) -> None:
        self.cliente = cliente or bigquery.Client(project=config.PROJECT_ID)

    def _consultar(self, sql: str, parametros: list) -> list[dict[str, Any]]:
        sql = sql.format(projeto=config.PROJECT_ID, dataset=config.DATASET_BRONZE)
        job = self.cliente.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=parametros))
        return [dict(linha.items()) for linha in job.result()]

    def registros(self, session_key: int) -> dict[str, list[dict[str, Any]]]:
        """Os registros da corrida por topico, cada um como o JSON da OpenF1."""
        por_sessao = "\n".join(
            _POR_SESSAO.format(nome=t) for t in config.TOPICOS if t not in ("meetings", "sessions")
        )
        linhas = self._consultar(
            _SQL_REGISTROS.replace("{por_sessao}", por_sessao),
            [bigquery.ScalarQueryParameter("session_key", "INT64", session_key)],
        )
        saida: dict[str, list[dict[str, Any]]] = {t: [] for t in config.TOPICOS}
        for linha in linhas:
            if linha["payload"] is not None:
                saida[linha["topico"]].append(json.loads(linha["payload"]))
        return saida

    def corridas(self, limite: int = 10) -> list[dict[str, Any]]:
        """Corridas com voltas na Bronze, da mais recente para a mais antiga."""
        return self._consultar(
            _SQL_CORRIDAS, [bigquery.ScalarQueryParameter("limite", "INT64", limite)]
        )
