"""Leitura das voltas de uma corrida na Silver, que e a fonte do replay.

Uma consulta por replay: a corrida inteira tem ~1.000 voltas e a
silver_voltas e clusterizada por session_key, entao o custo e de poucos MB.
"""

from __future__ import annotations

from typing import Any

from google.cloud import bigquery

import config

# Ordem de publicacao: a hora em que a volta TERMINOU, e nao a em que
# comecou. Um evento "volta concluida" so existe depois da bandeirada de
# cada volta, e ordenar pelo fim intercala os pilotos como na corrida real.
# Volta sem data nenhuma vai para o fim, para nao travar o relogio do replay.
#
# volta_no_stint e calculada aqui, sobre todas as voltas do stint. A Gold
# conta so as voltas limpas; a diferenca esta registrada no contrato.
#
# eh_saida_box e eh_entrada_box sao obrigatorios no contrato. Nulo na Silver
# significa que a origem nao marcou a volta, e ausencia de marca e "nao".
_SQL_VOLTAS = """
SELECT
  v.session_key,
  v.driver_number,
  v.lap_number,
  s.year,
  s.circuito,
  s.circuit_type,
  v.data_inicio,
  v.data_fim,
  v.duracao_s,
  v.setor_1_s,
  v.setor_2_s,
  v.setor_3_s,
  v.composto,
  v.stint_number,
  v.idade_pneu,
  IF(v.stint_number IS NULL, NULL,
     ROW_NUMBER() OVER (PARTITION BY v.driver_number, v.stint_number ORDER BY v.lap_number)
  ) AS volta_no_stint,
  COALESCE(v.eh_saida_box, FALSE)   AS eh_saida_box,
  COALESCE(v.eh_entrada_box, FALSE) AS eh_entrada_box,
  v.gap_medio_a_frente,
  v.temp_ar,
  v.temp_pista,
  v.umidade,
  v.chuva
FROM `{projeto}.{dataset}.silver_voltas` v
JOIN `{projeto}.{dataset}.silver_sessoes` s USING (session_key)
WHERE v.session_key = @session_key
ORDER BY
  COALESCE(v.data_fim, v.data_inicio) IS NULL,
  COALESCE(v.data_fim, v.data_inicio),
  v.driver_number
"""

_SQL_CORRIDAS = """
SELECT
  s.session_key,
  s.nome_evento,
  s.circuito,
  DATE(s.data_inicio_agendada) AS data,
  COUNT(v.lap_number) AS voltas
FROM `{projeto}.{dataset}.silver_sessoes` s
JOIN `{projeto}.{dataset}.silver_voltas` v USING (session_key)
WHERE s.session_name = 'Race' AND s.tem_dado
GROUP BY 1, 2, 3, 4
ORDER BY data DESC
LIMIT @limite
"""


class FonteSilver:
    def __init__(self, cliente: bigquery.Client | None = None) -> None:
        self.cliente = cliente or bigquery.Client(project=config.PROJECT_ID)

    def _consultar(self, sql: str, parametros: list) -> list[dict[str, Any]]:
        sql = sql.format(projeto=config.PROJECT_ID, dataset=config.DATASET_SILVER)
        job = self.cliente.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=parametros))
        return [dict(linha.items()) for linha in job.result()]

    def voltas(self, session_key: int) -> list[dict[str, Any]]:
        """Todas as voltas da sessao, na ordem em que terminaram."""
        return self._consultar(
            _SQL_VOLTAS, [bigquery.ScalarQueryParameter("session_key", "INT64", session_key)]
        )

    def corridas(self, limite: int = 10) -> list[dict[str, Any]]:
        """Corridas com dado na Silver, da mais recente para a mais antiga."""
        return self._consultar(
            _SQL_CORRIDAS, [bigquery.ScalarQueryParameter("limite", "INT64", limite)]
        )
