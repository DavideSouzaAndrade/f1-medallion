"""Valida todas as consultas da API contra o BigQuery real, sem executá-las (dry run).

O BigQuery compila cada SQL e confere tabelas, colunas e tipos; não lê dados e não cobra.
Mostra também quantos bytes cada consulta leria.

    gcloud auth application-default login
    python validar_sql.py
"""
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

import config as cfg
from repo_bq import RepoBigQuery


class RepoDryRun(RepoBigQuery):
    def _rodar(self, sql, params=None):
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=params or [], dry_run=True, use_query_cache=False))
        self.ultimo_custo = job.total_bytes_processed
        return [{}]   # sem dados reais: o código do repositório que indexa o resultado vai levantar KeyError


# Cada caso dispara as consultas de um método do repositório.
CASOS = [
    ("sessoes", lambda r: r.sessoes(None)),
    ("pilotos", lambda r: r.pilotos(11361)),
    ("voltas", lambda r: r.voltas(11361, None, None)),
    ("degradacao", lambda r: r.degradacao(None, None)),
    ("comparativo_origem", lambda r: r.comparativo_origem()),
    ("stream_resumo", lambda r: r.stream_resumo()),
    ("replays", lambda r: r.replays()),
    ("eventos_stream (inicial)", lambda r: r.eventos_stream(None, 100)),
    ("eventos_stream (com cursor)", lambda r: r.eventos_stream(datetime.now(timezone.utc), 100)),
    ("previsto_real", lambda r: r.previsto_real(None, None, 100)),
]


def main() -> int:
    repo = RepoDryRun.__new__(RepoDryRun)
    repo.client = bigquery.Client(project=cfg.PROJECT, location=cfg.LOCATION)
    falhas = 0
    for nome, chamar in CASOS:
        repo.ultimo_custo = None
        try:
            chamar(repo)
        except (KeyError, IndexError, TypeError):
            pass      # esperado: o dry run devolve uma linha vazia; o que importa é a consulta ter compilado
        except Exception as e:
            falhas += 1
            print(f"FALHOU  {nome}: {e}")
            continue
        mb = (repo.ultimo_custo or 0) / 1e6
        print(f"ok      {nome:28s} leria {mb:10.1f} MB")
    print("\nTodas as consultas compilam." if not falhas else f"\n{falhas} consulta(s) com erro.")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
