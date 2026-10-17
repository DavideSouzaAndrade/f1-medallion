"""
Primeira hora da Pessoa C: o artefato exportado carrega e prevê o MESMO que o BigQuery?

Pega N voltas reais da tabela de features, prevê com ML.PREDICT (referência) e com o
SavedModel exportado (o que a API usa), e compara. Se bater, a API pode usar o artefato;
se não carregar, a API cai para ML.PREDICT (BACKEND=auto faz isso sozinho).

Uso:
  python model/validar_export.py --bq-model projeto.dataset.modelo_degradacao_pneu \
      --tabela projeto.dataset.tabela_features \
      --uri gs://f1-raw-pdm-davi-2026/modelos/degradacao_pneu/v1/
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "api"))
from preditor import BQMLPreditor, TFPreditor  # noqa: E402

CAMPOS = ["composto", "idade_pneu", "volta_no_stint", "volta_corrida", "gap_medio_a_frente",
          "temp_pista", "temp_ar", "umidade", "circuito", "circuit_type"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bq-model", required=True)
    ap.add_argument("--tabela", required=True)
    ap.add_argument("--uri", required=True)
    ap.add_argument("-n", type=int, default=50)
    a = ap.parse_args()

    from google.cloud import bigquery
    projeto = a.bq_model.split(".")[0]
    cli = bigquery.Client(project=projeto)
    sql = (f"SELECT {', '.join(CAMPOS)} FROM `{a.tabela}` "
           f"WHERE composto IN ('SOFT','MEDIUM','HARD') ORDER BY FARM_FINGERPRINT(CAST(volta_corrida AS STRING) || circuito) "
           f"LIMIT {a.n}")
    linhas = [dict(r) for r in cli.query(sql).result()]
    for r in linhas:  # mesma regra da API
        if r["gap_medio_a_frente"] is None:
            r["gap_medio_a_frente"] = 99.0
    print(f"{len(linhas)} voltas de {a.tabela}")

    t = time.perf_counter()
    ref = BQMLPreditor(a.bq_model, projeto).predizer(linhas)
    print(f"ML.PREDICT ........ {1000 * (time.perf_counter() - t):7.0f} ms")

    t = time.perf_counter()
    tf = TFPreditor(a.uri)
    print(f"carregar SavedModel {1000 * (time.perf_counter() - t):7.0f} ms")
    t = time.perf_counter()
    loc = tf.predizer(linhas)
    print(f"SavedModel predizer {1000 * (time.perf_counter() - t):7.0f} ms")

    dif = [abs(x - y) for x, y in zip(ref, loc)]
    print(f"\ndiferença máx = {max(dif):.6f} s | média = {sum(dif) / len(dif):.6f} s")
    for r, x, y in list(zip(linhas, ref, loc))[:5]:
        print(f"  {r['circuito']:<14} {r['composto']:<6} idade={r['idade_pneu']:>2}  bq={x:8.3f}  local={y:8.3f}")
    ok = max(dif) < 1e-3
    print("\nOK: artefato equivalente ao BigQuery" if ok else
          "\nATENÇÃO: diferença acima de 1 ms; use BACKEND=bqml até investigar")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
