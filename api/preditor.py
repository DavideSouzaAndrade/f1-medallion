"""
Preditores do modelo de degradação de pneu (BigQuery ML).

  TFPreditor   -> carrega o SavedModel gerado pelo EXPORT MODEL (gs:// ou pasta local)
                  UMA vez e prediz em memória (baixa latência).
  BQMLPreditor -> plano B: ML.PREDICT no BigQuery (1-2 s, sem artefato).

Os dois recebem uma lista de dicts (instâncias do contrato v1) e devolvem
uma lista de floats na mesma ordem.
"""
import logging
import os
import shutil
import tempfile

log = logging.getLogger("preditor")


# ------------------------------------------------------------------ TensorFlow
def _baixar_gcs(uri: str) -> str:
    """Copia gs://bucket/pasta/ para uma pasta local e devolve o caminho."""
    from google.cloud import storage
    bucket_nome, prefixo = uri[5:].split("/", 1)
    prefixo = prefixo.rstrip("/") + "/"
    destino = tempfile.mkdtemp(prefix="modelo_")
    cliente = storage.Client()
    n = 0
    for blob in cliente.list_blobs(bucket_nome, prefix=prefixo):
        if blob.name.endswith("/"):
            continue
        local = os.path.join(destino, blob.name[len(prefixo):])
        os.makedirs(os.path.dirname(local), exist_ok=True)
        blob.download_to_filename(local)
        n += 1
    if n == 0:
        raise FileNotFoundError(f"nada encontrado em {uri}")
    log.info("baixados %d arquivos de %s", n, uri)
    return destino


class TFPreditor:
    nome = "tensorflow"

    def __init__(self, uri: str):
        pasta = _baixar_gcs(uri) if uri.startswith("gs://") else uri
        if os.path.exists(os.path.join(pasta, "model.bst")):
            raise RuntimeError("artefato é XGBoost (boosted tree); use o backend bqml")
        if not os.path.exists(os.path.join(pasta, "saved_model.pb")):
            raise FileNotFoundError(f"saved_model.pb não encontrado em {pasta}")

        import tensorflow as tf
        self.tf = tf
        self.modelo = tf.saved_model.load(pasta)
        self.sig = self.modelo.signatures["serving_default"]
        self.entradas = self.sig.structured_input_signature[1]  # nome -> TensorSpec

        # Modelos com TRANSFORM exportam o pré-processamento numa subpasta transform/
        self.transform = None
        pasta_t = os.path.join(pasta, "transform")
        if os.path.exists(os.path.join(pasta_t, "saved_model.pb")):
            self.transform = tf.saved_model.load(pasta_t).signatures["serving_default"]
            self.entradas_t = self.transform.structured_input_signature[1]
            log.info("TRANSFORM detectado; entradas brutas: %s", sorted(self.entradas_t))
        log.info("SavedModel carregado; entradas: %s", sorted(self.entradas))
        if pasta != uri:
            shutil.rmtree(pasta, ignore_errors=True)

    def _tensores(self, specs, instancias):
        tf = self.tf
        out = {}
        for nome, spec in specs.items():
            faltando = [i for i, inst in enumerate(instancias) if nome not in inst]
            if faltando:
                raise ValueError(f"o modelo espera a feature '{nome}', ausente na requisição")
            vals = []
            for inst in instancias:
                v = inst[nome]
                if spec.dtype == tf.string:
                    vals.append("" if v is None else str(v))
                elif spec.dtype.is_floating:
                    vals.append(float("nan") if v is None else float(v))
                elif spec.dtype == tf.bool:
                    vals.append(bool(v))
                else:
                    vals.append(0 if v is None else int(v))
            t = tf.constant(vals, dtype=spec.dtype)
            if spec.shape.rank == 2:
                t = tf.reshape(t, [-1, 1])
            out[nome] = t
        return out

    @staticmethod
    def _saida(resultado: dict):
        chaves = [k for k in resultado if k.startswith("predicted_")] or list(resultado)
        return resultado[chaves[0]].numpy().reshape(-1)

    def predizer(self, instancias: list[dict]) -> list[float]:
        if self.transform is None:
            res = self.sig(**self._tensores(self.entradas, instancias))
        else:
            # pré-processamento (TRANSFORM) -> modelo, passando tensores direto
            t_out = self.transform(**self._tensores(self.entradas_t, instancias))
            tf = self.tf
            entrada = {}
            for nome, spec in self.entradas.items():
                t = tf.cast(t_out[nome], spec.dtype)
                entrada[nome] = tf.reshape(t, [-1, 1] if spec.shape.rank == 2 else [-1])
            res = self.sig(**entrada)
        return [float(x) for x in self._saida(res)]


# ------------------------------------------------------------------ BigQuery ML
class BQMLPreditor:
    nome = "bqml"

    def __init__(self, modelo: str, projeto: str | None = None):
        from google.cloud import bigquery
        if not modelo:
            raise RuntimeError("defina BQ_MODEL=projeto.dataset.modelo")
        self.bq = bigquery
        self.cliente = bigquery.Client(project=projeto)
        self.modelo = modelo

    @staticmethod
    def _tipo(v):
        if isinstance(v, bool):
            return "BOOL"
        if isinstance(v, int):
            return "INT64"
        if isinstance(v, float):
            return "FLOAT64"
        return "STRING"

    def montar_sql(self, instancias: list[dict]):
        """Uma única consulta para o lote inteiro; _idx garante a ordem."""
        selects, params = [], []
        for i, inst in enumerate(instancias):
            cols = [f"{i} AS _idx"]
            for k, v in inst.items():
                p = f"p{i}_{k}"
                if v is None:
                    cols.append(f"CAST(NULL AS FLOAT64) AS {k}")  # só floats são anuláveis no contrato
                else:
                    cols.append(f"@{p} AS {k}")
                    params.append(self.bq.ScalarQueryParameter(p, self._tipo(v), v))
            selects.append("SELECT " + ", ".join(cols))
        sql = (f"SELECT * FROM ML.PREDICT(MODEL `{self.modelo}`, ("
               + " UNION ALL ".join(selects) + ")) ORDER BY _idx")
        return sql, params

    def predizer(self, instancias: list[dict]) -> list[float]:
        sql, params = self.montar_sql(instancias)
        job = self.cliente.query(sql, job_config=self.bq.QueryJobConfig(query_parameters=params))
        linhas = list(job.result())
        col = next(c for c in linhas[0].keys() if c.startswith("predicted_") and not c.endswith("_probs"))
        return [float(r[col]) for r in linhas]


# ------------------------------------------------------------------ fábrica
def criar_preditor():
    """BACKEND=auto tenta o SavedModel e, se falhar, cai para ML.PREDICT (plano B)."""
    backend = os.getenv("BACKEND", "auto").lower()
    uri = os.getenv("MODEL_URI", "")
    bq_model = os.getenv("BQ_MODEL", "")
    projeto = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")

    if backend in ("auto", "tensorflow"):
        try:
            return TFPreditor(uri)
        except Exception as e:
            if backend == "tensorflow" or not bq_model:
                raise
            log.warning("SavedModel indisponível (%s); usando ML.PREDICT", e)
    return BQMLPreditor(bq_model, projeto)
