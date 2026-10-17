"""
API do modelo de degradação de pneu — F1 Medallion, parte 2 (Pessoa C).

Contrato v1:
  POST /predict  {"instancias": [{...}, ...]}
              -> {"modelo": "f1-degradacao-pneu@v1", "predicoes": [{"tempo_previsto_s": float}, ...]}
  GET  /health  -> {"status": "ok", "modelo": "..."}

O modelo é carregado UMA vez, no startup do container (não a cada requisição).
"""
import logging
import os
import time
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from preditor import criar_preditor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

MODELO = os.getenv("MODELO_NOME", "f1-degradacao-pneu@v1")
GAP_SEM_CARRO_A_FRENTE = 99.0   # mesmo valor usado no treino
MAX_INSTANCIAS = 500

Composto = Literal["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN", "TEST_UNKNOWN"]
TipoCircuito = Literal["Permanent", "Temporary - Street", "Temporary - Road"]


class Instancia(BaseModel):
    """Uma volta. Campos e tipos conforme o contrato v1."""
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    composto: Composto
    idade_pneu: int = Field(..., ge=0)
    volta_no_stint: int = Field(..., ge=1)
    volta_corrida: int = Field(..., ge=1, description="lap_number do evento")
    gap_medio_a_frente: Optional[float] = Field(None, description="nulo vira 99.0")
    temp_pista: float
    temp_ar: Optional[float] = None
    umidade: Optional[float] = None
    circuito: str
    circuit_type: TipoCircuito

    def para_modelo(self) -> dict:
        d = self.model_dump()
        if d["gap_medio_a_frente"] is None:
            d["gap_medio_a_frente"] = GAP_SEM_CARRO_A_FRENTE
        return d


class Requisicao(BaseModel):
    instancias: list[Instancia] = Field(..., min_length=1, max_length=MAX_INSTANCIAS)


class Predicao(BaseModel):
    tempo_previsto_s: float


class Resposta(BaseModel):
    modelo: str
    predicoes: list[Predicao]


t0 = time.perf_counter()
preditor = criar_preditor()
log.info("modelo %s pronto via %s em %.1fs", MODELO, preditor.nome, time.perf_counter() - t0)

app = FastAPI(title="F1 — degradação de pneu", version=MODELO)


@app.middleware("http")
async def medir_latencia(request: Request, call_next):
    inicio = time.perf_counter()
    resp = await call_next(request)
    resp.headers["X-Latencia-ms"] = f"{(time.perf_counter() - inicio) * 1000:.1f}"
    return resp


@app.get("/health")
def health():
    return {"status": "ok", "modelo": MODELO, "backend": preditor.nome}


@app.post("/predict", response_model=Resposta)
def predict(req: Requisicao):
    inicio = time.perf_counter()
    entradas = [i.para_modelo() for i in req.instancias]
    try:
        valores = preditor.predizer(entradas)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        log.exception("falha na predição")
        raise HTTPException(status_code=500, detail=f"erro na predição: {type(e).__name__}")
    ms = (time.perf_counter() - inicio) * 1000
    # log estruturado (Cloud Logging): base para monitorar drift na parte 3
    log.info({"evento": "predicao", "modelo": MODELO, "n": len(entradas),
              "latencia_ms": round(ms, 1), "entradas": entradas[:5], "saidas": valores[:5]})
    return {"modelo": MODELO, "predicoes": [{"tempo_previsto_s": round(v, 3)} for v in valores]}
