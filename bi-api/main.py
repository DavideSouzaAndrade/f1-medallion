"""
API de BI — F1 Medallion, front-end analítico (Davi).

Expõe ao front o que está no BigQuery (Silver/Gold e o stream do Dataflow). O navegador só
fala com esta API: nunca com o BigQuery, e sem credencial da GCP.

  GET /api/health                     saúde da API e do BigQuery (503 se o BigQuery não responde)
  GET /api/pipeline/status            checagens do pipeline para o Airflow (503 se alguma falha)
  GET /api/sessoes                    corridas com dados (histórico)
  GET /api/sessoes/{key}/pilotos      pilotos da corrida
  GET /api/voltas                     voltas de uma corrida, batch e stream, coluna `origem`
  GET /api/degradacao                 tempo médio por idade do pneu e composto (Gold)
  GET /api/comparativo/origem         volume batch × stream na view unificada
  GET /api/stream/resumo              saúde do pipeline: volume, latência, DLQ, vazão
  GET /api/stream/replays             execuções do gerador vistas no stream
  GET /api/stream/eventos             eventos ao vivo por polling (alternativa ao SSE)
  GET /api/stream/sse                 eventos ao vivo por Server-Sent Events
  GET /api/previsto-real              previsão × real, casados pela chave da volta
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from google.api_core.exceptions import GoogleAPICallError

import config as cfg
from schemas import (Checagem, EventoStream, Origem, Piloto, PontoDegradacao, PrevistoReal, Replay,
                     ResumoOrigem, ResumoStream, Sessao, StatusPipeline, Volta)
from stream import Difusor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("bi.api")

BATIMENTO_S = 15   # comentário SSE periódico: mantém a conexão viva atrás de proxies


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    if cfg.MOCK:
        from repo_mock import RepoMock
        app.state.repo = RepoMock()
    else:
        from repo_bq import RepoBigQuery
        app.state.repo = RepoBigQuery()
    app.state.difusor = Difusor(app.state.repo)
    log.info("repositório: %s (projeto %s)", app.state.repo.nome, cfg.PROJECT)
    yield


app = FastAPI(title="F1 — API de BI", version="1", lifespan=ciclo_de_vida)
app.add_middleware(CORSMiddleware, allow_origins=cfg.CORS_ORIGINS, allow_methods=["GET"], allow_headers=["*"])


@app.exception_handler(GoogleAPICallError)
async def erro_bigquery(_, exc: GoogleAPICallError):
    log.error("BigQuery: %s", exc)
    return JSONResponse(status_code=502, content={"detail": "falha ao consultar o BigQuery"})


@app.get("/")
def raiz():
    return {"servico": "f1-bi-api", "modo": "mock" if cfg.MOCK else "bigquery", "documentacao": "/docs"}


@app.get("/api/health")
def health():
    modo = "mock" if cfg.MOCK else "bigquery"
    try:
        app.state.repo.ping()
    except Exception as e:
        log.error("health: %s", e)
        raise HTTPException(status_code=503, detail=f"BigQuery indisponível ({type(e).__name__})")
    return {"status": "ok", "modo": modo}


@app.get("/api/pipeline/status", response_model=StatusPipeline)
def pipeline_status(max_idade_s: int = Query(300, ge=1, description="idade máxima do último evento do stream"),
                    max_p95_ms: int = Query(10_000, ge=1, description="latência p95 máxima (últimas 24 h)"),
                    max_dlq_pct: float = Query(30.0, ge=0, le=100, description="% máximo de rejeitados na DLQ"),
                    sempre_200: bool = Query(False, description="devolve 200 mesmo com falha (para exibir na tela)")):
    """Checagens do pipeline Pub/Sub → Dataflow → BigQuery, pensadas para uma DAG do Airflow.

    Status HTTP 200 se tudo passa, 503 se alguma checagem falha: um HttpSensor/HttpOperator marca a
    tarefa como falha sem precisar interpretar o corpo. `predicoes` é só informativa (pendente
    enquanto o Dataflow não anexa a previsão) e nunca derruba o status.
    """
    agora = datetime.now(timezone.utc)
    r = app.state.repo.stream_resumo()          # se o BigQuery cair, o handler devolve 502
    checagens: list[Checagem] = []

    ultimo = r["ultimo_evento"]
    if ultimo is None:
        checagens.append(Checagem(nome="stream_recente", estado="falha", detalhe="nenhum evento no stream"))
    else:
        idade = int((agora - ultimo).total_seconds())
        checagens.append(Checagem(nome="stream_recente", estado="ok" if idade <= max_idade_s else "falha",
                                  detalhe=f"último evento há {idade} s (limite {max_idade_s} s)"))

    p95 = r["latencia_p95_ms"]
    if p95 is None:
        checagens.append(Checagem(nome="latencia_p95", estado="falha", detalhe="sem eventos nas últimas 24 h"))
    else:
        checagens.append(Checagem(nome="latencia_p95", estado="ok" if p95 <= max_p95_ms else "falha",
                                  detalhe=f"p95 de {p95} ms (limite {max_p95_ms} ms)"))

    recebidos = r["total_eventos"] + r["dlq_total"]
    pct = 100 * r["dlq_total"] / recebidos if recebidos else 0.0
    checagens.append(Checagem(nome="dlq", estado="ok" if pct <= max_dlq_pct else "falha",
                              detalhe=f"{r['dlq_total']} rejeitados, {pct:.1f}% do que chegou (limite {max_dlq_pct:g}%)"))

    com_previsao = r["com_predicao"]
    checagens.append(Checagem(
        nome="predicoes", estado="ok" if com_previsao else "pendente",
        detalhe=f"{com_previsao} voltas com previsão" if com_previsao else "nenhuma volta com previsão ainda"))

    ok = all(c.estado != "falha" for c in checagens)
    corpo = StatusPipeline(status="ok" if ok else "falha", verificado_em=agora, checagens=checagens)
    return JSONResponse(status_code=200 if ok or sempre_200 else 503, content=jsonable_encoder(corpo))


# ---------- histórico ----------

@app.get("/api/sessoes", response_model=list[Sessao])
def sessoes(year: Optional[int] = None):
    return app.state.repo.sessoes(year)


@app.get("/api/sessoes/{session_key}/pilotos", response_model=list[Piloto])
def pilotos(session_key: int):
    return app.state.repo.pilotos(session_key)


@app.get("/api/voltas", response_model=list[Volta])
def voltas(session_key: int, driver_number: Optional[int] = None, origem: Optional[Origem] = None):
    return app.state.repo.voltas(session_key, driver_number, origem)


@app.get("/api/degradacao", response_model=list[PontoDegradacao])
def degradacao(session_key: Optional[int] = None, circuito: Optional[str] = None):
    return app.state.repo.degradacao(session_key, circuito)


@app.get("/api/comparativo/origem", response_model=list[ResumoOrigem])
def comparativo_origem():
    return app.state.repo.comparativo_origem()


# ---------- stream ----------

@app.get("/api/stream/resumo", response_model=ResumoStream)
def stream_resumo():
    return app.state.repo.stream_resumo()


@app.get("/api/stream/replays", response_model=list[Replay])
def stream_replays():
    return app.state.repo.replays()


@app.get("/api/stream/eventos", response_model=list[EventoStream])
def stream_eventos(desde: Optional[datetime] = None, limite: int = Query(100, ge=1, le=500)):
    """Sem `desde`: os mais recentes. Com `desde`: o que foi processado a partir daquele instante."""
    return app.state.repo.eventos_stream(desde, limite)


def _sse(event: str, data: str) -> str:
    return f"event: {event}\ndata: {data}\n\n"


@app.get("/api/stream/sse")
async def stream_sse():
    difusor: Difusor = app.state.difusor

    async def gerar():
        yield "retry: 3000\n\n"            # o EventSource reconecta sozinho após 3 s
        assinatura = difusor.assinar()
        proxima = asyncio.ensure_future(anext(assinatura, None))
        try:
            while True:
                pronto, _ = await asyncio.wait({proxima}, timeout=BATIMENTO_S)
                if not pronto:
                    yield ": batimento\n\n"
                    continue
                msg = proxima.result()
                if msg is None:
                    return
                yield _sse(*msg)
                proxima = asyncio.ensure_future(anext(assinatura, None))
        finally:
            proxima.cancel()
            await assinatura.aclose()

    return StreamingResponse(gerar(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/previsto-real", response_model=PrevistoReal)
def previsto_real(session_key: Optional[int] = None, replay_id: Optional[str] = None,
                  limite: int = Query(300, ge=1, le=2000)):
    return app.state.repo.previsto_real(session_key, replay_id, limite)
