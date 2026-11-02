"""Testes offline (repositório mock): contrato das rotas e a entrega de eventos por SSE.

    pip install -r requirements-dev.txt && pytest
"""
import asyncio
import json
import os

os.environ["MOCK"] = "1"
os.environ["MOCK_TICK_S"] = "0.05"      # a corrida simulada anda rápido
os.environ["MOCK_LATENCIA_MAX_MS"] = "100"
os.environ["STREAM_POLL_S"] = "0.1"

import pytest
from fastapi.testclient import TestClient

import main
from repo_mock import RepoMock
from stream import Difusor


@pytest.fixture
def client():
    with TestClient(main.app) as c:
        yield c


def test_health_mock(client):
    r = client.get("/api/health")
    assert r.status_code == 200 and r.json() == {"status": "ok", "modo": "mock"}


def test_historico(client):
    sessoes = client.get("/api/sessoes").json()
    assert sessoes and all(s["eh_corrida"] for s in sessoes)
    key = sessoes[0]["session_key"]
    assert client.get(f"/api/sessoes/{key}/pilotos").json()
    voltas = client.get("/api/voltas", params={"session_key": key, "driver_number": 44}).json()
    assert voltas and {v["driver_number"] for v in voltas} == {44}
    assert client.get("/api/degradacao", params={"session_key": key}).json()


def test_origem_invalida_e_422(client):
    assert client.get("/api/voltas", params={"session_key": 1, "origem": "outra"}).status_code == 422


def test_stream_e_previsto_real(client):
    import time
    time.sleep(0.6)
    resumo = client.get("/api/stream/resumo").json()
    assert resumo["total_eventos"] > 0 and resumo["latencia_p50_ms"] is not None
    pr = client.get("/api/previsto-real").json()
    assert pr["metricas"]["voltas"] == len(pr["pares"]) > 0
    for p in pr["pares"]:   # previsto e real da MESMA volta: o erro é a diferença entre os dois
        assert p["erro_s"] == pytest.approx(p["tempo_previsto_s"] - p["duracao_s"], abs=1e-3)


def test_replays_trazem_o_total_de_voltas_da_corrida(client):
    import time
    time.sleep(0.4)
    replays = client.get("/api/stream/replays").json()
    assert replays and all(r["voltas_na_corrida"] >= r["eventos"] > 0 for r in replays)
    # As contagens da execução inteira, que o placar do Ao vivo usa no lugar do buffer do SSE.
    assert all(r["eventos"] >= r["aptos_modelo"] >= r["com_predicao"] >= 0 for r in replays)


def test_batch_e_stream_separados(client):
    import time
    time.sleep(0.4)
    origens = {o["origem"]: o for o in client.get("/api/comparativo/origem").json()}
    assert set(origens) == {"batch", "stream"} and origens["batch"]["voltas"] > origens["stream"]["voltas"] > 0
    stream = client.get("/api/voltas", params={"session_key": 11361, "origem": "stream"}).json()
    assert stream and {v["origem"] for v in stream} == {"stream"}


def test_sem_predicao_vira_aguardando(monkeypatch):
    import repo_mock
    monkeypatch.setattr(repo_mock, "COM_PREDICAO", False)
    repo = RepoMock()
    import time
    time.sleep(0.4)
    pr = repo.previsto_real(None, None, 100)
    assert pr["metricas"]["voltas"] == 0 and pr["metricas"]["mae_s"] is None
    assert pr["aguardando_predicao"] > 0


def test_polling_continua_do_cursor(client):
    import time
    time.sleep(0.4)
    primeira = client.get("/api/stream/eventos", params={"limite": 5}).json()
    assert len(primeira) == 5
    time.sleep(0.4)
    depois = client.get("/api/stream/eventos", params={"desde": primeira[-1]["ts_processamento"]}).json()
    assert depois and depois[0]["evento_id"] == primeira[-1]["evento_id"]   # desde é inclusivo


def test_sse_entrega_snapshot_sem_repetir_e_depois_o_novo():
    async def cenario():
        difusor = Difusor(RepoMock())
        await asyncio.sleep(0.5)                       # a corrida simulada acumula eventos
        ids, tipos = [], set()
        assinatura = difusor.assinar()
        try:
            async with asyncio.timeout(5):
                async for msg in assinatura:
                    tipos.add(msg[0])
                    if msg[0] == "volta":
                        ids.append(json.loads(msg[1])["evento_id"])
                    if len(ids) >= 15 and "ok" in tipos:
                        break
        finally:
            await assinatura.aclose()
        return ids, tipos, difusor

    ids, tipos, difusor = asyncio.run(cenario())
    assert len(ids) == len(set(ids)), "evento repetido no SSE"
    assert ids == sorted(ids)                          # ordem de processamento preservada
    assert tipos == {"volta", "ok"}


def test_sse_avisa_quando_a_consulta_falha():
    class RepoQuebrado(RepoMock):
        def eventos_stream(self, desde, limite):
            raise RuntimeError("bigquery fora")

    async def cenario():
        difusor = Difusor(RepoQuebrado())
        assinatura = difusor.assinar()
        try:
            async with asyncio.timeout(3):
                return await anext(assinatura)
        finally:
            await assinatura.aclose()

    event, data = asyncio.run(cenario())
    assert event == "erro" and "BigQuery" in json.loads(data)["mensagem"]


def test_pipeline_status_ok_com_stream_fresco(client):
    import time
    time.sleep(0.6)
    r = client.get("/api/pipeline/status")
    corpo = r.json()
    assert r.status_code == 200 and corpo["status"] == "ok"
    estados = {c["nome"]: c["estado"] for c in corpo["checagens"]}
    assert estados == {"stream_recente": "ok", "latencia_p95": "ok", "dlq": "ok", "predicoes": "ok"}


def test_pipeline_status_503_quando_stream_parado_e_200_se_pedido(client, monkeypatch):
    import time
    from datetime import datetime, timedelta, timezone
    time.sleep(0.6)
    resumo = client.app.state.repo.stream_resumo()
    resumo["ultimo_evento"] = datetime.now(timezone.utc) - timedelta(hours=2)
    monkeypatch.setattr(client.app.state.repo, "stream_resumo", lambda: resumo)
    r = client.get("/api/pipeline/status", params={"max_idade_s": 300})
    assert r.status_code == 503 and r.json()["status"] == "falha"
    assert any(c["nome"] == "stream_recente" and c["estado"] == "falha" for c in r.json()["checagens"])
    assert client.get("/api/pipeline/status", params={"sempre_200": True}).status_code == 200


def test_pipeline_status_predicoes_pendentes_nao_derrubam(client, monkeypatch):
    import time
    time.sleep(0.6)
    resumo = client.app.state.repo.stream_resumo()
    monkeypatch.setattr(client.app.state.repo, "stream_resumo", lambda: {**resumo, "com_predicao": 0})
    r = client.get("/api/pipeline/status")
    assert r.status_code == 200
    assert {c["nome"]: c["estado"] for c in r.json()["checagens"]}["predicoes"] == "pendente"
