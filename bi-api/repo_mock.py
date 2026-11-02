"""Dados SIMULADOS com a mesma interface do repo_bq.py (MOCK=1).

Serve para desenvolver o front sem GCP e como plano B de rede na demonstração. Pilotos,
sessões e tempos aqui são inventados; a API avisa isso em /api/health (modo = "mock") e o
front mostra uma faixa na tela. Nada daqui vai para o BigQuery.

O "stream" simulado reproduz uma corrida em loop: uma volta de um piloto a cada TICK_S
segundos, com o atraso de processamento de ~2 s que o Dataflow real apresenta.
"""
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

# Mesmo limiar do config.py da API, relido aqui para o mock nao depender
# do modulo de configuracao (ele roda isolado nos testes).
EM_CURSO_S = int(os.getenv("EM_CURSO_S", "120"))

TICK_S = float(os.getenv("MOCK_TICK_S", "2"))
LATENCIA_MAX_MS = int(os.getenv("MOCK_LATENCIA_MAX_MS", "2800"))   # o Dataflow real: mediana 2,2 s, máx. 2,8 s
COM_PREDICAO = os.getenv("MOCK_PREDICOES", "1") == "1"   # 0 = simula a parte 2 (previsão nula)

# session_key: (nome_evento, circuito, tipo, pais, voltas, tempo-base da volta, ano)
SESSOES = {
    11361: ("Italian Grand Prix", "Monza", "Permanent", "Italy", 53, 83.4, 2026),
    11340: ("Belgian Grand Prix", "Spa-Francorchamps", "Permanent", "Belgium", 44, 105.2, 2026),
    11302: ("Monaco Grand Prix", "Monte Carlo", "Temporary - Street", "Monaco", 78, 74.6, 2026),
}
SESSAO_AO_VIVO = 11361

PILOTOS = [  # numero, sigla, nome, equipe, ritmo (s por volta, relativo)
    (1, "VER", "Piloto Simulado 1", "Equipe A", -0.30),
    (4, "NOR", "Piloto Simulado 2", "Equipe B", -0.10),
    (16, "LEC", "Piloto Simulado 3", "Equipe C", 0.00),
    (44, "HAM", "Piloto Simulado 4", "Equipe C", 0.15),
    (63, "RUS", "Piloto Simulado 5", "Equipe D", 0.25),
]
_PILOTO = {p[0]: p for p in PILOTOS}
COMPOSTO_OFFSET = {"SOFT": -0.5, "MEDIUM": 0.0, "HARD": 0.4}
MOTIVOS_DLQ = ["JSON inválido", "duracao_s <= 0", "campo obrigatório ausente",
               "lap_number <= 0", "composto fora da lista", "tipo errado"]


def _volta(session_key: int, driver: int, lap: int) -> dict:
    """Uma volta determinística: mesma entrada, mesma saída."""
    _, _, _, _, total, base, _ = SESSOES[session_key]
    rng = random.Random(session_key * 100_000 + driver * 1_000 + lap)
    pit = 14 + (driver % 5) * 3                           # volta do pit stop de cada piloto
    stint, ini = (1, 1) if lap <= pit else (2, pit + 1)
    composto = ("SOFT" if driver == 63 else "MEDIUM") if stint == 1 else "HARD"
    idade = lap - ini + 1
    eh_entrada, eh_saida = lap == pit, lap == pit + 1
    tempo = (base + _PILOTO[driver][4] + COMPOSTO_OFFSET[composto] + 0.055 * idade
             - 0.025 * lap + rng.gauss(0, 0.22))
    tempo += 3.0 if eh_entrada else 18.0 if eh_saida else 0.0
    s1, s2 = tempo * 0.32, tempo * 0.35
    return {"session_key": session_key, "driver_number": driver, "lap_number": lap,
            "duracao_s": round(tempo, 3), "setor_1_s": round(s1, 3), "setor_2_s": round(s2, 3),
            "setor_3_s": round(tempo - s1 - s2, 3), "composto": composto, "stint_number": stint,
            "idade_pneu": idade, "eh_saida_box": eh_saida, "eh_entrada_box": eh_entrada,
            "gap_medio_a_frente": round(abs(rng.gauss(4, 3)), 3), "temp_pista": round(rng.gauss(53, 1.2), 1)}


def _todas_voltas(session_key: int) -> list[dict]:
    return [_volta(session_key, p[0], n) for p in PILOTOS for n in range(1, SESSOES[session_key][4] + 1)]


class RepoMock:
    nome = "mock"

    def __init__(self):
        self.t0 = datetime.now(timezone.utc)
        self._eventos: dict[int, dict] = {}

    def ping(self) -> None:
        return None

    # ---------- histórico (batch) ----------

    def sessoes(self, year: Optional[int]) -> list[dict]:
        saida = []
        for key, (evento, circuito, tipo, pais, _, _, ano) in SESSOES.items():
            if year in (None, ano):
                saida.append({"session_key": key, "year": ano, "session_name": "Race",
                              "nome_evento": evento, "circuito": circuito, "circuit_type": tipo,
                              "pais": pais, "eh_corrida": True,
                              "data_inicio_agendada": datetime(ano, 9 - (key % 3), 6, 13, tzinfo=timezone.utc)})
        return saida

    def pilotos(self, session_key: int) -> list[dict]:
        if session_key not in SESSOES:
            return []
        return [{"driver_number": n, "sigla": s, "nome_completo": nome, "equipe": eq}
                for n, s, nome, eq, _ in PILOTOS]

    def voltas(self, session_key: int, driver_number: Optional[int], origem: Optional[str]) -> list[dict]:
        if session_key not in SESSOES:
            return []
        saida = []
        if origem in (None, "batch"):
            saida += [{"origem": "batch", **v} for v in _todas_voltas(session_key)]
        if origem in (None, "stream") and session_key == SESSAO_AO_VIVO:
            saida += [{"origem": "stream", **{k: e[k] for k in _CAMPOS_VOLTA}} for e in self._visiveis()]
        return [v for v in saida if driver_number in (None, v["driver_number"])]

    def degradacao(self, session_key: Optional[int], circuito: Optional[str]) -> list[dict]:
        grupos: dict[tuple, list[float]] = {}
        for key, dados in SESSOES.items():
            if session_key not in (None, key) or circuito not in (None, dados[1]):
                continue
            for v in _todas_voltas(key):
                if not (v["eh_entrada_box"] or v["eh_saida_box"]):
                    grupos.setdefault((v["composto"], v["idade_pneu"]), []).append(v["duracao_s"])
        return [{"composto": c, "idade_pneu": i, "tempo_medio_s": round(sum(t) / len(t), 3), "voltas": len(t)}
                for (c, i), t in sorted(grupos.items())]

    def comparativo_origem(self) -> list[dict]:
        batch = sum(d[4] for d in SESSOES.values()) * len(PILOTOS)
        stream = len(self._visiveis())
        return [{"origem": "batch", "voltas": batch, "sessoes": len(SESSOES),
                 "primeira_volta": datetime(2026, 3, 1, tzinfo=timezone.utc),
                 "ultima_volta": datetime(2026, 9, 6, tzinfo=timezone.utc)},
                {"origem": "stream", "voltas": stream, "sessoes": 1 if stream else 0,
                 "primeira_volta": self.t0 if stream else None, "ultima_volta": datetime.now(timezone.utc) if stream else None}]

    # ---------- stream ----------

    def _evento(self, k: int) -> dict:
        if k not in self._eventos:
            por_replay = SESSOES[SESSAO_AO_VIVO][4] * len(PILOTOS)
            replay, j = divmod(k, por_replay)
            lap, drv = j // len(PILOTOS) + 1, PILOTOS[j % len(PILOTOS)][0]
            volta = _volta(SESSAO_AO_VIVO, drv, lap)
            rng = random.Random(k)
            ts = self.t0 + timedelta(seconds=k * TICK_S)
            latencia = int(rng.uniform(LATENCIA_MAX_MS * 0.55, LATENCIA_MAX_MS))
            apto = not (volta["eh_entrada_box"] or volta["eh_saida_box"])
            prevista = round(volta["duracao_s"] + rng.gauss(0.05, 0.35), 3) if COM_PREDICAO and apto else None
            self._eventos[k] = {
                **volta, "evento_id": f"mock-{k:06d}", "replay_id": f"replay-mock-{replay + 1}",
                "ts_evento": ts, "ts_processamento": ts + timedelta(milliseconds=latencia),
                "piloto": _PILOTO[drv][1], "year": 2026, "circuito": SESSOES[SESSAO_AO_VIVO][1],
                "apto_modelo": apto, "latencia_ms": latencia, "tempo_previsto_s": prevista,
                "modelo_versao": "f1-degradacao-pneu@v1" if prevista else None}
        return self._eventos[k]

    def _visiveis(self) -> list[dict]:
        """Eventos já processados, em ordem de processamento."""
        agora = datetime.now(timezone.utc)
        n = max(0, int((agora - self.t0).total_seconds() / TICK_S) + 1)
        return [e for e in (self._evento(k) for k in range(n)) if e["ts_processamento"] <= agora]

    def eventos_stream(self, desde: Optional[datetime], limite: int) -> list[dict]:
        todos = self._visiveis()
        if desde:
            return [e for e in todos if e["ts_processamento"] >= desde][:limite]
        return todos[-limite:]

    def stream_resumo(self) -> dict:
        todos = self._visiveis()
        lat = sorted(e["latencia_ms"] for e in todos)
        corte = datetime.now(timezone.utc) - timedelta(minutes=30)
        minutos: dict[datetime, int] = {}
        for e in todos:
            if e["ts_evento"] >= corte:
                m = e["ts_evento"].replace(second=0, microsecond=0)
                minutos[m] = minutos.get(m, 0) + 1
        dlq_total = len(todos) // 20                       # ~5% inválidos, como no gerador
        dlq = [{"motivo": m, "eventos": dlq_total // 6 + (1 if i < dlq_total % 6 else 0)}
               for i, m in enumerate(MOTIVOS_DLQ)]
        return {"total_eventos": len(todos), "ultimo_evento": todos[-1]["ts_evento"] if todos else None,
                "aptos_modelo": sum(e["apto_modelo"] for e in todos),
                "com_predicao": sum(e["tempo_previsto_s"] is not None for e in todos),
                "latencia_p50_ms": lat[len(lat) // 2] if lat else None,
                "latencia_p95_ms": lat[int(len(lat) * 0.95)] if lat else None,
                "dlq_total": dlq_total, "dlq_por_motivo": [d for d in dlq if d["eventos"]],
                "por_minuto": [{"minuto": m, "eventos": n} for m, n in sorted(minutos.items())]}

    def replays(self) -> list[dict]:
        grupos: dict[str, list[dict]] = {}
        for e in self._visiveis():
            grupos.setdefault(e["replay_id"], []).append(e)
        return [{"replay_id": r, "session_key": SESSAO_AO_VIVO, "circuito": SESSOES[SESSAO_AO_VIVO][1],
                 "eventos": len(es), "ultimo_evento": es[-1]["ts_evento"],
                 "voltas_na_corrida": SESSOES[SESSAO_AO_VIVO][4] * len(PILOTOS),
                 "fonte": "replay",
                 "em_curso": (datetime.now(timezone.utc) - es[-1]["ts_evento"]).total_seconds()
                             <= EM_CURSO_S}
                for r, es in sorted(grupos.items(), reverse=True)]

    def previsto_real(self, session_key: Optional[int], replay_id: Optional[str], limite: int) -> dict:
        evs = [e for e in self._visiveis()
               if session_key in (None, e["session_key"]) and replay_id in (None, e["replay_id"])]
        pares = [{**{k: e[k] for k in ("evento_id", "replay_id", "session_key", "driver_number", "piloto",
                                       "lap_number", "composto", "duracao_s", "tempo_previsto_s", "modelo_versao")},
                  "erro_s": round(e["tempo_previsto_s"] - e["duracao_s"], 3)}
                 for e in evs if e["tempo_previsto_s"] is not None]
        erros = [p["erro_s"] for p in pares]
        return {"metricas": {"voltas": len(pares),
                             "mae_s": round(sum(map(abs, erros)) / len(erros), 3) if erros else None,
                             "vies_s": round(sum(erros) / len(erros), 3) if erros else None},
                "pares": pares[-limite:],
                "aguardando_predicao": sum(e["apto_modelo"] and e["tempo_previsto_s"] is None for e in evs)}


_CAMPOS_VOLTA = ("session_key", "driver_number", "lap_number", "duracao_s", "setor_1_s", "setor_2_s",
                 "setor_3_s", "composto", "stint_number", "idade_pneu", "eh_saida_box", "eh_entrada_box",
                 "gap_medio_a_frente", "temp_pista")
