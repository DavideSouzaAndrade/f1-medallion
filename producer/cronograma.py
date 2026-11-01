"""Linha do tempo do replay: quais mensagens a OpenF1 teria publicado, e quando.

A Bronze guarda a versao final de cada registro. Ao vivo, a OpenF1 publica
alguns registros varias vezes, conforme eles vao sendo preenchidos, e cada
versao repete o mesmo `_key`. Este modulo reconstroi essas versoes a partir
da versao final, seguindo o que o codigo de ingestao ao vivo da OpenF1 faz
(github.com/br-g/openf1, ingestor_livetiming/core/processing/collections):

- laps: a volta nasce quando o piloto cruza a linha e ganha uma versao a cada
  setor. So a ultima tem lap_duration.
- stints: o stint nasce com lap_end = lap_start e ganha uma versao a cada
  volta, com lap_end avancando. Publicar o lap_end final no inicio seria
  entregar o futuro.
- weather, intervals e pit: uma mensagem por registro, no instante `date`.
- meetings e sessions: uma mensagem cada, antes de todo o resto.

O replay comeca pouco antes da largada, como quem assina o feed alguns
minutos antes da corrida. O pre-corrida da Bronze (~1 h de clima e
intervalos do grid) fica de fora, menos o ultimo registro de clima: sem ele,
as primeiras voltas chegariam sem temperatura.

Nada aqui toca em rede: entra o dicionario de registros da Bronze, sai a
lista ordenada de mensagens. E isso que deixa o modulo testavel offline.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

ANTECEDENCIA = timedelta(minutes=1)

from config import TOPICOS

Registro = dict[str, Any]

# O que a volta tem em cada momento. Campos fora destas listas so aparecem
# na versao final, que e sempre o registro da Bronze sem alteracao.
_CAMPOS_BASE = ("meeting_key", "session_key", "driver_number", "lap_number", "date_start", "is_pit_out_lap")
_CAMPOS_SETOR = {
    1: ("duration_sector_1", "segments_sector_1", "i1_speed"),
    2: ("duration_sector_2", "segments_sector_2", "i2_speed"),
}


@dataclass(frozen=True)
class Mensagem:
    instante: datetime  # momento na corrida em que a OpenF1 publicaria
    topico: str         # endpoint, sem o prefixo "v1/"
    corpo: Registro     # JSON da OpenF1, sem _key e _id
    chave: str          # o _key da OpenF1

    def serializar(self, id_: int) -> bytes:
        """O corpo como a OpenF1 entrega: o registro mais _key e _id."""
        doc = {**self.corpo, "_key": self.chave, "_id": id_}
        return json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@dataclass
class Cronograma:
    mensagens: list[Mensagem]
    largada: datetime | None    # inicio da primeira volta da corrida
    descartados: dict[str, int] = field(default_factory=dict)  # registros sem instante, por topico
    pre_corrida: int = 0        # registros anteriores ao inicio do replay


def data(valor: Any) -> datetime | None:
    if not isinstance(valor, str):
        return None
    try:
        return datetime.fromisoformat(valor)
    except ValueError:
        return None


def _ms(instante: datetime) -> str:
    # Mesma conta da OpenF1 (objects.py, _get_key_str).
    return str(int(instante.timestamp() * 1000))


def chave(topico: str, r: Registro) -> str:
    """O _key da OpenF1: a chave unica do registro unida por "_"."""
    if topico in ("laps", "pit"):
        partes = (r.get("session_key"), r.get("lap_number"), r.get("driver_number"))
    elif topico == "stints":
        partes = (r.get("session_key"), r.get("stint_number"), r.get("driver_number"))
    elif topico == "intervals":
        d = data(r.get("date"))
        partes = (_ms(d) if d else r.get("date"), r.get("driver_number"))
    elif topico == "weather":
        d = data(r.get("date"))
        partes = (_ms(d) if d else r.get("date"),)
    elif topico == "sessions":
        partes = (r.get("session_key"),)
    elif topico == "meetings":
        partes = (r.get("meeting_key"),)
    else:
        raise ValueError(f"topico fora do contrato: {topico}")
    return "_".join(str(p) for p in partes)


def _segundos(valor: Any) -> float | None:
    return float(valor) if isinstance(valor, (int, float)) and not isinstance(valor, bool) and valor > 0 else None


def _parcial(r: Registro, visiveis: set[str]) -> Registro:
    """O registro com so os campos ja conhecidos; os demais ficam nulos."""
    return {k: (v if k in visiveis else None) for k, v in r.items()}


def _versoes_da_volta(r: Registro, fim: datetime) -> list[tuple[datetime, Registro]]:
    inicio = data(r.get("date_start"))
    if inicio is None:
        return [(fim, r)]
    versoes = []
    visiveis = set(_CAMPOS_BASE)
    versoes.append((inicio, _parcial(r, visiveis)))

    # So publica a versao de um setor se souber quando ele terminou: sem o
    # setor 1, nao ha como saber o instante do fim do setor 2.
    decorrido = 0.0
    for n in (1, 2):
        duracao = _segundos(r.get(f"duration_sector_{n}"))
        if duracao is None:
            break
        decorrido += duracao
        instante = inicio + timedelta(seconds=decorrido)
        if instante >= fim:
            break
        visiveis |= set(_CAMPOS_SETOR[n])
        versoes.append((instante, _parcial(r, visiveis)))

    versoes.append((fim, r))
    return versoes


def _fim_da_volta(r: Registro, proxima: datetime | None) -> datetime | None:
    inicio = data(r.get("date_start"))
    duracao = _segundos(r.get("lap_duration"))
    if inicio is not None and duracao is not None:
        return inicio + timedelta(seconds=duracao)
    # Sem lap_duration (volta de entrada no box, abandono): a volta fecha
    # quando a seguinte comeca. Sem seguinte, soma os setores que houver.
    if proxima is not None:
        return proxima
    if inicio is not None:
        setores = [_segundos(r.get(f"duration_sector_{n}")) for n in (1, 2, 3)]
        return inicio + timedelta(seconds=sum(s for s in setores if s))
    return None


def montar(registros: dict[str, list[Registro]]) -> Cronograma:
    pendentes: list[tuple[datetime, str, Registro]] = []
    descartados: dict[str, int] = defaultdict(int)

    for topico in ("weather", "intervals", "pit"):
        for r in registros.get(topico, []):
            instante = data(r.get("date"))
            if instante is None:
                descartados[topico] += 1
                continue
            pendentes.append((instante, topico, r))

    # Inicio de cada volta por piloto: serve para fechar voltas sem duracao e
    # para saber quando cada stint avanca.
    inicio_volta: dict[int, dict[int, datetime]] = defaultdict(dict)
    for r in registros.get("laps", []):
        inicio = data(r.get("date_start"))
        if inicio is not None and isinstance(r.get("lap_number"), int):
            inicio_volta[r.get("driver_number")][r["lap_number"]] = inicio

    for r in registros.get("laps", []):
        proxima = inicio_volta[r.get("driver_number")].get((r.get("lap_number") or 0) + 1)
        fim = _fim_da_volta(r, proxima)
        if fim is None:
            descartados["laps"] += 1
            continue
        for instante, versao in _versoes_da_volta(r, fim):
            pendentes.append((instante, "laps", versao))

    for r in registros.get("stints", []):
        voltas = inicio_volta[r.get("driver_number")]
        lap_start, lap_end = r.get("lap_start"), r.get("lap_end")
        if not isinstance(lap_start, int) or not isinstance(lap_end, int):
            descartados["stints"] += 1
            continue
        # O stint nasce quando o piloto comeca a primeira volta com o pneu.
        no_stint = sorted(n for n in voltas if lap_start <= n <= lap_end)
        if not no_stint:
            descartados["stints"] += 1  # stint que nunca foi para a pista
            continue
        for n in no_stint:
            versao = r if n == lap_end else {**r, "lap_end": max(n, lap_start)}
            pendentes.append((voltas[n], "stints", versao))
        if no_stint[-1] != lap_end:
            # A ultima volta do stint nao tem inicio registrado: a versao
            # final sai junto com a ultima conhecida, para o stint terminar
            # igual ao da Bronze.
            pendentes.append((voltas[no_stint[-1]], "stints", r))

    largada = min((i for voltas in inicio_volta.values() for i in voltas.values()), default=None)
    pre_corrida = 0
    if largada is not None:
        inicio = largada - ANTECEDENCIA
        antes = [p for p in pendentes if p[0] < inicio]
        pendentes = [p for p in pendentes if p[0] >= inicio]
        pre_corrida = len(antes)
        clima = [p for p in antes if p[1] == "weather"]
        if clima:
            ultimo = max(clima, key=lambda p: p[0])
            pendentes.append((inicio, "weather", ultimo[2]))
            pre_corrida -= 1
        for topico in ("meetings", "sessions"):
            for r in registros.get(topico, []):
                pendentes.append((inicio, topico, r))

    ordem = {t: i for i, t in enumerate(TOPICOS)}
    # sorted e estavel: versoes do mesmo registro no mesmo instante mantem a
    # ordem em que foram criadas.
    pendentes.sort(key=lambda p: (p[0], ordem[p[1]]))
    mensagens = [Mensagem(instante, topico, r, chave(topico, r)) for instante, topico, r in pendentes]
    return Cronograma(mensagens, largada, dict(descartados), pre_corrida)
