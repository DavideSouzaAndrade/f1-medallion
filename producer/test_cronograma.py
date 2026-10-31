"""Verificacoes do replay contra o contrato v2 (docs/contrato-openf1.md).

Nao toca no BigQuery nem no Pub/Sub: usa uma corrida em miniatura escrita a
mao, com registros no formato da Bronze, entao roda offline e em menos de um
segundo.

    python test_cronograma.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import json, random, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corrupcao import TIPOS_INVALIDOS, TOPICO_DESCONHECIDO, corromper, sortear_invalido
from cronograma import ANTECEDENCIA, data, montar

BASE = {"meeting_key": 1293, "session_key": 11361}


def volta(n, inicio, s1, s2, s3, duracao, piloto=44, saida_box=False):
    return {**BASE, "driver_number": piloto, "lap_number": n, "date_start": inicio,
            "duration_sector_1": s1, "duration_sector_2": s2, "duration_sector_3": s3,
            "i1_speed": 300, "i2_speed": 301, "is_pit_out_lap": saida_box, "lap_duration": duracao,
            "segments_sector_1": [2048], "segments_sector_2": [2048], "segments_sector_3": [2048], "st_speed": 327}


# Piloto 44: volta 1 completa, volta 2 sem setor 1 (saida do box), volta 3
# sem lap_duration e sem seguinte. Troca de pneu na volta 2.
REGISTROS = {
    "meetings": [{"meeting_key": 1293, "meeting_name": "Italian Grand Prix", "circuit_type": "Permanent"}],
    "sessions": [{**BASE, "session_name": "Race", "circuit_short_name": "Monza", "year": 2026}],
    "laps": [
        volta(1, "2026-09-06T13:03:00+00:00", 30.0, 30.0, 30.0, 90.0),
        volta(2, "2026-09-06T13:04:30+00:00", None, 31.0, 29.0, 95.0, saida_box=True),
        volta(3, "2026-09-06T13:06:05+00:00", 28.0, 29.0, None, None),
    ],
    "stints": [
        {**BASE, "stint_number": 1, "driver_number": 44, "lap_start": 1, "lap_end": 1, "compound": "SOFT", "tyre_age_at_start": 0},
        {**BASE, "stint_number": 2, "driver_number": 44, "lap_start": 2, "lap_end": 3, "compound": "MEDIUM", "tyre_age_at_start": 0},
    ],
    "weather": [
        {**BASE, "date": "2026-09-06T12:00:00+00:00", "track_temperature": 50.0},  # pre-corrida, antigo
        {**BASE, "date": "2026-09-06T12:59:00+00:00", "track_temperature": 55.8},  # pre-corrida, o ultimo
        {**BASE, "date": "2026-09-06T13:05:00+00:00", "track_temperature": 56.0},
    ],
    "intervals": [
        {**BASE, "date": "2026-09-06T12:30:00+00:00", "driver_number": 44, "interval": 0.0, "gap_to_leader": 0.0},
        {**BASE, "date": "2026-09-06T13:03:30.250000+00:00", "driver_number": 44, "interval": 1.2, "gap_to_leader": "+1 LAP"},
    ],
    "pit": [{**BASE, "date": "2026-09-06T13:04:20+00:00", "driver_number": 44, "lap_number": 1, "lane_duration": 22.1}],
}

falhas = []


def conferir(condicao, descricao):
    print(("ok    " if condicao else "FALHA ") + descricao)
    if not condicao:
        falhas.append(descricao)


c = montar(REGISTROS)
msgs = c.mensagens
laps = [m for m in msgs if m.topico == "laps"]
stints = [m for m in msgs if m.topico == "stints"]


def versoes(n):
    return [m for m in laps if m.corpo["lap_number"] == n]


# --- ordem e inicio ----------------------------------------------------------
conferir(all(a.instante <= b.instante for a, b in zip(msgs, msgs[1:])), "mensagens em ordem de instante")
conferir([m.topico for m in msgs[:2]] == ["meetings", "sessions"], "meetings e sessions saem primeiro")
conferir(c.largada == data("2026-09-06T13:03:00+00:00"), "largada = inicio da primeira volta")
conferir(min(m.instante for m in msgs) == c.largada - ANTECEDENCIA, "replay comeca 1 min antes da largada")
clima = [m.corpo["track_temperature"] for m in msgs if m.topico == "weather"]
conferir(clima == [55.8, 56.0], "do pre-corrida so entra o ultimo clima")
conferir(c.pre_corrida == 2, "pre-corrida descartado e contado (clima antigo e intervalo do grid)")

# --- laps: versoes ---------------------------------------------------------------
v1 = versoes(1)
conferir(len(v1) == 4, "volta completa sai em 4 versoes (inicio, setor 1, setor 2, fim)")
conferir(len({m.chave for m in v1}) == 1 and v1[0].chave == "11361_1_44", "todas as versoes com o mesmo _key session_lap_driver")
conferir(v1[-1].corpo == REGISTROS["laps"][0], "versao final identica ao registro da Bronze")
conferir(all(m.corpo["lap_duration"] is None for m in v1[:-1]), "so a versao final tem lap_duration")
conferir(v1[0].corpo["duration_sector_1"] is None and v1[0].corpo["date_start"] is not None,
         "versao inicial so com o que se sabe ao cruzar a linha")
conferir(v1[1].corpo["duration_sector_1"] == 30.0 and v1[1].corpo["duration_sector_2"] is None,
         "versao do setor 1 sem o setor 2")
conferir(list(v1[0].corpo) == list(REGISTROS["laps"][0]), "versoes parciais mantem os campos e a ordem do registro")
conferir([(m.instante - c.largada).total_seconds() for m in v1] == [0, 30, 60, 90],
         "versoes nos instantes de cada setor")
conferir(len(versoes(2)) == 2, "sem setor 1, sai so inicio e fim (nao da para datar o setor 2)")
v3 = versoes(3)
conferir(v3[-1].corpo == REGISTROS["laps"][2] and (v3[-1].instante - v3[0].instante).total_seconds() == 57,
         "volta sem lap_duration e sem seguinte fecha na soma dos setores")
conferir((versoes(2)[-1].instante - c.largada).total_seconds() == 185, "volta fecha em date_start + lap_duration")

# --- stints: sem informacao do futuro -------------------------------------------
s2 = [m for m in stints if m.corpo["stint_number"] == 2]
conferir([m.corpo["lap_end"] for m in s2] == [2, 3], "stint nasce com lap_end = lap_start e avanca volta a volta")
conferir(s2[-1].corpo == REGISTROS["stints"][1], "versao final do stint identica a Bronze")
inicio_volta = {n: versoes(n)[0].instante for n in (1, 2, 3)}
conferir(all(m.instante >= inicio_volta[m.corpo["lap_end"]] for m in stints),
         "nenhuma versao do stint anuncia uma volta que ainda nao comecou")
conferir(all(m.chave == f"11361_{m.corpo['stint_number']}_44" for m in stints), "_key do stint e session_stint_driver")

# --- registros simples e dado bruto ------------------------------------------------
intervalo = next(m for m in msgs if m.topico == "intervals")
conferir(intervalo.corpo["gap_to_leader"] == "+1 LAP", "texto da origem (+1 LAP) passa intacto")
conferir(intervalo.chave == "1788699810250_44", "_key do intervalo e date em ms + piloto")
conferir(next(m for m in msgs if m.topico == "pit").chave == "11361_1_44", "_key do pit e session_lap_driver")

# --- serializacao -----------------------------------------------------------------
doc = json.loads(v1[-1].serializar(123))
conferir(list(doc)[-2:] == ["_key", "_id"] and doc["_id"] == 123, "corpo publicado = registro + _key + _id")
conferir({k: v for k, v in doc.items() if not k.startswith("_")} == REGISTROS["laps"][0], "nenhum campo do registro alterado")

# --- corrupcao ----------------------------------------------------------------------
for tipo in TIPOS_INVALIDOS:
    corpo, topico = corromper(v1[-1], 1, tipo)
    if tipo == "json_quebrado":
        try:
            json.loads(corpo)
            ok = False
        except ValueError:
            ok = True
    elif tipo == "sem_chave":
        ok = "session_key" not in json.loads(corpo)
    elif tipo == "tipo_errado":
        ok = json.loads(corpo)["driver_number"] == "N/D"
    else:
        ok = topico == TOPICO_DESCONHECIDO
    conferir(ok, f"corrupcao {tipo}")
conferir(corromper(v1[-1], 1, "sem_chave")[1] == "v1/laps", "corrupcao preserva o topico, exceto topico_desconhecido")
sorteio = random.Random(1)
conferir(all(sortear_invalido(sorteio, 0) is None for _ in range(100)), "fracao 0 nunca injeta")
conferir(all(sortear_invalido(sorteio, 1) in TIPOS_INVALIDOS for _ in range(100)), "fracao 1 sempre injeta")

print()
if falhas:
    print(f"{len(falhas)} falha(s)")
    sys.exit(1)
print("tudo certo")
