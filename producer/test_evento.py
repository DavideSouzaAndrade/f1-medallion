"""Validacao da montagem do evento contra o contrato v1.

Nao toca no BigQuery nem no Pub/Sub: usa uma volta escrita a mao, entao
roda offline e em menos de um segundo.

    python test_evento.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import json, random, sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evento import CAMPOS_DA_VOLTA, TIPOS_INVALIDOS, corromper, montar_evento, serializar, sortear_invalido

# Volta 23 do piloto 44 no GP da Italia de 2026, como vem da Silver.
VOLTA = {
    "session_key": 11361, "driver_number": 44, "lap_number": 23, "year": 2026,
    "circuito": "Monza", "circuit_type": "Permanent",
    "data_inicio": datetime(2026, 9, 6, 14, 10, 15, tzinfo=timezone.utc),
    "data_fim": datetime(2026, 9, 6, 14, 11, 40, 300000, tzinfo=timezone.utc),
    "duracao_s": 85.3, "setor_1_s": 27.485, "setor_2_s": 29.499, "setor_3_s": 28.316,
    "composto": "MEDIUM", "stint_number": 2, "idade_pneu": 20, "volta_no_stint": 20,
    "eh_saida_box": False, "eh_entrada_box": False,
    "gap_medio_a_frente": float("nan"), "temp_ar": 32.4, "temp_pista": 53.8, "umidade": 37.5, "chuva": 0,
}
AGORA = datetime(2026, 10, 17, 15, 42, 10, 123456, tzinfo=timezone.utc)

falhas = []
def confere(nome, condicao):
    print(("ok   " if condicao else "FALHA"), nome)
    if not condicao:
        falhas.append(nome)

evento = montar_evento(VOLTA, "replay-teste", agora=AGORA)
corpo = serializar(evento)
lido = json.loads(corpo)

confere("campos na ordem do contrato",
        list(lido) == ["evento_id", "replay_id", "ts_evento", *CAMPOS_DA_VOLTA])
confere("data_fim nao vaza para o evento", "data_fim" not in lido)
confere("ts_evento em ISO UTC com milissegundos e Z", lido["ts_evento"] == "2026-10-17T15:42:10.123Z")
confere("data_inicio em ISO UTC", lido["data_inicio"] == "2026-09-06T14:10:15.000Z")
confere("NaN vira null", lido["gap_medio_a_frente"] is None)
confere("chuva continua inteiro", lido["chuva"] == 0 and isinstance(lido["chuva"], int))
confere("evento_id unico por chamada",
        montar_evento(VOLTA, "r", agora=AGORA)["evento_id"] != montar_evento(VOLTA, "r", agora=AGORA)["evento_id"])

# Cada tipo invalido quebra exatamente a regra que diz quebrar.
quebrados = {tipo: corromper(evento, tipo) for tipo in TIPOS_INVALIDOS}
try:
    json.loads(quebrados["json_quebrado"])
    confere("json_quebrado nao faz parse", False)
except json.JSONDecodeError:
    confere("json_quebrado nao faz parse", True)
confere("sem_session_key remove o campo", "session_key" not in json.loads(quebrados["sem_session_key"]))
confere("tipo_errado poe texto em lap_number", isinstance(json.loads(quebrados["tipo_errado"])["lap_number"], str))
confere("volta_zero zera lap_number", json.loads(quebrados["volta_zero"])["lap_number"] == 0)
confere("duracao_negativa", json.loads(quebrados["duracao_negativa"])["duracao_s"] < 0)
confere("composto_invalido", json.loads(quebrados["composto_invalido"])["composto"] == "SUPERSOFT")
confere("invalido ganha evento_id proprio",
        json.loads(quebrados["volta_zero"])["evento_id"] != evento["evento_id"])
confere("evento original intacto apos corromper", json.loads(serializar(evento)) == lido)

sorteio = random.Random(42)
sorteados = [sortear_invalido(sorteio, 0.1) for _ in range(10_000)]
proporcao = sum(t is not None for t in sorteados) / len(sorteados)
confere("fracao 0.1 sorteia ~10% de invalidos", 0.08 < proporcao < 0.12)
confere("fracao 0 nunca sorteia", all(sortear_invalido(sorteio, 0) is None for _ in range(1000)))

print()
print(f"{len(falhas)} falha(s)" if falhas else "tudo certo")
sys.exit(1 if falhas else 0)
