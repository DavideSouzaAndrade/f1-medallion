"""Validacao do lado do Dataflow contra o contrato v1.

Nao toca no Pub/Sub nem no BigQuery: monta os eventos com o proprio
producer/evento.py, entao roda offline em menos de um segundo e cobre as
duas frentes de uma vez -- se o gerador e a validacao divergirem do
contrato, a falha aparece aqui, nao em producao.

    python streaming/test_validacao.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "streaming"))
sys.path.insert(0, str(RAIZ / "producer"))

from evento import TIPOS_INVALIDOS, corromper, montar_evento, serializar
from validacao import (
    CAMPOS_DA_TABELA, MOTIVO_AUSENTE, MOTIVO_COMPOSTO, MOTIVO_DURACAO,
    MOTIVO_JSON, MOTIVO_TIPO, MOTIVO_VOLTA, EventoInvalido, decodificar, validar,
)

# A mesma volta do test_evento.py do gerador: volta 23 do piloto 44 no GP da
# Italia de 2026, como vem da Silver.
VOLTA = {
    "session_key": 11361, "driver_number": 44, "lap_number": 23, "year": 2026,
    "circuito": "Monza", "circuit_type": "Permanent",
    "data_inicio": datetime(2026, 9, 6, 14, 10, 15, tzinfo=timezone.utc),
    "duracao_s": 85.3, "setor_1_s": 27.485, "setor_2_s": 29.499, "setor_3_s": 28.316,
    "composto": "MEDIUM", "stint_number": 2, "idade_pneu": 20, "volta_no_stint": 20,
    "eh_saida_box": False, "eh_entrada_box": False,
    "gap_medio_a_frente": 3.328, "temp_ar": 32.4, "temp_pista": 53.8, "umidade": 37.5, "chuva": 0,
}
PUBLICADO = datetime(2026, 10, 17, 15, 42, 10, 123000, tzinfo=timezone.utc)
PROCESSADO = PUBLICADO + timedelta(milliseconds=250)

falhas = []


def confere(nome, condicao):
    print(("ok   " if condicao else "FALHA"), nome)
    if not condicao:
        falhas.append(nome)


def motivo_de(corpo: bytes) -> str | None:
    """O motivo com que este corpo seria rejeitado, ou None se passa."""
    try:
        validar(decodificar(corpo), PROCESSADO)
    except EventoInvalido as erro:
        return str(erro)
    return None


# --- caminho feliz -----------------------------------------------------------

evento = montar_evento(VOLTA, "replay-teste", agora=PUBLICADO)
linha = validar(decodificar(serializar(evento)), PROCESSADO)

confere("linha tem exatamente as colunas da tabela", tuple(linha) == CAMPOS_DA_TABELA)
confere("ts_evento vira datetime com fuso",
        linha["ts_evento"] == PUBLICADO and linha["ts_evento"].tzinfo is not None)
confere("data_inicio preservada", linha["data_inicio"] == VOLTA["data_inicio"])
confere("latencia_ms = processamento - ts_evento", linha["latencia_ms"] == 250)
confere("_ts_processamento e o instante passado", linha["_ts_processamento"] == PROCESSADO)
confere("chuva continua int", linha["chuva"] == 0 and isinstance(linha["chuva"], int))
confere("duracao_s continua float", linha["duracao_s"] == 85.3)
confere("booleanos preservados",
        linha["eh_saida_box"] is False and linha["eh_entrada_box"] is False)
confere("colunas da parte 3 nao sao produzidas aqui",
        "tempo_previsto_s" not in linha and "modelo_versao" not in linha)

# --- apto_modelo -------------------------------------------------------------

confere("volta seca e completa e apta", linha["apto_modelo"] is True)


def apto_com(**mudancas) -> bool:
    return validar(
        decodificar(serializar(montar_evento({**VOLTA, **mudancas}, "r", agora=PUBLICADO))),
        PROCESSADO,
    )["apto_modelo"]


confere("composto de chuva nao e apto", apto_com(composto="INTERMEDIATE") is False)
confere("composto desconhecido nao e apto", apto_com(composto="UNKNOWN") is False)
confere("sem duracao nao e apto", apto_com(duracao_s=None) is False)
confere("sem idade de pneu nao e apto", apto_com(idade_pneu=None) is False)
confere("sem temperatura de pista nao e apto", apto_com(temp_pista=None) is False)
confere("saida de box nao e apta", apto_com(eh_saida_box=True) is False)
confere("entrada de box nao e apta", apto_com(eh_entrada_box=True) is False)
confere("inapta ainda assim vira linha, nao DLQ",
        motivo_de(serializar(montar_evento({**VOLTA, "composto": "WET"}, "r", agora=PUBLICADO))) is None)

# --- normalizacao ------------------------------------------------------------

minusculo = validar(
    decodificar(serializar(montar_evento({**VOLTA, "composto": " medium "}, "r", agora=PUBLICADO))),
    PROCESSADO,
)
confere("composto em minuscula com espacos vira MEDIUM", minusculo["composto"] == "MEDIUM")
confere("composto normalizado volta a ser apto", minusculo["apto_modelo"] is True)
confere("espaco nas pontas do texto sai",
        validar(decodificar(serializar(montar_evento({**VOLTA, "circuito": "  Monza  "}, "r", agora=PUBLICADO))),
                PROCESSADO)["circuito"] == "Monza")

# --- os seis tipos invalidos do gerador --------------------------------------
#
# Cada tipo injetado tem que cair na DLQ com o motivo que o contrato promete.

ESPERADO = {
    "json_quebrado": MOTIVO_JSON,
    "sem_session_key": MOTIVO_AUSENTE,
    "tipo_errado": MOTIVO_TIPO,
    "volta_zero": MOTIVO_VOLTA,
    "duracao_negativa": MOTIVO_DURACAO,
    "composto_invalido": MOTIVO_COMPOSTO,
}

confere("os seis tipos do gerador estao cobertos", set(TIPOS_INVALIDOS) == set(ESPERADO))

for tipo, motivo_esperado in ESPERADO.items():
    obtido = motivo_de(corromper(evento, tipo))
    confere(f"{tipo} -> {motivo_esperado}", obtido == motivo_esperado)

# --- casos de borda que o gerador nao produz ---------------------------------

confere("corpo vazio e JSON invalido", motivo_de(b"") == MOTIVO_JSON)
confere("JSON que nao e objeto e invalido", motivo_de(b"123") == MOTIVO_JSON)
confere("bytes que nao sao UTF-8", motivo_de(b"\xff\xfe") == MOTIVO_JSON)
confere("campo obrigatorio explicitamente nulo conta como ausente",
        motivo_de(serializar({**evento, "circuito": None})) == MOTIVO_AUSENTE)
confere("lap_number negativo", motivo_de(serializar({**evento, "lap_number": -3})) == MOTIVO_VOLTA)
confere("duracao zero", motivo_de(serializar({**evento, "duracao_s": 0})) == MOTIVO_DURACAO)
confere("lap_number 23.0 e aceito como inteiro",
        motivo_de(serializar({**evento, "lap_number": 23.0})) is None)
confere("lap_number 23.5 nao e inteiro",
        motivo_de(serializar({**evento, "lap_number": 23.5})) == MOTIVO_TIPO)
confere("booleano no lugar de inteiro e tipo errado",
        motivo_de(serializar({**evento, "lap_number": True})) == MOTIVO_TIPO)
confere("inteiro no lugar de booleano e tipo errado",
        motivo_de(serializar({**evento, "eh_saida_box": 1})) == MOTIVO_TIPO)
confere("ts_evento com data impossivel",
        motivo_de(serializar({**evento, "ts_evento": "ontem"})) == MOTIVO_TIPO)
confere("inteiro e aceito onde se espera float",
        motivo_de(serializar({**evento, "temp_pista": 54})) is None)
confere("composto nulo passa, so nao fica apto",
        motivo_de(serializar({**evento, "composto": None})) is None)

# A ordem das checagens decide o motivo quando o evento viola mais de uma
# regra: o contrato lista ausencia antes de faixa de valor.
confere("evento com dois defeitos reporta o primeiro da ordem do contrato",
        motivo_de(serializar({**evento, "circuito": None, "lap_number": 0})) == MOTIVO_AUSENTE)

# Latencia negativa e possivel: relogio do worker atras do relogio do
# gerador. Nao e motivo de rejeicao, so um numero estranho.
confere("relogio atrasado nao rejeita o evento",
        motivo_de(serializar({**evento, "ts_evento": "2099-01-01T00:00:00.000Z"})) is None)

print()
print(f"{len(falhas)} falha(s)" if falhas else "tudo certo")
sys.exit(1 if falhas else 0)
