"""Montagem da volta conferida contra o contrato v2, offline.

Nao toca no Pub/Sub, no BigQuery nem no Beam: monta a linha do tempo das
mensagens com o proprio producer/cronograma.py e empurra os registros pelo
contrato.py e pelo montagem.py, na mao. Roda em menos de um segundo e cobre
as duas frentes de uma vez -- se o gerador e a montagem divergirem do
contrato, a falha aparece aqui, nao em producao.

    python streaming/test_montagem.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "streaming"))
sys.path.insert(0, str(RAIZ / "producer"))

import montagem
from contrato import (
    MOTIVO_AUSENTE, MOTIVO_JSON, MOTIVO_SEM_CONTEXTO, MOTIVO_TIPO, MOTIVO_TOPICO,
    MensagemInvalida, chave_da_sessao, decodificar, gap, ler, topico_de,
)
from corrupcao import TIPOS_INVALIDOS, corromper
from cronograma import montar as montar_cronograma

SESSION_KEY = 11361
PILOTO = 44
REPLAY = "replay-teste"
T0 = datetime(2026, 9, 6, 14, 0, 0, tzinfo=timezone.utc)

falhas = []


def confere(nome, condicao):
    print(("ok   " if condicao else "FALHA"), nome)
    if not condicao:
        falhas.append(nome)


def iso(instante):
    return instante.isoformat()


# --- a corrida de mentira ----------------------------------------------------
#
# Cinco voltas de um piloto, com um caso de cada coisa que a silver_voltas
# trata: saida de box, entrada de box, volta sem tempo e trafego medido.

DURACOES = {1: 90.0, 2: 85.0, 3: 85.5, 4: 86.0, 5: None}
INICIOS = {}
_t = T0
for _n in range(1, 6):
    INICIOS[_n] = _t
    _t = _t + timedelta(seconds=DURACOES[_n] or 95.0)

LAPS = []
for n, inicio in INICIOS.items():
    duracao = DURACOES[n]
    setores = None if duracao is None else (duracao * 0.32, duracao * 0.35, duracao * 0.33)
    LAPS.append({
        "meeting_key": 1293, "session_key": SESSION_KEY, "driver_number": PILOTO,
        "lap_number": n, "date_start": iso(inicio),
        "lap_duration": duracao,
        "duration_sector_1": setores[0] if setores else None,
        "duration_sector_2": setores[1] if setores else None,
        "duration_sector_3": setores[2] if setores else None,
        "is_pit_out_lap": n == 1,
        "i1_speed": 300, "i2_speed": 301, "st_speed": 327,
        "segments_sector_1": [2048], "segments_sector_2": [2048], "segments_sector_3": [2048],
    })

STINTS = [{
    "meeting_key": 1293, "session_key": SESSION_KEY, "driver_number": PILOTO,
    "stint_number": 1, "lap_start": 1, "lap_end": 5,
    "compound": "MEDIUM", "tyre_age_at_start": 0,
}]

# Entrada no box na volta 4: o endpoint laps nao tem flag de entrada, so o pit.
PIT = [{
    "meeting_key": 1293, "session_key": SESSION_KEY, "driver_number": PILOTO,
    "lap_number": 4, "date": iso(INICIOS[4] + timedelta(seconds=80)), "pit_duration": 24.5,
}]

WEATHER = [
    {"meeting_key": 1293, "session_key": SESSION_KEY, "date": iso(T0 - timedelta(seconds=30)),
     "air_temperature": 32.4, "track_temperature": 53.8, "humidity": 37.5,
     "rainfall": 0, "wind_speed": 3.1},
    {"meeting_key": 1293, "session_key": SESSION_KEY, "date": iso(INICIOS[3] + timedelta(seconds=5)),
     "air_temperature": 33.0, "track_temperature": 55.0, "humidity": 36.0,
     "rainfall": 0, "wind_speed": 3.4},
]

# Dois intervals por volta, dentro da janela dela. Um deles na volta 2 vem
# como "+1 LAP": nao e tempo, e distancia -- fica fora da media do gap.
GAPS = {1: (1.0, 3.0), 2: (2.0, "+1 LAP"), 3: (4.0, 6.0), 4: (8.0, 10.0), 5: (None, 12.0)}
INTERVALS = []
for n, inicio in INICIOS.items():
    for i, valor in enumerate(GAPS[n]):
        INTERVALS.append({
            "meeting_key": 1293, "session_key": SESSION_KEY, "driver_number": PILOTO,
            "date": iso(inicio + timedelta(seconds=10 + i * 20)),
            "interval": valor, "gap_to_leader": valor,
        })

SESSIONS = [{
    "meeting_key": 1293, "session_key": SESSION_KEY, "year": 2026,
    "circuit_short_name": "Monza", "circuit_key": 39, "session_name": "Race",
    "session_type": "Race", "date_start": iso(T0), "date_end": iso(_t),
    "gmt_offset": "02:00:00", "location": "Monza", "country_name": "Italy",
    "country_code": "ITA", "is_cancelled": False,
}]

MEETINGS = [{
    "meeting_key": 1293, "circuit_type": "Permanent", "meeting_name": "Italian Grand Prix",
    "meeting_official_name": "FORMULA 1 GRAN PREMIO D'ITALIA 2026",
}]

REGISTROS = {"laps": LAPS, "stints": STINTS, "pit": PIT, "weather": WEATHER,
             "intervals": INTERVALS, "sessions": SESSIONS, "meetings": MEETINGS}

cronograma = montar_cronograma(REGISTROS)
confere("o cronograma do gerador produziu mensagens", len(cronograma.mensagens) > 0)
confere("a largada e o inicio da volta 1", cronograma.largada == INICIOS[1])


# --- a propriedade que autoriza substituir a versao em vez de mesclar --------
#
# As versoes intermediarias da volta so PREENCHEM campos: cada uma e um
# superconjunto da anterior. E isso que torna "ficar com a de maior _id"
# equivalente a mesclar campo a campo. Se o gerador mudar essa propriedade,
# montagem._volta passa a perder dado -- e este teste quebra primeiro.

versoes = [m for m in cronograma.mensagens if m.topico == "laps" and m.chave == f"{SESSION_KEY}_2_{PILOTO}"]
confere("a volta tem varias versoes com o mesmo _key", len(versoes) >= 2)

cresce = True
for anterior, seguinte in zip(versoes, versoes[1:]):
    cheios_antes = {k for k, v in anterior.corpo.items() if v is not None}
    cheios_depois = {k for k, v in seguinte.corpo.items() if v is not None}
    if not cheios_antes <= cheios_depois:
        cresce = False
confere("cada versao da volta e superconjunto da anterior", cresce)
confere("so a ultima versao tem lap_duration",
        [v.corpo.get("lap_duration") is not None for v in versoes] ==
        [False] * (len(versoes) - 1) + [True])


# --- empurrar as mensagens pelo pipeline, fora de ordem ----------------------


def atributos(msg, **extras):
    return {"topico": f"v1/{msg.topico}", "session_key": str(SESSION_KEY),
            "fonte": "replay", "replay_id": REPLAY, **extras}


def rodar(mensagens, embaralhar=None):
    """Processa as mensagens e devolve (linhas emitidas, motivos de DLQ)."""
    estado = montagem.estado_novo()
    linhas, motivos = [], []
    agora = 1_000_000.0

    pares = []
    for i, msg in enumerate(mensagens, start=1):
        pares.append((msg, i))
    if embaralhar is not None:
        random.Random(embaralhar).shuffle(pares)

    for msg, id_ in pares:
        agora += 0.01
        corpo = msg.serializar(id_)
        atrs = atributos(msg)
        try:
            topico = topico_de(atrs)
            chave_da_sessao(atrs)
            registro = ler(topico, decodificar(corpo))
        except MensagemInvalida as erro:
            motivos.append(str(erro))
            continue
        montagem.aplicar(estado, registro, msg.instante, agora)

    # Vence a carencia e colhe; depois vence o prazo de abandono, que solta
    # a ultima volta do piloto (sem lap_duration e sem volta seguinte).
    for salto in (montagem.GRACA_S + 1, montagem.ABANDONO_S + 1):
        agora += salto
        processado = datetime.fromtimestamp(agora, timezone.utc)
        for piloto, volta in montagem.vencidas(estado, agora):
            try:
                linhas.append(montagem.montar(estado, SESSION_KEY, REPLAY, piloto, volta, processado))
            except MensagemInvalida as erro:
                motivos.append(str(erro))
            montagem.remover(estado, piloto, volta)

    return linhas, motivos


linhas, motivos = rodar(cronograma.mensagens)
por_volta = {l["lap_number"]: l for l in linhas}

confere("saiu uma linha por volta", sorted(por_volta) == [1, 2, 3, 4, 5])
confere("a DLQ ficou vazia com dado limpo", motivos == [])
confere("a linha tem as colunas da tabela, mais a auxiliar",
        all(tuple(l) == montagem.CAMPOS_DA_TABELA + (montagem.AUXILIAR_ANOMALA,)
            for l in linhas))
confere("a auxiliar nao e coluna de f1_stream.voltas",
        montagem.AUXILIAR_ANOMALA not in montagem.CAMPOS_DA_TABELA)
confere("a auxiliar e booleana",
        all(isinstance(l[montagem.AUXILIAR_ANOMALA], bool) for l in linhas))

# --- contexto da sessao, que nao existe no registro da volta -----------------

confere("year vem de v1/sessions", all(l["year"] == 2026 for l in linhas))
confere("circuito vem de v1/sessions", all(l["circuito"] == "Monza" for l in linhas))
confere("circuit_type vem de v1/meetings", all(l["circuit_type"] == "Permanent" for l in linhas))
confere("nenhuma coluna NOT NULL saiu nula",
        all(l[c] is not None for l in linhas for c in
            ("evento_id", "replay_id", "ts_evento", "session_key", "driver_number",
             "lap_number", "year", "circuito", "circuit_type", "eh_saida_box",
             "eh_entrada_box", "apto_modelo", "_ts_processamento")))

# --- a volta em si -----------------------------------------------------------

confere("duracao_s e a da versao final", por_volta[2]["duracao_s"] == 85.0)
confere("volta sem lap_duration sai com duracao nula", por_volta[5]["duracao_s"] is None)
confere("os tres setores chegaram", all(por_volta[2][f"setor_{n}_s"] is not None for n in (1, 2, 3)))
confere("data_inicio e a da corrida, nao a do replay", por_volta[2]["data_inicio"] == INICIOS[2])

# --- box ---------------------------------------------------------------------

confere("eh_saida_box vem de is_pit_out_lap", por_volta[1]["eh_saida_box"] is True)
confere("volta sem saida de box nao e marcada", por_volta[2]["eh_saida_box"] is False)
confere("eh_entrada_box e deduzida do endpoint pit", por_volta[4]["eh_entrada_box"] is True)
confere("volta sem registro de pit nao e entrada", por_volta[2]["eh_entrada_box"] is False)

# --- pneu --------------------------------------------------------------------

confere("composto vem do stint", all(l["composto"] == "MEDIUM" for l in linhas))
confere("stint_number vem do stint", all(l["stint_number"] == 1 for l in linhas))
# idade = tyre_age_at_start + (volta - lap_start) + 1, igual a silver_voltas
confere("idade_pneu conta a propria volta", [por_volta[n]["idade_pneu"] for n in (1, 2, 3, 4, 5)] == [1, 2, 3, 4, 5])

# --- clima -------------------------------------------------------------------

confere("clima do inicio da corrida vale para as voltas 1 e 2",
        por_volta[2]["temp_pista"] == 53.8 and por_volta[2]["temp_ar"] == 32.4)
# A segunda leitura e 5 s DEPOIS do inicio da volta 3: a silver_voltas usa o
# clima vigente no INICIO da volta, entao a volta 3 ainda e a leitura antiga.
confere("a volta 3 usa o clima vigente no inicio dela", por_volta[3]["temp_pista"] == 53.8)
confere("a volta 4 ja usa a leitura nova", por_volta[4]["temp_pista"] == 55.0)
confere("chuva continua inteiro", por_volta[2]["chuva"] == 0 and isinstance(por_volta[2]["chuva"], int))

# --- trafego -----------------------------------------------------------------

confere("gap medio e a media dos intervals da janela", por_volta[1]["gap_medio_a_frente"] == 2.0)
# Na volta 2 um dos dois gaps e "+1 LAP": fica fora da media, que vira so o outro.
confere('gap em "+N LAP" fica fora da media', por_volta[2]["gap_medio_a_frente"] == 2.0)
confere("gap nulo fica fora da media", por_volta[5]["gap_medio_a_frente"] == 12.0)

# --- modelo ------------------------------------------------------------------

confere("saida de box nao e apta", por_volta[1]["apto_modelo"] is False)
confere("entrada de box nao e apta", por_volta[4]["apto_modelo"] is False)
confere("volta sem tempo nao e apta", por_volta[5]["apto_modelo"] is False)
confere("volta seca e completa e apta",
        por_volta[2]["apto_modelo"] is True and por_volta[3]["apto_modelo"] is True)
# volta_no_stint numera as voltas aptas do stint, como a gold_degradacao_pneu
confere("volta_no_stint numera so as voltas aptas",
        por_volta[2]["volta_no_stint"] == 1 and por_volta[3]["volta_no_stint"] == 2)
confere("volta inapta nao recebe volta_no_stint",
        all(por_volta[n]["volta_no_stint"] is None for n in (1, 4, 5)))
confere("as colunas da predicao saem nulas nesta etapa",
        all(l["tempo_previsto_s"] is None and l["modelo_versao"] is None for l in linhas))

# --- identidade e latencia ---------------------------------------------------

confere("evento_id e deterministico e identifica a volta",
        por_volta[2]["evento_id"] == f"{REPLAY}:{SESSION_KEY}:{PILOTO}:2")
confere("evento_id nao repete entre voltas", len({l["evento_id"] for l in linhas}) == len(linhas))
confere("replay_id vem do atributo da mensagem", all(l["replay_id"] == REPLAY for l in linhas))
confere("latencia_ms = processamento - ts_evento",
        all(l["latencia_ms"] == int((l["_ts_processamento"] - l["ts_evento"]).total_seconds() * 1000)
            for l in linhas))

# --- colher no meio do fluxo -------------------------------------------------
#
# A regressao que importa. Rodar tudo e colher no fim esconde o bug de
# emissao prematura: em producao o timer dispara CONTINUAMENTE, no meio da
# chegada das mensagens. O primeiro desenho fechava a volta quando a
# seguinte comecava, e com isso emitia a volta antes da versao final chegar
# -- 11 de 64 voltas sem duracao_s num replay real. O resultado com colheita
# intercalada tem que ser identico ao da colheita no fim.


def reordenar(pares, janela, semente):
    """Embaralha dentro de uma janela deslizante.

    E assim que o Pub/Sub reordena: localmente, entre mensagens proximas, e
    nao a corrida inteira. Embaralhar tudo inverteria a causalidade do
    replay e testaria um cenario que nao existe.
    """
    sorteio = random.Random(semente)
    saida, restante = [], list(pares)
    while restante:
        pedaco = restante[:janela]
        del restante[:janela]
        sorteio.shuffle(pedaco)
        saida.extend(pedaco)
    return saida


def rodar_intercalado(mensagens, passo=0.5, janela=None, semente=0):
    estado = montagem.estado_novo()
    linhas = []
    agora = 1_000_000.0
    pares = list(enumerate(mensagens, start=1))
    if janela:
        pares = reordenar(pares, janela, semente)
    for i, msg in pares:
        agora += passo
        atrs = atributos(msg)
        montagem.aplicar(estado, ler(topico_de(atrs), decodificar(msg.serializar(i))),
                         msg.instante, agora)
        # O timer do Beam dispara a cada prazo vencido, nao so no fim.
        processado = datetime.fromtimestamp(agora, timezone.utc)
        for piloto, volta in montagem.vencidas(estado, agora):
            linhas.append(montagem.montar(estado, SESSION_KEY, REPLAY, piloto, volta, processado))
            montagem.remover(estado, piloto, volta)
    # Solta o que ficou aberto.
    for salto in (montagem.GRACA_SEM_TEMPO_S + 1, montagem.ABANDONO_S + 1):  # noqa: B007
        agora += salto
        processado = datetime.fromtimestamp(agora, timezone.utc)
        for piloto, volta in montagem.vencidas(estado, agora):
            linhas.append(montagem.montar(estado, SESSION_KEY, REPLAY, piloto, volta, processado))
            montagem.remover(estado, piloto, volta)
    return linhas, estado


intercaladas, estado_final = rodar_intercalado(cronograma.mensagens)
por_volta_i = {l["lap_number"]: l for l in intercaladas}

confere("colher no meio nao emite volta duas vezes", len(intercaladas) == len(linhas))
confere("colher no meio nao perde duracao_s",
        {n: l["duracao_s"] for n, l in por_volta_i.items()} ==
        {n: l["duracao_s"] for n, l in por_volta.items()})
confere("colher no meio nao perde composto",
        all(por_volta_i[n]["composto"] == "MEDIUM" for n in (1, 2, 3, 4, 5)))
confere("colher no meio nao perde idade_pneu",
        [por_volta_i[n]["idade_pneu"] for n in (1, 2, 3, 4, 5)] == [1, 2, 3, 4, 5])
confere("colher no meio da o mesmo clima",
        all(por_volta_i[n]["temp_pista"] == por_volta[n]["temp_pista"] for n in por_volta))

# A combinacao que pega os bugs de verdade: reordenacao local E colheita
# intercalada. Rodar tudo e colher no fim passava com as duas versoes do
# codigo; so esta matriz mostrou que o primeiro desenho perdia duracao_s,
# composto e gap.
#
# A premissa que isto fixa: a carencia tem que ser maior que o atraso de
# reordenacao. Medido nesta matriz, variando o passo entre mensagens:
#
#     deslocamento maximo   execucoes divergentes
#          0,2 s                   0/15
#          0,5 s                   0/15
#          1,0 s                   0/15
#          2,0 s                   1/15
#
# A fronteira e a GRACA_COMPLETA_S (1 s): a volta que ja tem tudo sai depois
# dela, e nao dos 3 s da GRACA_S. Em producao, a 110 mensagens/s do fator
# 30, 20 mensagens de deslocamento sao 0,18 s -- margem de ~5x. O teste roda
# no limite (1 s) e exige igualdade exata.
CAMPOS_SENSIVEIS = ("duracao_s", "composto", "idade_pneu", "temp_pista",
                    "gap_medio_a_frente", "volta_no_stint")
esperado = {n: {c: l[c] for c in CAMPOS_SENSIVEIS} for n, l in por_volta.items()}

reordenado_ok = True
for janela in (5, 10, 20):
    for semente in (1, 7, 42, 99, 123):
        obtidas, _ = rodar_intercalado(cronograma.mensagens, passo=0.05,
                                       janela=janela, semente=semente)
        if len(obtidas) != len(linhas):
            reordenado_ok = False
        if {l["lap_number"]: {c: l[c] for c in CAMPOS_SENSIVEIS}
                for l in obtidas} != esperado:
            reordenado_ok = False
confere("reordenacao dentro da carencia da o mesmo resultado", reordenado_ok)

# Reordenacao ACIMA da carencia degrada o valor de uma coluna, mas nunca
# perde a volta nem a duplica: o que falta e o que ainda nao tinha chegado.
degrada_sem_perder = True
for semente in (1, 7, 42, 99, 123):
    obtidas, _ = rodar_intercalado(cronograma.mensagens, passo=0.5, janela=20, semente=semente)
    if len(obtidas) != len(linhas):
        degrada_sem_perder = False
    if {l["lap_number"] for l in obtidas} != set(por_volta):
        degrada_sem_perder = False
    if any(l["duracao_s"] != por_volta[l["lap_number"]]["duracao_s"] for l in obtidas):
        degrada_sem_perder = False
confere("reordenacao acima da carencia nao perde nem duplica volta", degrada_sem_perder)

# Reentrega DEPOIS da emissao nao pode gravar uma segunda linha.
agora = 2_000_000.0
ressuscitadas = []
for i, msg in enumerate(cronograma.mensagens, start=10_000):
    agora += 0.01
    atrs = atributos(msg)
    montagem.aplicar(estado_final, ler(topico_de(atrs), decodificar(msg.serializar(i))),
                     msg.instante, agora)
agora += montagem.ABANDONO_S + 1
for piloto, volta in montagem.vencidas(estado_final, agora):
    ressuscitadas.append(volta)
confere("reentrega depois da emissao nao ressuscita a volta", ressuscitadas == [])

# O clima vigente e resolvido na EMISSAO: a leitura certa pode chegar depois
# da volta que a usa, porque o Pub/Sub nao garante ordem.
atrasadas = [m for m in cronograma.mensagens if m.topico != "weather"] + \
            [m for m in cronograma.mensagens if m.topico == "weather"]
com_clima_atrasado, _ = rodar(atrasadas)
confere("clima que chega depois da volta ainda e aplicado",
        {l["lap_number"]: l["temp_pista"] for l in com_clima_atrasado} ==
        {n: l["temp_pista"] for n, l in por_volta.items()})


# --- fora de ordem -----------------------------------------------------------
#
# O Pub/Sub nao garante ordem: a volta pode chegar antes do stint dela. O
# resultado tem que ser o mesmo.

iguais = True
for semente in (1, 7, 42):
    outras, _ = rodar(cronograma.mensagens, embaralhar=semente)
    if {l["lap_number"]: l["duracao_s"] for l in outras} != {n: l["duracao_s"] for n, l in por_volta.items()}:
        iguais = False
    if {l["lap_number"]: l["idade_pneu"] for l in outras} != {n: l["idade_pneu"] for n, l in por_volta.items()}:
        iguais = False
confere("mensagens fora de ordem dao o mesmo resultado", iguais)

# Reentrega: o Pub/Sub entrega pelo menos uma vez. A mesma mensagem duas
# vezes nao pode duplicar a volta nem somar o gap duas vezes.
dobradas, _ = rodar([m for m in cronograma.mensagens for _ in (0, 1)])
confere("mensagem repetida nao duplica a volta", len(dobradas) == len(linhas))

# --- estado pequeno ----------------------------------------------------------
#
# A chave unica por session_key so se sustenta porque o estado nao cresce
# com o volume: intervals nao sao guardados e voltas emitidas saem.

estado = montagem.estado_novo()
agora = 1_000_000.0
for i, msg in enumerate(cronograma.mensagens, start=1):
    agora += 0.01
    atrs = atributos(msg)
    montagem.aplicar(estado, ler(topico_de(atrs), decodificar(msg.serializar(i))), msg.instante, agora)
tamanho = len(json.dumps(estado, default=str))
confere(f"o estado nao guarda os intervals ({tamanho} bytes para {len(cronograma.mensagens)} mensagens)",
        tamanho < 6000)

# --- volta anomala pela mediana corrente -------------------------------------
#
# A gold_degradacao_pneu treina so com volta dentro de 1,15x a mediana da
# sessao: safety car e bandeira ficam fora. Ao vivo nao se sabe a mediana da
# corrida, so a das voltas recebidas -- e e o que o contrato v2 autoriza.

est = montagem.estado_novo()
confere("sem mediana ainda, nada e anomalo", montagem.eh_anomala(est, 300.0) is False)
est["duracoes"] = [85.0, 86.0, 87.0]
confere("volta normal nao e anomala", montagem.eh_anomala(est, 88.0) is False)
confere("volta no limite de 1,15x nao e anomala",
        montagem.eh_anomala(est, 86.0 * montagem.RAZAO_MAXIMA) is False)
confere("volta de safety car e anomala", montagem.eh_anomala(est, 145.0) is True)
confere("volta sem tempo nao e anomala", montagem.eh_anomala(est, None) is False)
confere("a razao maxima e a mesma da Gold", montagem.RAZAO_MAXIMA == 1.15)

# A mediana e das voltas JA EMITIDAS, entao as primeiras da sessao passam.
confere("a volta 1 da sessao nunca e marcada anomala",
        por_volta[1][montagem.AUXILIAR_ANOMALA] is False)
# Na corrida de mentira as cinco voltas sao parecidas: nenhuma e anomala.
confere("corrida sem incidente nao marca volta anomala",
        not any(l[montagem.AUXILIAR_ANOMALA] for l in linhas))

# --- os quatro tipos invalidos do gerador ------------------------------------

ESPERADO = {
    "json_quebrado": MOTIVO_JSON,
    "sem_chave": MOTIVO_AUSENTE,
    "tipo_errado": MOTIVO_TIPO,
    "topico_desconhecido": MOTIVO_TOPICO,
}
confere("os quatro tipos do gerador estao cobertos", set(TIPOS_INVALIDOS) == set(ESPERADO))

uma_volta = next(m for m in cronograma.mensagens if m.topico == "laps")
for tipo, esperado in ESPERADO.items():
    corpo, topico_bruto = corromper(uma_volta, 999, tipo)
    atrs = {"topico": topico_bruto, "session_key": str(SESSION_KEY), "injetado": tipo}
    try:
        registro = ler(topico_de(atrs), decodificar(corpo))
        obtido = None
    except MensagemInvalida as erro:
        obtido = str(erro)
    confere(f"{tipo} -> {esperado}", obtido == esperado)

# --- casos de borda do contrato ---------------------------------------------


def motivo_de(topico, corpo):
    try:
        ler(topico, decodificar(corpo))
    except MensagemInvalida as erro:
        return str(erro)
    return None


confere("corpo vazio e JSON invalido", motivo_de("laps", b"") == MOTIVO_JSON)
confere("JSON que nao e objeto e invalido", motivo_de("laps", b"123") == MOTIVO_JSON)
confere("bytes que nao sao UTF-8", motivo_de("laps", b"\xff\xfe") == MOTIVO_JSON)
def _erro(f):
    try:
        f()
    except MensagemInvalida as erro:
        return str(erro)
    return None


confere("topico sem o prefixo v1/ e recusado", _erro(lambda: topico_de({"topico": "laps"})) == MOTIVO_TOPICO)
confere("topico ausente e recusado", _erro(lambda: topico_de({})) == MOTIVO_TOPICO)
confere("topico fora dos sete e recusado",
        _erro(lambda: topico_de({"topico": "v1/car_data"})) == MOTIVO_TOPICO)
confere("session_key ausente no atributo e recusada",
        _erro(lambda: chave_da_sessao({})) == MOTIVO_AUSENTE)
confere("session_key nao numerica no atributo e recusada",
        _erro(lambda: chave_da_sessao({"session_key": "N/D"})) == MOTIVO_AUSENTE)

confere("stint sem lap_start e recusado",
        motivo_de("stints", json.dumps({**STINTS[0], "lap_start": None}).encode()) == MOTIVO_AUSENTE)
confere("lap_number 23.0 e aceito como inteiro",
        motivo_de("laps", json.dumps({**LAPS[1], "lap_number": 23.0}).encode()) is None)
confere("lap_number 23.5 nao e inteiro",
        motivo_de("laps", json.dumps({**LAPS[1], "lap_number": 23.5}).encode()) == MOTIVO_TIPO)
confere("booleano no lugar de inteiro e tipo errado",
        motivo_de("laps", json.dumps({**LAPS[1], "lap_number": True}).encode()) == MOTIVO_TIPO)
confere("data impossivel e tipo errado",
        motivo_de("laps", json.dumps({**LAPS[1], "date_start": "ontem"}).encode()) == MOTIVO_TIPO)
confere("meetings nao precisa de session_key no corpo",
        motivo_de("meetings", json.dumps(MEETINGS[0]).encode()) is None)

# gap: o tratamento do dado bruto real
confere('"+1 LAP" vira voltas atras, nao segundos', gap("+1 LAP") == (None, 1))
confere('"+2 LAPS" idem', gap("+2 LAPS") == (None, 2))
confere("gap numerico e segundos", gap(3.328) == (3.328, None))
confere("gap nulo nao e nem um nem outro", gap(None) == (None, None))
confere("texto em outro formato nao derruba a mensagem", gap("qualquer coisa") == (None, None))

# --- volta sem contexto da sessao -------------------------------------------
#
# year e circuito sao NOT NULL. Se v1/sessions nunca chegou (o Dataflow subiu
# no meio do replay), a volta vai para a DLQ em vez de ser inventada.

sem_contexto = [m for m in cronograma.mensagens if m.topico != "sessions"]
_linhas, _motivos = rodar(sem_contexto)
confere("volta sem v1/sessions vai para a DLQ",
        _linhas == [] and set(_motivos) == {MOTIVO_SEM_CONTEXTO})

print()
print(f"{len(falhas)} falha(s)" if falhas else "tudo certo")
sys.exit(1 if falhas else 0)
