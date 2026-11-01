"""Montagem da volta a partir dos registros brutos, sem rede.

Este modulo e a silver_voltas em streaming. A versao em lote
(definitions/silver/silver_voltas.sqlx) ve a corrida inteira de uma vez e
resolve tudo com janela e join; aqui os registros chegam um a um, fora de
ordem e em varias versoes, e a volta tem que ser montada incrementalmente.

O estado e um dicionario comum, serializavel: quem o guarda entre mensagens
e o pipeline.py, no estado do Beam. Nada aqui toca em rede nem depende do
Beam, e e isso que deixa a regra testavel offline (test_montagem.py).

O que o estado NAO guarda, de proposito:

- os intervals. Sao 80% do volume (21.955 por corrida) e servem a uma unica
  coluna, o gap medio da volta. Guardar soma e contagem por volta aberta da
  a mesma media em O(1) e deixa o estado da sessao inteira em ~15 KB.
- o historico de clima. A silver_voltas usa o clima vigente no inicio da
  volta, entao basta a ultima leitura e um retrato quando a volta nasce.
- as voltas ja emitidas. Saem do estado assim que viram linha.

Divergencias inevitaveis em relacao ao lote, pela natureza do ao vivo:

- razao_mediana_sessao e volta_limpa nao existem aqui: dependem da mediana
  da corrida inteira, que ninguem sabe antes do fim. Nao estao no schema de
  f1_stream.voltas.
- volta_no_stint conta as voltas aptas vistas ate agora no stint. A
  gold_degradacao_pneu numera as voltas que passam pelo filtro dela, que
  inclui razao_mediana_sessao <= 1.15 -- indisponivel ao vivo.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from contrato import MOTIVO_SEM_CONTEXTO, MensagemInvalida

# Quanto esperar, depois de a volta fechar, por registros atrasados. O
# Pub/Sub nao garante ordem: a versao final da volta pode chegar antes do
# stint dela. Tres segundos sao folgados no fator 30, em que uma volta de 85
# s cabe em 2,8 s de replay, e cabem no limite de latencia p95 que o painel
# de saude do BI cobra (10 s).
GRACA_S = 3.0

# Carencia de quem ja tem tudo. Quando stint, clima e cobertura de intervals
# ja chegaram, nao ha atrasado para esperar e a volta pode sair quase na
# hora -- derruba o piso de latencia de ~3 s para ~1 s.
#
# Nao e zero: a reordenacao do Pub/Sub e sub-segundo (medido: ~0,18 s a 110
# mensagens/s), e 1 s da ~5x de margem para a medicao de gap que ainda
# estiver em voo dentro da janela. E a troca que a alavanca faz -- menos
# latencia, menos margem de reordenacao.
GRACA_COMPLETA_S = 1.0

# Carencia de quem ainda NAO fechou, quando a volta seguinte comeca.
#
# No lote, a janela da volta termina no inicio da seguinte (o LEAD da
# silver_voltas). Usar isso como gatilho de emissao foi o primeiro desenho e
# estava errado: fora de ordem, a volta seguinte chega antes da versao final
# da anterior, e a volta saia sem duracao_s e sem composto. Medido num replay
# real: 11 de 64 voltas sem tempo e 18 sem composto.
#
# O inicio da volta seguinte agora so fecha a janela dos intervals. Para
# emitir, espera-se a versao final (lap_duration) ou esta carencia longa --
# que e a saida das 3 voltas por corrida que nunca ganham lap_duration.
GRACA_SEM_TEMPO_S = 30.0

# Rede de seguranca: volta que nunca fecha (sem lap_duration e sem volta
# seguinte -- a ultima de cada piloto, ou um abandono) sai do estado depois
# disto, com o que tiver. Sem isto ela ficaria no estado para sempre.
ABANDONO_S = 600.0

# Mesmo recorte da gold_degradacao_pneu: os de chuva tem outra fisica e
# UNKNOWN/TEST_UNKNOWN nao identificam o pneu.
COMPOSTOS_SECOS = frozenset({"SOFT", "MEDIUM", "HARD"})

# Ordem das colunas de f1_stream.voltas. As duas da predicao entram no
# predicao.py; aqui saem nulas.
CAMPOS_DA_TABELA = (
    "evento_id", "replay_id", "ts_evento",
    "session_key", "driver_number", "lap_number", "year",
    "circuito", "circuit_type",
    "data_inicio", "duracao_s", "setor_1_s", "setor_2_s", "setor_3_s",
    "composto", "stint_number", "idade_pneu", "volta_no_stint",
    "eh_saida_box", "eh_entrada_box",
    "gap_medio_a_frente", "temp_ar", "temp_pista", "umidade", "chuva",
    "apto_modelo", "_ts_processamento", "latencia_ms",
    "tempo_previsto_s", "modelo_versao",
)

_CAMPOS_CLIMA = ("temp_ar", "temp_pista", "umidade", "chuva")

# Chave auxiliar que viaja com a linha entre a montagem e a predicao, e sai
# antes da escrita. Nao e coluna de f1_stream.voltas.
AUXILIAR_ANOMALA = "_anomala"

# Quantas voltas emitidas lembrar por piloto, para barrar reentrega. Uma
# corrida tem ~60 voltas, entao na pratica guarda a corrida inteira.
_MAX_EMITIDAS = 100

# Corte de volta anomala, o mesmo da gold_degradacao_pneu: safety car,
# bandeira amarela, problema mecanico. Mais frouxo que o 1,07 da Silver de
# proposito -- o corte apertado descartaria justamente as voltas mais
# degradadas, que sao o fenomeno que o modelo quer capturar.
#
# A diferenca inevitavel com o lote: a Gold divide pela mediana da corrida
# inteira, e ao vivo ninguem a sabe antes do fim. Aqui a mediana e das
# voltas recebidas ate o momento, como o contrato v2 preve.
RAZAO_MAXIMA = 1.15

# Teto de duracoes guardadas para a mediana corrente. Uma corrida tem ~1.100
# voltas; 600 ja da uma mediana estavel e limita o custo da ordenacao.
_MAX_DURACOES = 600

# Teto de medicoes de gap por piloto. A poda normal e por tempo, em
# remover(); este teto so protege o estado se nenhuma volta fechar.
_MAX_INTERVALS = 2000


# --- estado ------------------------------------------------------------------


def estado_novo() -> dict[str, Any]:
    return {
        # year e circuito vem de v1/sessions, circuit_type de v1/meetings.
        # Nenhum dos tres existe no registro da volta, e os tres sao NOT
        # NULL na tabela: sem eles a volta nao pode ser gravada.
        "contexto": {"year": None, "circuito": None, "circuit_type": None},
        # Leituras de clima da sessao, em ordem de medicao. A volta usa a
        # vigente no inicio dela (o as-of join da silver_voltas), resolvido
        # na emissao e nao na chegada: a leitura certa pode chegar depois da
        # volta que a usa, e o Pub/Sub nao garante ordem.
        "clima": [],
        # Duracoes das voltas ja emitidas, para a mediana corrente da
        # sessao. E ela que identifica a volta anomala.
        "duracoes": [],
        # driver_number (como texto, para o estado ser serializavel) -> piloto
        "pilotos": {},
    }


def _piloto(estado: dict[str, Any], numero: int) -> dict[str, Any]:
    return estado["pilotos"].setdefault(
        str(numero),
        {
            "stints": {},   # stint_number -> ultima versao
            "pit": {},      # lap_number -> duracao_box_s
            "voltas": {},   # lap_number -> volta em montagem
            # Medicoes de gap recentes, em ordem de medicao. A media da
            # volta e calculada na emissao, sobre a janela dela: atribuir a
            # medicao a volta na CHEGADA tornava a media dependente da ordem
            # de entrega, que o Pub/Sub nao garante.
            #
            # Nao e o historico da corrida: a poda em remover() deixa so o
            # que ainda pode cair numa volta aberta, ~2 voltas por piloto.
            "intervals": [],
            # stint_number -> voltas aptas ja numeradas, para o
            # volta_no_stint nao depender da ordem de emissao.
            "aptas": {},
            # Voltas ja emitidas. Uma reentrega do Pub/Sub depois da
            # emissao nao pode ressuscitar a volta e gravar uma segunda
            # linha: com isto o evento_id e unico de verdade por replay.
            #
            # Um conjunto, e nao a maior volta emitida: com marca d'agua,
            # uma volta que saisse fora de ordem barraria todas as
            # anteriores, e elas se perderiam em silencio.
            "emitidas": [],
        },
    )


def _epoch(instante: datetime | None) -> float | None:
    return None if instante is None else instante.timestamp()


def _data(epoch: float | None) -> datetime | None:
    return None if epoch is None else datetime.fromtimestamp(epoch, timezone.utc)


# --- entrada de registros ----------------------------------------------------


def aplicar(estado: dict[str, Any], registro: dict[str, Any],
            publicado_em: datetime, agora: float) -> None:
    """Incorpora um registro tipado ao estado da sessao.

    `publicado_em` e o publish_time da mensagem no Pub/Sub, que o contrato
    define como a referencia de latencia. `agora` e o relogio de
    processamento, em epoch, usado para os prazos.
    """
    topico = registro["_topico"]

    if topico == "sessions":
        estado["contexto"]["year"] = registro["year"]
        estado["contexto"]["circuito"] = registro["circuito"]
    elif topico == "meetings":
        estado["contexto"]["circuit_type"] = registro["circuit_type"]
    elif topico == "weather":
        _clima(estado, registro)
    elif topico == "stints":
        _stint(estado, registro)
    elif topico == "pit":
        _pit(estado, registro)
    elif topico == "laps":
        _volta(estado, registro, publicado_em, agora)
    elif topico == "intervals":
        _interval(estado, registro)


# Teto de leituras de clima guardadas. Sao ~176 por corrida, uma por minuto,
# entao na pratica nunca poda: o teto existe para o estado nao crescer sem
# limite num replay muito longo.
_MAX_CLIMA = 400


def _clima(estado: dict[str, Any], registro: dict[str, Any]) -> None:
    quando = _epoch(registro["data"])
    if quando is None:
        return
    leituras = estado["clima"]
    # Ordenado pela data da medicao, nao pela ordem de chegada: e por data
    # que a volta procura a leitura vigente.
    posicao = len(leituras)
    while posicao > 0 and leituras[posicao - 1]["data"] > quando:
        posicao -= 1
    if posicao > 0 and leituras[posicao - 1]["data"] == quando:
        return  # mesma medicao reentregue
    leituras.insert(posicao, {"data": quando, **{c: registro.get(c) for c in _CAMPOS_CLIMA}})
    if len(leituras) > _MAX_CLIMA:
        del leituras[: len(leituras) - _MAX_CLIMA]


def _clima_em(estado: dict[str, Any], quando: float | None) -> dict[str, Any]:
    """A leitura de clima vigente no instante dado.

    A ultima medicao feita ATE aquele instante, que e o as-of join da
    silver_voltas (b.data_inicio >= c.clima_ini). Sem leitura anterior o
    clima fica nulo, como o LEFT JOIN do lote faria: usar a leitura seguinte
    seria usar o futuro.
    """
    vazio = {c: None for c in _CAMPOS_CLIMA}
    if quando is None:
        return vazio
    vigente = vazio
    for leitura in estado["clima"]:
        if leitura["data"] > quando:
            break
        vigente = {c: leitura.get(c) for c in _CAMPOS_CLIMA}
    return vigente


def _stint(estado: dict[str, Any], registro: dict[str, Any]) -> None:
    # O stint ganha uma versao nova a cada volta, com lap_end avancando.
    # Mesmo _key = mesmo stint: fica a de maior _id.
    stints = _piloto(estado, registro["driver_number"])["stints"]
    chave = str(registro["stint_number"])
    anterior = stints.get(chave)
    if anterior is not None and registro["_id"] <= anterior["_id"]:
        return
    stints[chave] = {
        "_id": registro["_id"],
        "stint_number": registro["stint_number"],
        "lap_start": registro["lap_start"],
        "lap_end": registro["lap_end"],
        "composto": registro["composto"],
        "idade_pneu_inicio": registro["idade_pneu_inicio"],
    }


def _pit(estado: dict[str, Any], registro: dict[str, Any]) -> None:
    # O endpoint laps so traz a flag de SAIDA do box; a entrada se deduz da
    # existencia de um registro de pit naquela volta, como na silver_voltas.
    _piloto(estado, registro["driver_number"])["pit"][str(registro["lap_number"])] = \
        registro["duracao_box_s"]


def _volta(estado: dict[str, Any], registro: dict[str, Any],
           publicado_em: datetime, agora: float) -> None:
    piloto = _piloto(estado, registro["driver_number"])
    voltas = piloto["voltas"]
    numero = registro["lap_number"]
    chave = str(numero)

    if numero in piloto["emitidas"]:
        return  # volta ja emitida: reentrega nao a ressuscita

    atual = voltas.get(chave)
    if atual is not None and registro["_id"] <= atual["_id"]:
        return  # versao antiga, ou reentrega do Pub/Sub

    if atual is None:
        atual = {
            "lap_number": numero,
            "ts_evento": None,
            "prazo": None,
            "limite": agora + ABANDONO_S,
        }
        voltas[chave] = atual
        # A janela de intervals da volta anterior fecha aqui, como o LEAD da
        # silver_voltas. Mas a EMISSAO dela espera a versao final: so se
        # ela nunca chegar e que a carencia longa a solta, com o que tiver.
        anterior = voltas.get(str(numero - 1))
        if anterior is not None and anterior["prazo"] is None:
            anterior["prazo"] = agora + GRACA_SEM_TEMPO_S
            anterior["ts_evento"] = anterior["ts_evento"] or publicado_em.timestamp()

    # As versoes intermediarias so preenchem campos: cada uma e um
    # superconjunto da anterior (producer/cronograma.py, _parcial). Entao
    # substituir e seguro e nao perde nada -- o test_montagem.py verifica
    # essa propriedade, que e o que autoriza nao fazer merge campo a campo.
    atual["_id"] = registro["_id"]
    atual["data_inicio"] = _epoch(registro["data_inicio"])
    atual["duracao_s"] = registro["duracao_s"]
    atual["setor_1_s"] = registro["setor_1_s"]
    atual["setor_2_s"] = registro["setor_2_s"]
    atual["setor_3_s"] = registro["setor_3_s"]
    atual["eh_saida_box"] = bool(registro["eh_saida_box"])

    # lap_duration so aparece na versao final: a volta fechou. Dai conta a
    # carencia para os atrasados -- ou nada, se nao houver atrasado nenhum.
    # O min() antecipa um prazo longo que a volta seguinte ja tenha marcado:
    # e o caso comum, e sem isto a latencia seria a carencia longa sempre.
    if registro["duracao_s"] is not None:
        atual["ts_evento"] = publicado_em.timestamp()
        proximo = agora + (GRACA_COMPLETA_S if _completa(estado, piloto, atual) else GRACA_S)
        atual["prazo"] = proximo if atual["prazo"] is None else min(atual["prazo"], proximo)


def _completa(estado: dict[str, Any], piloto: dict[str, Any],
              volta: dict[str, Any]) -> bool:
    """Ja chegou tudo o que a volta precisa?

    A carencia existe para esperar dado atrasado. Quando nada esta
    atrasado, esperar so adiciona latencia -- e no caso comum nada esta: o
    stint sai no inicio da volta, o clima ja vigia, e os intervals sao
    medidos durante ela. Confirmado isso, a volta sai na hora.

    Nao e um palpite: cada condicao e a presenca efetiva do dado que
    alimenta uma coluna. A cobertura dos intervals ate o fim da volta e a
    mais exigente, e e justamente a que protege a media do gap -- a coluna
    que mais sofria quando eu decidia na chegada.
    """
    inicio, duracao = volta.get("data_inicio"), volta.get("duracao_s")
    if inicio is None or duracao is None:
        return False
    if _stint_da_volta(piloto["stints"], volta["lap_number"]) is None:
        return False
    if not any(leitura["data"] <= inicio for leitura in estado["clima"]):
        return False
    # Medicao de gap alcancando o fim da volta: o que vinha depois dela
    # pertence a volta seguinte.
    return any(m["data"] >= inicio + duracao for m in piloto["intervals"])


def _interval(estado: dict[str, Any], registro: dict[str, Any]) -> None:
    """Guarda a medicao de gap, pela data em que foi medida.

    Nao decide a que volta ela pertence: isso e feito na emissao, por
    janela, como o join da silver_voltas. Decidir na chegada fazia a media
    depender da ordem de entrega -- a medicao que chegasse antes da volta
    que a contem ia para a volta anterior, ou se perdia.
    """
    quando = _epoch(registro["data"])
    if quando is None:
        return
    medicoes = _piloto(estado, registro["driver_number"])["intervals"]
    posicao = len(medicoes)
    while posicao > 0 and medicoes[posicao - 1]["data"] > quando:
        posicao -= 1
    medicoes.insert(posicao, {
        "data": quando,
        "gap": registro["gap_frente_s"],
        "atras": registro.get("voltas_atras"),
    })
    if len(medicoes) > _MAX_INTERVALS:
        del medicoes[: len(medicoes) - _MAX_INTERVALS]


def _trafego(piloto: dict[str, Any], inicio: float | None,
             fim: float | None) -> tuple[float | None, int | None]:
    """Media do gap e voltas atras na janela da volta.

    Mesma janela do lote: [inicio da volta, inicio da seguinte). Gap nulo e
    gap em "+N LAPS" ficam fora da media, como na silver_voltas.
    """
    if inicio is None:
        return (None, None)
    soma, n, atras = 0.0, 0, None
    for medicao in piloto["intervals"]:
        if medicao["data"] < inicio:
            continue
        if fim is not None and medicao["data"] >= fim:
            break
        if medicao["gap"] is not None:
            soma += medicao["gap"]
            n += 1
        if medicao["atras"] is not None:
            atras = max(medicao["atras"], atras or 0)
    return (round(soma / n, 3) if n else None, atras)


def _fim_da_janela(piloto: dict[str, Any], volta: dict[str, Any]) -> float | None:
    """O fim da janela da volta, igual ao COALESCE do lote.

    O inicio da volta seguinte, se ela ja chegou; senao o inicio mais a
    duracao. Sem os dois, a janela fica aberta: e a ultima volta do piloto,
    e a silver_voltas tambem a fecha pela duracao.
    """
    inicio = volta.get("data_inicio")
    seguinte = piloto["voltas"].get(str(volta["lap_number"] + 1))
    if seguinte is not None and seguinte.get("data_inicio") is not None:
        return seguinte["data_inicio"]
    if inicio is not None and volta.get("duracao_s") is not None:
        return inicio + volta["duracao_s"]
    return None


# --- saida de voltas ---------------------------------------------------------


def tem_contexto(estado: dict[str, Any]) -> bool:
    """O contexto da sessao ja chegou? year e circuito sao NOT NULL."""
    return estado["contexto"]["year"] is not None and estado["contexto"]["circuito"] is not None


def vencidas(estado: dict[str, Any], agora: float) -> list[tuple[int, int]]:
    """(piloto, volta) das voltas prontas para emitir, em ordem de volta.

    Sem o contexto da sessao nada e emitido pelo prazo normal: a volta
    espera. v1/sessions e v1/meetings saem no inicio do replay, mas o
    Pub/Sub nao garante ordem, e mandar a volta para a DLQ porque a mensagem
    de contexto ficou dois segundos atras seria perder dado por nada. So o
    prazo de abandono vence a espera -- ai sim a volta vai para a DLQ.
    """
    espera_contexto = not tem_contexto(estado)
    prontas = []
    for numero, piloto in estado["pilotos"].items():
        # Em ordem de volta, parando na primeira que ainda nao venceu: as
        # voltas de um piloto saem na ordem em que ele as rodou.
        #
        # Nao e esmero: volta_no_stint e a posicao da volta entre as voltas
        # aptas do stint, e so da para saber isso tendo decidido as
        # anteriores. Emitir fora de ordem dava o numero da anterior a volta
        # seguinte. O prazo de abandono impede que uma volta travada segure
        # as outras para sempre.
        for volta in sorted(piloto["voltas"].values(), key=lambda v: v["lap_number"]):
            prazo = volta["prazo"]
            no_prazo = prazo is not None and prazo <= agora and not espera_contexto
            if not (no_prazo or volta["limite"] <= agora):
                break
            prontas.append((int(numero), volta["lap_number"]))
    # Por volta antes de por piloto, para a saida ser estavel.
    return sorted(prontas, key=lambda p: (p[1], p[0]))


def proximo_prazo(estado: dict[str, Any]) -> float | None:
    """O instante do prazo mais proximo, para reagendar o timer do Beam."""
    prazos = [
        p
        for piloto in estado["pilotos"].values()
        for volta in piloto["voltas"].values()
        for p in (volta["prazo"], volta["limite"])
        if p is not None
    ]
    return min(prazos) if prazos else None


def _stint_da_volta(stints: dict[str, Any], numero: int) -> dict[str, Any] | None:
    """O stint a que a volta pertence, com o desempate da silver_voltas.

    Uma volta pode casar com mais de um stint: na origem o lap_end de um e o
    lap_start do seguinte, porque a volta do pit stop e rodada com o pneu
    velho e termina com o novo. Desempate identico ao do lote -- descarta o
    stint fantasma (composto UNKNOWN ou nulo, que a API emite para quem nao
    participou) e fica com o pneu mais recente.
    """
    candidatos = [
        s for s in stints.values()
        if s["lap_start"] <= numero <= s["lap_end"]
    ]
    if not candidatos:
        return None
    return min(
        candidatos,
        key=lambda s: (
            0 if s["composto"] and s["composto"] != "UNKNOWN" else 1,
            -s["stint_number"],
        ),
    )


def _mediana(valores: list[float]) -> float | None:
    if not valores:
        return None
    ordenados = sorted(valores)
    meio = len(ordenados) // 2
    if len(ordenados) % 2:
        return ordenados[meio]
    return (ordenados[meio - 1] + ordenados[meio]) / 2


def eh_anomala(estado: dict[str, Any], duracao: float | None) -> bool:
    """A volta esta fora da faixa de volta aproveitavel da sessao?

    Compara com a mediana das voltas recebidas ate agora, no mesmo corte da
    gold_degradacao_pneu. Serve para nao pedir previsao de uma volta sob
    safety car ou bandeira: o modelo foi treinado sem elas, e prever ali
    produziria um erro de dezenas de segundos que poluiria o painel.

    Sem mediana ainda -- as primeiras voltas da sessao -- nada e anomalo: o
    benefico da duvida fica com a volta.
    """
    mediana = _mediana(estado["duracoes"])
    if duracao is None or mediana is None or mediana <= 0:
        return False
    return duracao / mediana > RAZAO_MAXIMA


def eh_apto_para_modelo(linha: dict[str, Any]) -> bool:
    """A volta serve para o modelo de degradacao?

    Mesmo recorte da gold_degradacao_pneu. A volta inapta e gravada mesmo
    assim, apenas marcada: a filosofia da Silver e tratar sem filtrar, e
    quem consome decide.
    """
    if linha.get("composto") not in COMPOSTOS_SECOS:
        return False
    if any(linha.get(c) is None for c in ("duracao_s", "idade_pneu", "temp_pista")):
        return False
    return not (linha.get("eh_saida_box") or linha.get("eh_entrada_box"))


def montar(estado: dict[str, Any], session_key: int, replay_id: str,
           numero_piloto: int, numero_volta: int, processado_em: datetime) -> dict[str, Any]:
    """A volta pronta como linha de f1_stream.voltas.

    Levanta MensagemInvalida se o contexto da sessao nunca chegou: year,
    circuito e circuit_type sao NOT NULL na tabela, e inventar valor para
    eles seria pior do que mandar a volta para a DLQ.
    """
    piloto = estado["pilotos"][str(numero_piloto)]
    volta = piloto["voltas"][str(numero_volta)]
    contexto = estado["contexto"]

    if contexto["year"] is None or contexto["circuito"] is None:
        raise MensagemInvalida(MOTIVO_SEM_CONTEXTO)

    stint = _stint_da_volta(piloto["stints"], numero_volta)
    stint_valido = bool(stint and stint["composto"] and stint["composto"] != "UNKNOWN")

    idade_pneu = None
    if stint_valido and stint["idade_pneu_inicio"] is not None:
        # Idade ao FINAL da volta: +1 porque a propria volta conta. Identico
        # a silver_voltas.
        idade_pneu = stint["idade_pneu_inicio"] + (numero_volta - stint["lap_start"]) + 1

    linha: dict[str, Any] = {
        "evento_id": f"{replay_id}:{session_key}:{numero_piloto}:{numero_volta}",
        "replay_id": replay_id,
        "session_key": session_key,
        "driver_number": numero_piloto,
        "lap_number": numero_volta,
        "year": contexto["year"],
        "circuito": contexto["circuito"],
        # NOT NULL na tabela. v1/meetings costuma chegar antes de tudo, mas
        # se faltar o circuit_type a volta nao se perde por isso: ela entra
        # marcada, e o predicao.py nao chama a API para ela, porque o modelo
        # so conhece os tres tipos reais.
        "circuit_type": contexto["circuit_type"] or "UNKNOWN",
        "data_inicio": _data(volta.get("data_inicio")),
        "duracao_s": volta.get("duracao_s"),
        "setor_1_s": volta.get("setor_1_s"),
        "setor_2_s": volta.get("setor_2_s"),
        "setor_3_s": volta.get("setor_3_s"),
        "composto": stint["composto"] if stint else None,
        "stint_number": stint["stint_number"] if stint else None,
        "idade_pneu": idade_pneu,
        "eh_saida_box": bool(volta.get("eh_saida_box")),
        "eh_entrada_box": str(numero_volta) in piloto["pit"],
        "gap_medio_a_frente": _trafego(
            piloto, volta.get("data_inicio"), _fim_da_janela(piloto, volta)
        )[0],
        # Resolvido agora, na emissao: a leitura vigente no inicio da volta
        # pode ter chegado depois da propria volta.
        **_clima_em(estado, volta.get("data_inicio")),
        "tempo_previsto_s": None,
        "modelo_versao": None,
    }

    linha["apto_modelo"] = eh_apto_para_modelo(linha)

    # volta_no_stint: posicao da volta entre as voltas aptas do stint, como
    # a gold_degradacao_pneu numera. So a volta apta recebe numero, porque
    # so ela existe na tabela de treino -- e so ela vai a API.
    if linha["apto_modelo"] and stint:
        chave = str(stint["stint_number"])
        aptas = piloto["aptas"].setdefault(chave, [])
        # Posicao entre as voltas aptas do stint, e nao um contador de
        # emissoes: se uma volta sair fora de ordem, o contador daria a ela
        # o numero da seguinte. Aqui a posicao vem do proprio lap_number.
        linha["volta_no_stint"] = sum(1 for n in aptas if n < numero_volta) + 1
        if numero_volta not in aptas:
            aptas.append(numero_volta)
    else:
        linha["volta_no_stint"] = None

    ts_evento = _data(volta.get("ts_evento")) or processado_em
    linha["ts_evento"] = ts_evento
    linha["_ts_processamento"] = processado_em
    linha["latencia_ms"] = int((processado_em - ts_evento).total_seconds() * 1000)

    anomala = eh_anomala(estado, linha["duracao_s"])
    if linha["duracao_s"] is not None:
        estado["duracoes"].append(linha["duracao_s"])
        if len(estado["duracoes"]) > _MAX_DURACOES:
            del estado["duracoes"][: len(estado["duracoes"]) - _MAX_DURACOES]

    saida = {campo: linha.get(campo) for campo in CAMPOS_DA_TABELA}
    # Auxiliar, nao e coluna: diz ao predicao.py para nao pedir previsao
    # desta volta. O pipeline a remove antes de gravar, porque o BigQuery
    # recusa a linha com campo que nao existe no schema.
    saida[AUXILIAR_ANOMALA] = anomala
    return saida


def remover(estado: dict[str, Any], numero_piloto: int, numero_volta: int) -> None:
    """Tira a volta do estado depois de emitida.

    E o que mantem o estado pequeno: so as voltas abertas ficam. O stint
    permanece, porque as voltas seguintes ainda precisam dele.
    """
    piloto = estado["pilotos"].get(str(numero_piloto))
    if piloto:
        piloto["voltas"].pop(str(numero_volta), None)
        # O registro de pit so serve a volta dele.
        piloto["pit"].pop(str(numero_volta), None)
        # Barra a ressurreicao da volta por uma reentrega tardia. Vale
        # tambem para a volta que foi para a DLQ. Poda pelo fim: uma
        # reentrega chega em segundos, nao cem voltas depois.
        piloto["emitidas"].append(numero_volta)
        if len(piloto["emitidas"]) > _MAX_EMITIDAS:
            del piloto["emitidas"][: len(piloto["emitidas"]) - _MAX_EMITIDAS]
        _podar_intervals(piloto)


def _podar_intervals(piloto: dict[str, Any]) -> None:
    """Descarta as medicoes de gap que nenhuma volta aberta pode mais usar.

    E o que mantem o buffer em ~2 voltas por piloto em vez do historico da
    corrida: 80% do volume de mensagens passa por aqui e nao fica.
    """
    inicios = [v["data_inicio"] for v in piloto["voltas"].values()
               if v.get("data_inicio") is not None]
    if not inicios:
        piloto["intervals"].clear()
        return
    corte = min(inicios)
    piloto["intervals"] = [m for m in piloto["intervals"] if m["data"] >= corte]
