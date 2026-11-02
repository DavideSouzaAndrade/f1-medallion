"""Chamada a API de predicao durante o processamento da volta.

Esta e a parte 3 do trabalho: o Dataflow nao e um passador de dados. Quando
a volta fecha, ainda dentro do pipeline, ele chama a API no Cloud Run e
grava a previsao na MESMA linha da volta -- e por isso que o BI consegue
mostrar previsto e real lado a lado sem join nenhum.

O modelo preve o tempo DA PROPRIA volta a partir das features dela (composto,
idade de pneu, temperatura, gap). Entao tempo_previsto_s e duracao_s na mesma
linha se referem a mesma volta N, e o erro e a subtracao direta. Nao ha
deslocamento para N+1.

Cuidados que a rede impoe num pipeline de streaming:

- a sessao HTTP e aberta uma vez por worker, no setup(), e reusada. Abrir
  conexao por evento somaria o handshake TLS a cada volta.
- timeout curto e poucas tentativas: o pipeline nao pode ficar preso numa
  chamada. O Dataflow reentrega a mensagem se o DoFn levantar excecao, e uma
  API fora do ar viraria reentrega infinita.
- falha nunca perde a volta. A linha e gravada com tempo_previsto_s nulo e a
  volta segue para o BigQuery. O painel de saude do BI mostra isso como
  predicao pendente, e a DAG de reprocessamento em lote pode completar depois.
- so a volta apta vai a API. O modelo conhece tres compostos secos e tres
  tipos de circuito; mandar uma volta de chuva renderia 422 e, num lote,
  derrubaria a predicao das outras voltas junto.
- a volta anomala tambem fica fora, e isso ja vem decidido no apto_modelo:
  ele carrega o recorte inteiro da gold_degradacao_pneu, inclusive o corte
  de 1,15x a mediana da sessao. Prever uma volta sob safety car daria
  dezenas de segundos de erro -- medido: 54,98 s na volta de bandeira
  vermelha, contra 1,32 s na volta de corrida normal.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics

log = logging.getLogger("predicao")

API = os.environ.get("API_URL", "https://f1-api-ytz2c7b2wq-uc.a.run.app").rstrip("/")

# (conectar, ler). O p95 da API quente e de poucas centenas de ms; 10 s de
# leitura cobrem uma oscilacao sem prender o pipeline.
TIMEOUT_S = (3.05, 10.0)

# A API aceita ate 500 instancias por requisicao. 50 mantem o corpo pequeno
# e ja dilui o custo da chamada entre as voltas que fecham no mesmo instante
# -- com 20 pilotos na pista, uma emissao traz ~20 voltas de uma vez.
MAX_LOTE = 50

# O que o contrato da API aceita (api/main.py). Fora disto a requisicao
# volta 422.
COMPOSTOS_DO_MODELO = frozenset({"SOFT", "MEDIUM", "HARD"})
TIPOS_DE_CIRCUITO = frozenset({"Permanent", "Temporary - Street", "Temporary - Road"})

_CAMPOS_DA_INSTANCIA = (
    "composto", "idade_pneu", "volta_no_stint", "gap_medio_a_frente",
    "temp_pista", "temp_ar", "umidade", "circuito", "circuit_type",
)


def pode_prever(linha: dict[str, Any]) -> bool:
    """A volta satisfaz o contrato de entrada da API?

    Mais estrito que apto_modelo de proposito: apto_modelo diz se a volta
    serve ao modelo, e esta funcao diz se a requisicao vai ser aceita. As
    duas quase coincidem, e a diferenca (circuit_type desconhecido,
    volta_no_stint ausente) sao casos em que a volta entra no BigQuery sem
    previsao em vez de derrubar o lote inteiro com 422.
    """
    # apto_modelo ja carrega o recorte inteiro da gold_degradacao_pneu,
    # inclusive o corte de 1,15x a mediana da sessao: a volta sob safety car
    # nao chega aqui como apta.
    if not linha.get("apto_modelo"):
        return False
    if linha.get("composto") not in COMPOSTOS_DO_MODELO:
        return False
    if linha.get("circuit_type") not in TIPOS_DE_CIRCUITO:
        return False
    if linha.get("temp_pista") is None:
        return False
    idade, no_stint, volta = (
        linha.get("idade_pneu"), linha.get("volta_no_stint"), linha.get("lap_number"),
    )
    return (
        idade is not None and idade >= 0
        and no_stint is not None and no_stint >= 1
        and volta is not None and volta >= 1
    )


def instancia(linha: dict[str, Any]) -> dict[str, Any]:
    """A volta no formato que a API espera.

    gap_medio_a_frente nulo vai nulo: a API o troca por 99.0, o mesmo valor
    do IFNULL do treino. Fazer a troca aqui duplicaria a regra.
    """
    corpo = {campo: linha.get(campo) for campo in _CAMPOS_DA_INSTANCIA}
    corpo["volta_corrida"] = linha.get("lap_number")
    return corpo


class Predizer(beam.DoFn):
    """Recebe um lote de voltas e devolve as mesmas voltas, com a previsao."""

    def __init__(self, api: str = API) -> None:
        self.api = api.rstrip("/")
        self.previstas = Metrics.counter("f1", "voltas_previstas")
        self.sem_previsao = Metrics.counter("f1", "voltas_sem_previsao")
        self.falhas = Metrics.counter("f1", "falhas_da_api")
        self.sessao = None

    def setup(self) -> None:
        # Uma sessao por worker, reusada por todas as voltas: o handshake TLS
        # custa mais que a propria predicao.
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        self.sessao = requests.Session()
        adaptador = HTTPAdapter(
            # Poucas tentativas e de proposito. Numa pipeline de streaming,
            # insistir e acumular atraso em todas as voltas seguintes; e
            # melhor gravar sem previsao e seguir.
            max_retries=Retry(
                total=2,
                backoff_factor=0.3,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"POST"}),
            ),
            pool_connections=4,
            pool_maxsize=8,
        )
        self.sessao.mount("https://", adaptador)
        self.sessao.mount("http://", adaptador)

    def teardown(self) -> None:
        if self.sessao is not None:
            self.sessao.close()

    def process(self, lote: list[dict[str, Any]]):
        aptas = [linha for linha in lote if pode_prever(linha)]
        if not aptas:
            self.sem_previsao.inc(len(lote))
            yield from lote
            return

        previsoes, modelo = self._chamar([instancia(linha) for linha in aptas])

        if previsoes is None:
            # A volta nao se perde: entra com previsao nula e o BI a mostra
            # como pendente.
            self.sem_previsao.inc(len(lote))
            yield from lote
            return

        for linha, previsao in zip(aptas, previsoes):
            linha["tempo_previsto_s"] = previsao
            linha["modelo_versao"] = modelo
        self.previstas.inc(len(aptas))
        self.sem_previsao.inc(len(lote) - len(aptas))
        yield from lote

    def _chamar(self, instancias: list[dict[str, Any]]):
        """(previsoes, versao do modelo), ou (None, None) se a API falhou.

        Nunca levanta: uma excecao aqui faria o Dataflow reentregar a
        mensagem, e uma API fora do ar viraria reentrega infinita.
        """
        try:
            resposta = self.sessao.post(
                f"{self.api}/predict",
                json={"instancias": instancias},
                timeout=TIMEOUT_S,
            )
            resposta.raise_for_status()
            corpo = resposta.json()
            previsoes = [p["tempo_previsto_s"] for p in corpo["predicoes"]]
            if len(previsoes) != len(instancias):
                raise ValueError(
                    f"API devolveu {len(previsoes)} previsoes para {len(instancias)} voltas"
                )
            return previsoes, corpo.get("modelo")
        except Exception as erro:  # noqa: BLE001 -- qualquer falha de rede ou contrato
            self.falhas.inc()
            log.warning("predicao falhou para %d volta(s): %s", len(instancias), erro)
            return None, None
