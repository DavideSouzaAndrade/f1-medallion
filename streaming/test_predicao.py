"""Predicao conferida sem tocar na rede.

Troca a sessao HTTP por uma falsa e verifica o que importa: que a previsao
cai na volta certa, que o contrato enviado a API e o que ela aceita, e --
sobretudo -- que nenhuma falha da API faz o pipeline perder uma volta.

    python streaming/test_predicao.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from montagem import AUXILIAR_ANOMALA, CAMPOS_DA_TABELA
from predicao import (
    COMPOSTOS_DO_MODELO, TIPOS_DE_CIRCUITO, Predizer, instancia, pode_prever,
)

falhas = []


def confere(nome, condicao):
    print(("ok   " if condicao else "FALHA"), nome)
    if not condicao:
        falhas.append(nome)


# Uma volta apta, como o montagem.py a entrega.
APTA = {
    "evento_id": "r:11361:44:23", "session_key": 11361, "driver_number": 44,
    "lap_number": 23, "year": 2026, "circuito": "Monza", "circuit_type": "Permanent",
    "duracao_s": 85.3, "composto": "MEDIUM", "stint_number": 2,
    "idade_pneu": 20, "volta_no_stint": 20,
    "eh_saida_box": False, "eh_entrada_box": False,
    "gap_medio_a_frente": 3.328, "temp_ar": 32.4, "temp_pista": 53.8, "umidade": 37.5,
    "apto_modelo": True, "tempo_previsto_s": None, "modelo_versao": None,
}


def volta(**mudancas):
    return {**APTA, **mudancas}


# --- quem vai a API ----------------------------------------------------------

confere("volta apta e completa vai a API", pode_prever(APTA) is True)
confere("volta inapta nao vai", pode_prever(volta(apto_modelo=False)) is False)
confere("composto de chuva nao vai", pode_prever(volta(composto="WET")) is False)
confere("composto desconhecido nao vai", pode_prever(volta(composto="UNKNOWN")) is False)
confere("circuit_type fora dos tres nao vai",
        pode_prever(volta(circuit_type="UNKNOWN")) is False)
confere("sem volta_no_stint nao vai", pode_prever(volta(volta_no_stint=None)) is False)
confere("volta_no_stint zero nao vai", pode_prever(volta(volta_no_stint=0)) is False)
confere("sem temp_pista nao vai", pode_prever(volta(temp_pista=None)) is False)
confere("sem idade_pneu nao vai", pode_prever(volta(idade_pneu=None)) is False)
confere("gap nulo ainda vai (a API troca por 99.0)",
        pode_prever(volta(gap_medio_a_frente=None)) is True)

# Volta anomala (safety car, bandeira): o modelo nao foi treinado nela.
confere("volta anomala nao vai a API",
        pode_prever(volta(**{AUXILIAR_ANOMALA: True})) is False)
confere("volta nao anomala vai",
        pode_prever(volta(**{AUXILIAR_ANOMALA: False})) is True)

# Os conjuntos tem que ser os mesmos do api/main.py.
confere("compostos do modelo sao os tres secos",
        COMPOSTOS_DO_MODELO == {"SOFT", "MEDIUM", "HARD"})
confere("tipos de circuito sao os tres do contrato",
        TIPOS_DE_CIRCUITO == {"Permanent", "Temporary - Street", "Temporary - Road"})

# --- o corpo da requisicao ---------------------------------------------------

ESPERADOS = {"composto", "idade_pneu", "volta_no_stint", "volta_corrida",
             "gap_medio_a_frente", "temp_pista", "temp_ar", "umidade",
             "circuito", "circuit_type"}
corpo = instancia(APTA)
confere("a instancia tem exatamente os campos do contrato da API",
        set(corpo) == ESPERADOS)
confere("volta_corrida e o lap_number do evento", corpo["volta_corrida"] == 23)
confere("a instancia nao leva duracao_s (seria vazar o alvo)", "duracao_s" not in corpo)
confere("a instancia nao leva evento_id nem chaves", "evento_id" not in corpo)
confere("a instancia e serializavel em JSON", json.dumps(corpo) and True)


# --- sessao HTTP falsa -------------------------------------------------------


class RespostaFalsa:
    def __init__(self, corpo, erro=None):
        self._corpo, self._erro = corpo, erro

    def raise_for_status(self):
        if self._erro:
            raise self._erro

    def json(self):
        return self._corpo


class SessaoFalsa:
    def __init__(self, resposta=None, excecao=None):
        self.resposta, self.excecao = resposta, excecao
        self.chamadas = []

    def post(self, url, json=None, timeout=None):
        self.chamadas.append({"url": url, "corpo": json, "timeout": timeout})
        if self.excecao:
            raise self.excecao
        return self.resposta

    def close(self):
        pass


def rodar(lote, sessao):
    d = Predizer("https://api.exemplo/")
    d.sessao = sessao          # no lugar do setup(), que abriria conexao
    return list(d.process(lote)), sessao


# --- caminho feliz -----------------------------------------------------------

resposta = RespostaFalsa({"modelo": "f1-degradacao-pneu@v1",
                          "predicoes": [{"tempo_previsto_s": 86.1}]})
saida, sessao = rodar([volta()], SessaoFalsa(resposta))

confere("a volta sai com a previsao", saida[0]["tempo_previsto_s"] == 86.1)
confere("a volta sai com a versao do modelo",
        saida[0]["modelo_versao"] == "f1-degradacao-pneu@v1")
confere("a previsao fica na MESMA linha da duracao real",
        saida[0]["duracao_s"] == 85.3 and saida[0]["tempo_previsto_s"] == 86.1)
confere("a URL chamada e /predict", sessao.chamadas[0]["url"] == "https://api.exemplo/predict")
confere("a requisicao tem timeout", sessao.chamadas[0]["timeout"] is not None)
confere("o corpo e {instancias: [...]}", list(sessao.chamadas[0]["corpo"]) == ["instancias"])

# --- lote misto: a previsao tem que casar com a volta certa ------------------
#
# A armadilha e deslocar as previsoes quando o lote mistura aptas e inaptas:
# a API recebe so as aptas, entao o zip e com a lista filtrada.

lote = [
    volta(lap_number=1, eh_saida_box=True, apto_modelo=False),   # inapta
    volta(lap_number=2),                                          # apta
    volta(lap_number=3, composto="WET", apto_modelo=False),       # inapta
    volta(lap_number=4),                                          # apta
]
resposta = RespostaFalsa({"modelo": "m@v1", "predicoes": [
    {"tempo_previsto_s": 88.0}, {"tempo_previsto_s": 89.0}]})
saida, sessao = rodar(lote, SessaoFalsa(resposta))
por_volta = {l["lap_number"]: l for l in saida}

confere("nenhuma volta do lote se perde", len(saida) == 4)
confere("a API recebeu so as duas aptas", len(sessao.chamadas[0]["corpo"]["instancias"]) == 2)
confere("a primeira previsao foi para a volta 2", por_volta[2]["tempo_previsto_s"] == 88.0)
confere("a segunda previsao foi para a volta 4", por_volta[4]["tempo_previsto_s"] == 89.0)
confere("a volta inapta sai sem previsao",
        por_volta[1]["tempo_previsto_s"] is None and por_volta[3]["tempo_previsto_s"] is None)
confere("a volta inapta sai sem versao de modelo",
        por_volta[1]["modelo_versao"] is None)

# --- a volta anomala atravessa, so sem previsao -----------------------------

lote = [volta(lap_number=10), volta(lap_number=11, **{AUXILIAR_ANOMALA: True})]
resposta = RespostaFalsa({"modelo": "m@v1", "predicoes": [{"tempo_previsto_s": 86.4}]})
saida, sessao = rodar(lote, SessaoFalsa(resposta))
por_volta = {l["lap_number"]: l for l in saida}

confere("a API recebeu so a volta normal",
        len(sessao.chamadas[0]["corpo"]["instancias"]) == 1)
confere("a volta normal recebe previsao", por_volta[10]["tempo_previsto_s"] == 86.4)
confere("a volta anomala e gravada sem previsao",
        por_volta[11]["tempo_previsto_s"] is None)
confere("a volta anomala conserva a duracao real", por_volta[11]["duracao_s"] == 85.3)

# A auxiliar nao pode chegar ao BigQuery: campo fora do schema derruba a
# linha inteira. Quem a remove e o pipeline, com as colunas da tabela.
confere("a auxiliar sai da linha ao projetar as colunas da tabela",
        AUXILIAR_ANOMALA not in {campo: None for campo in CAMPOS_DA_TABELA})
confere("tempo_previsto_s e modelo_versao SAO colunas da tabela",
        "tempo_previsto_s" in CAMPOS_DA_TABELA and "modelo_versao" in CAMPOS_DA_TABELA)

# --- falhas: a volta NUNCA se perde -----------------------------------------

for nome, sessao_ruim in (
    ("rede caiu", SessaoFalsa(excecao=OSError("connection reset"))),
    ("timeout", SessaoFalsa(excecao=TimeoutError("read timeout"))),
    ("500 da API", SessaoFalsa(RespostaFalsa(None, erro=RuntimeError("500")))),
    ("JSON invalido", SessaoFalsa(RespostaFalsa({"inesperado": True}))),
    ("predicoes a menos", SessaoFalsa(RespostaFalsa({"modelo": "m", "predicoes": []}))),
):
    saida, _ = rodar([volta(lap_number=7), volta(lap_number=8)], sessao_ruim)
    confere(f"{nome}: as duas voltas sao gravadas", len(saida) == 2)
    confere(f"{nome}: previsao nula, volta intacta",
            all(l["tempo_previsto_s"] is None and l["duracao_s"] == 85.3 for l in saida))

# Lote inteiro inapto nem chama a API.
saida, sessao = rodar([volta(apto_modelo=False)], SessaoFalsa())
confere("lote sem volta apta nao chama a API", sessao.chamadas == [])
confere("lote sem volta apta ainda grava a volta", len(saida) == 1)

print()
print(f"{len(falhas)} falha(s)" if falhas else "tudo certo")
sys.exit(1 if falhas else 0)
