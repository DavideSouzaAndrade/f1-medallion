"""DAG principal: liga o pipeline de ponta a ponta e o desliga no fim.

A ordem das tarefas nao e arbitraria -- cada aresta vem de uma medicao:

    aquecer_f1_api ──▶ subir_dataflow ──▶ esperar_dataflow_pronto
                                                  │
                                                  ▼
                                          disparar_gerador
                                                  │
                                                  ▼
                                       esperar_stream_saudavel
                                                  │
                                                  ▼
                                           drenar_dataflow

1. `aquecer_f1_api` PRIMEIRO. A API carrega um SavedModel do TensorFlow: 33 s
   de container, e o Cloud Run pode levar mais de um minuto so para alocar a
   instancia. O timeout de leitura do Dataflow e de 10 s, entao publicar com
   a API fria faria as primeiras voltas serem gravadas sem previsao -- e e
   justamente a previsao que se quer demonstrar.

2. `subir_dataflow` ANTES do gerador, e nao em paralelo. A assinatura retem
   as mensagens por 7 dias, entao nada se perde publicando antes; o problema
   e outro: o backlog inteiro seria entregue de uma vez, a reordenacao
   passaria da carencia de 1 s e a media de gap da volta sairia degradada.
   Com o job de pe, as mensagens chegam no ritmo do replay.

3. `esperar_dataflow_pronto` existe porque "Running" no Dataflow nao quer
   dizer "worker consumindo": entre um e outro ha de 2 a 4 minutos de
   provisionamento de VM e instalacao de dependencias.

4. `drenar_dataflow` SEMPRE, inclusive se algo acima falhar. O job cobra por
   hora de worker ligado, tenha mensagem ou nao, e nao escala a zero como o
   Cloud Run. Esta e a tarefa que nao pode ser esquecida -- por isso a regra
   de disparo e ALL_DONE.

O gerador nao devolve o replay_id ao Airflow, entao nenhuma tarefa espera
"o replay X": a vigilancia olha se HA evento recente, como o contrato da API
de BI (bi-api/AIRFLOW.md) recomenda.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

PROJETO = os.environ.get("PROJECT_ID", "pdm-davi-2026")
REGIAO = os.environ.get("REGIAO", "us-central1")

# Onde o repositorio esta montado dentro do container do Airflow. O
# docker-compose.yml monta a raiz do projeto aqui.
RAIZ = os.environ.get("F1_RAIZ", "/opt/f1-medallion")

# Prefixo do nome do job no Dataflow. O streaming/pipeline.py o usa ao
# nomear o job, e o parar.sh procura por ele: e o contrato entre a DAG e o
# pipeline.
PREFIXO_JOB = "f1-openf1-"

# Espaco no fim de todo bash_command que termina em .sh.
#
# Nao e enfeite: BashOperator.template_ext inclui ".sh", e o Airflow trata
# um bash_command terminado em .sh como CAMINHO DE ARQUIVO de template, nao
# como comando. Sem o espaco, a tarefa morre no carregamento com
# "jinja2.exceptions.TemplateNotFound: cd /opt/f1-medallion && bash
# streaming/subir.sh". E o workaround que a propria documentacao do Airflow
# recomenda -- nao remova.
FIM = " "

SESSAO = "{{ dag_run.conf.get('session_key', '11361') }}"
INVALIDOS = "{{ dag_run.conf.get('injetar_invalidos', '0.01') }}"

# Fator 10, e nao os 30 do gerador. A corrida 11361 tem 111 minutos, entao:
#
#     fator 30 -> 4,7 min de replay, ~95 mensagens/s
#     fator 10 -> 14,2 min de replay, ~32 mensagens/s
#
# Catorze minutos dao tempo de abrir o painel, mostrar as voltas entrando,
# explicar a predicao e responder pergunta. Cinco nao dao -- e o ritmo mais
# baixo ainda folga a reordenacao, que e o que a carencia precisa absorver.
FATOR = "{{ dag_run.conf.get('fator', '10') }}"

# Vazio por padrao: a corrida INTEIRA. Cortar o replay deixava a ultima
# volta de cada piloto sem tempo -- ela comecava e o replay acabava no meio
# dela -- e isso aparecia no painel como 21 voltas estranhas sem duracao.
MINUTOS = "{{ dag_run.conf.get('minutos', '') }}"

padrao = {
    "owner": "frente-b",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "f1_pipeline_fim_a_fim",
    description="Aquece a API, sobe o Dataflow, reproduz a corrida e drena o job",
    start_date=datetime(2026, 11, 1),
    # Sob demanda: cada execucao custa hora de worker do Dataflow. Quem
    # dispara e a apresentacao ou um teste, nao um relogio.
    schedule=None,
    catchup=False,
    default_args=padrao,
    tags=["f1", "streaming"],
    doc_md=__doc__,
) as dag:

    # A requisicao de verdade, e nao um GET /health: o que leva 33 s e
    # carregar o SavedModel, e isso so acontece na primeira predicao.
    aquecer_f1_api = HttpOperator(
        task_id="aquecer_f1_api",
        http_conn_id="f1_api",
        endpoint="predict",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=(
            '{"instancias": [{"composto": "MEDIUM", "idade_pneu": 20,'
            ' "volta_no_stint": 20, "volta_corrida": 23,'
            ' "gap_medio_a_frente": 3.3, "temp_pista": 53.8, "temp_ar": 32.4,'
            ' "umidade": 37.5, "circuito": "Monza", "circuit_type": "Permanent"}]}'
        ),
        # Partida a frio medida: 37 s de container e ate 2 min de alocacao.
        extra_options={"timeout": 240},
        retries=2,
        retry_delay=timedelta(seconds=20),
    )

    # Nao ha operador de Dataflow para pipeline Beam em Python com --setup_file
    # que nao passe por Flex Template, e montar um template para isto seria
    # trocar um script de 3 linhas por uma imagem para manter. O subir.sh ja
    # recusa subir um segundo job com o mesmo prefixo, que e a protecao que
    # importa (dois jobs na mesma assinatura = cobranca dobrada).
    subir_dataflow = BashOperator(
        task_id="subir_dataflow",
        bash_command=f"cd {RAIZ} && bash streaming/subir.sh{FIM}",
        # append_env e nao env puro: o env do BashOperator SUBSTITUI o
        # ambiente, e sem ele o subir.sh perderia PYTHON (o venv do Beam),
        # PATH (o gcloud) e as credenciais.
        env={"PROJECT_ID": PROJETO, "REGIAO": REGIAO},
        append_env=True,
    )

    # "Running" no Dataflow e o estado do JOB, nao do worker. Enquanto nao
    # houver worker consumindo, publicar so enche a assinatura.
    esperar_dataflow_pronto = BashOperator(
        task_id="esperar_dataflow_pronto",
        bash_command=(
            "set -euo pipefail\n"
            f"limite=$(( $(date +%s) + 420 ))\n"
            "while [ \"$(date +%s)\" -lt \"$limite\" ]; do\n"
            "  atual=$(gcloud dataflow jobs list --project="
            f"{PROJETO} --region={REGIAO} --status=active "
            f"--filter='name~^{PREFIXO_JOB}' --format='value(state)' | head -1)\n"
            "  if [ \"$atual\" = \"Running\" ]; then\n"
            # Depois de Running, o worker ainda instala as dependencias.
            "    sleep 150; echo 'worker pronto'; exit 0\n"
            "  fi\n"
            "  echo \"estado=$atual, aguardando\"; sleep 20\n"
            "done\n"
            "echo 'o job nao chegou a Running no prazo' >&2; exit 1\n"
        ),
    )

    # O gerador e um Cloud Run Job: escala a zero, e so cobra o tempo do
    # replay. No fator 10 do padrao, a corrida inteira sai em ~14 min.
    disparar_gerador = BashOperator(
        task_id="disparar_gerador",
        # Sem nova tentativa, como o proprio Cloud Run Job (--max-retries=0):
        # tentar de novo recomecaria o replay do zero e publicaria a corrida
        # duplicada. E e isto que faz o producer/parar.sh funcionar: cancelar
        # a execucao derruba esta tarefa uma vez so, e a drenar_dataflow
        # (ALL_DONE) desliga o Dataflow em seguida.
        retries=0,
        # --minutos so entra se vier na configuracao: o gerador trata a
        # ausencia dele como "a corrida inteira", e passar vazio seria erro
        # de argumento.
        bash_command=(
            "set -euo pipefail\n"
            f'ARGS="--session-key,{SESSAO},--fator,{FATOR},--injetar-invalidos,{INVALIDOS}"\n'
            f'MINUTOS="{MINUTOS}"\n'
            'if [ -n "$MINUTOS" ]; then ARGS="$ARGS,--minutos,$MINUTOS"; fi\n'
            'echo "argumentos do gerador: $ARGS"\n'
            f"gcloud run jobs execute f1-gerador --project={PROJETO} "
            f'--region={REGIAO} --args="$ARGS" --wait\n'
        ),
    )

    # A vigilancia fica na API de BI (bi-api/AIRFLOW.md): 200 quando o stream esta
    # recente, a latencia p95 esta no limite e a DLQ nao explodiu; 503 em
    # qualquer falha, o que o sensor entende sozinho.
    esperar_stream_saudavel = HttpSensor(
        task_id="esperar_stream_saudavel",
        http_conn_id="f1_bi_api",
        endpoint="api/pipeline/status",
        request_params={"max_idade_s": 120},
        poke_interval=15,
        timeout=int(timedelta(minutes=10).total_seconds()),  # o stream fica saudavel em segundos
        # reschedule e nao poke: libera o worker entre as tentativas em vez
        # de ocupar um slot por dez minutos.
        mode="reschedule",
    )

    # ALL_DONE, e nao a regra padrao: se qualquer tarefa acima falhar, o job
    # do Dataflow ficaria de pe cobrando. Esta e a tarefa que nao pode ser
    # pulada.
    #
    # ESPERAR=1 porque o drain nao e instantaneo -- o Dataflow reavalia o
    # estado a cada 10 minutos, e a cobranca so termina quando as VMs saem.
    # A tarefa so e marcada como concluida quando nao ha mais worker ligado:
    # e o que faz "a DAG terminou" significar "paramos de pagar", e e o que
    # permite a proxima execucao subir sem esbarrar num job ainda ativo.
    drenar_dataflow = BashOperator(
        task_id="drenar_dataflow",
        bash_command=f"cd {RAIZ} && ESPERAR=1 bash streaming/parar.sh{FIM}",
        env={"PROJECT_ID": PROJETO, "REGIAO": REGIAO},
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=2,
    )

    (
        aquecer_f1_api
        >> subir_dataflow
        >> esperar_dataflow_pronto
        >> disparar_gerador
        >> esperar_stream_saudavel
        >> drenar_dataflow
    )
