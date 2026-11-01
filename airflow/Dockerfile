# Airflow com o que as DAGs da frente B precisam chamar.
#
# Duas ferramentas que a imagem oficial nao traz:
#
#   gcloud  -- o subir.sh e o parar.sh falam com o Dataflow e o Cloud Run
#              por linha de comando.
#   Beam    -- lancar um job no Dataflow exige o SDK do Beam na maquina que
#              lanca: e ele que empacota o pipeline e o envia.
#
# O Beam vai num venv SEPARADO, em /opt/beam, e nao junto do Airflow. Os
# dois fixam versoes de grpcio, protobuf e google-cloud-* e instalar um
# sobre o outro quebra os dois. O subir.sh ja aceita a variavel PYTHON, que
# e por onde a DAG aponta para esse venv.
FROM apache/airflow:2.10.5-python3.11

USER root

# O gcloud oficial via apt, para o container nao depender de um curl de
# instalador a cada build.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg apt-transport-https ca-certificates \
 && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/gcloud.list \
 && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
      | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
 && sed -i 's|^deb |deb [signed-by=/usr/share/keyrings/cloud.google.gpg] |' \
      /etc/apt/sources.list.d/gcloud.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends google-cloud-cli \
 && rm -rf /var/lib/apt/lists/*

# O venv do Beam, isolado do Airflow.
#
# Le o streaming/requirements.txt do proprio repositorio -- o contexto do
# build e a raiz do projeto, definida no docker-compose.yml. Copiar o
# arquivo para ca criaria uma segunda fonte da verdade que divergiria na
# primeira atualizacao de versao.
COPY streaming/requirements.txt /tmp/beam-requirements.txt
RUN python -m venv /opt/beam \
 && /opt/beam/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/beam/bin/pip install --no-cache-dir -r /tmp/beam-requirements.txt \
 && chown -R airflow: /opt/beam

USER airflow

# Os providers que as DAGs importam. Vem na imagem oficial, mas fixar aqui
# documenta a dependencia e protege de uma imagem base mais magra.
RUN pip install --no-cache-dir \
      "apache-airflow-providers-google>=10.21" \
      "apache-airflow-providers-http>=4.13"
