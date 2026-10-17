FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Os parametros do replay sao passados na execucao do Cloud Run Job:
#   gcloud run jobs execute f1-gerador --args="--session-key,11361,--fator,30"
ENTRYPOINT ["python", "publisher.py"]
