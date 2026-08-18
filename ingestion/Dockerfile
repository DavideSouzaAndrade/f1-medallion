FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# A onda e passada como argumento na execucao do Cloud Run Job:
#   gcloud run jobs execute f1-ingestao --args="--wave,2"
ENTRYPOINT ["python", "main.py"]
