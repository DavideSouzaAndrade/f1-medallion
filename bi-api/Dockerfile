FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
# 1 worker: o difusor do SSE mantém uma única consulta ao BigQuery para todos os clientes
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1
