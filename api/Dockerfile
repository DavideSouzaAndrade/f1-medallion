FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 TF_CPP_MIN_LOG_LEVEL=2 PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py preditor.py ./
# 1 worker: o modelo fica em memória uma vez só por instância
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1
