"""Configuração da API de BI, toda por variável de ambiente (padrões servem para dev local)."""
import os

PROJECT = os.getenv("GCP_PROJECT", "pdm-davi-2026")
LOCATION = os.getenv("BQ_LOCATION", "US")          # mesmo valor do workflow_settings.yaml

# MOCK=1 troca o BigQuery por dados simulados: front e demo funcionam sem GCP.
MOCK = os.getenv("MOCK", "0") == "1"

# Teto de bytes lidos por consulta: um filtro esquecido não vira conta alta.
BQ_MAX_BYTES = int(os.getenv("BQ_MAX_BYTES", str(10 * 1024**3)))

# Cache das consultas históricas (o que muda devagar). O stream nunca passa por ele.
CACHE_S = int(os.getenv("CACHE_S", "60"))

# Intervalo da consulta que alimenta o SSE. Uma só, compartilhada por todos os clientes.
STREAM_POLL_S = float(os.getenv("STREAM_POLL_S", "3"))
STREAM_BUFFER = int(os.getenv("STREAM_BUFFER", "200"))   # eventos entregues a quem acabou de conectar

CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",") if o.strip()]

# Dataset.tabela, como nos .sqlx do Dataform.
T_SESSOES = "f1_silver.silver_sessoes"
T_PILOTOS = "f1_silver.silver_pilotos"
T_SILVER_VOLTAS = "f1_silver.silver_voltas"
T_UNIFICADA = "f1_silver.vw_voltas_unificada"
T_GOLD_PNEU = "f1_gold.gold_degradacao_pneu"
T_STREAM = "f1_stream.voltas"
T_DLQ = "f1_stream.voltas_dlq"
