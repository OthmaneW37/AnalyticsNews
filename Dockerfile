# ============================================================
# Dockerfile — News Pipeline API + Dashboard
# ============================================================
FROM python:3.11-slim

# Dépendances système pour torch, transformers, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache HuggingFace persistant
ENV HF_HOME=/app/.hf_cache
ENV TRANSFORMERS_CACHE=/app/.hf_cache
ENV HF_DATASETS_CACHE=/app/.hf_cache

# 1. Copie et installation des dépendances Python (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copie du code source
COPY scrapers/ ./scrapers/
COPY datalake/ ./datalake/
COPY ingestion/ ./ingestion/
COPY warehouse/ ./warehouse/
COPY orchestration/ ./orchestration/
COPY api_server.py .
COPY run_full_pipeline.py .
COPY test_phase1.py .

# 3. Copie du frontend buildé
COPY frontend/dist/ ./frontend/dist/

# 4. Création des répertoires de données
RUN mkdir -p data/bronze data/silver data/gold data/warehouse

ENV PYTHONUNBUFFERED=1
ENV WAREHOUSE_PATH=/app/data/warehouse/news_warehouse.duckdb

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
