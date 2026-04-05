# Gold layer — batch/ECS: CLI for silver → gold preprocessing (python -m src.gold.silver_to_gold_preprocessing).
# Build from repo root: docker build --platform linux/amd64 -f docker/gold/batch/Dockerfile .
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY docker/gold/batch/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENTRYPOINT ["python", "-m", "src.gold.silver_to_gold_preprocessing"]
