# Silver layer — batch/ECS: CLI for bronze → silver (python -m src.silver.bronze_to_silver_features).
# Build from repo root: docker build --platform linux/amd64 -f docker/silver/batch/Dockerfile .
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY docker/silver/batch/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENTRYPOINT ["python", "-m", "src.silver.bronze_to_silver_features"]
