# Build from repo root: docker build --platform linux/amd64 -f docker/features/Dockerfile .
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY docker/features/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# Default CLI command (override args at runtime as needed).
ENTRYPOINT ["python", "-m", "src.features.bronze_to_silver_features"]
