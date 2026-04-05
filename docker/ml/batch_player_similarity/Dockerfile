# ML layer — batch/ECS: CLI for player similarity KNN (python -m src.ml.player_similarity).
# Build from repo root:
#   docker build --platform linux/amd64 -f docker/ml/batch_player_similarity/Dockerfile .
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY docker/ml/batch/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENTRYPOINT ["python", "-m", "src.ml.player_similarity"]
