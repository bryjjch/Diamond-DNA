# ML layer — AWS Lambda: gold player KNN similarity in PCA space (src.ml.player_similarity).
# Build from repo root:
#   docker build --platform linux/amd64 --provenance=false -f docker/ml/lambda_player_similarity/Dockerfile .
FROM python:3.11-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install
COPY docker/ml/lambda/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.11

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.ml.player_similarity.handler"]
