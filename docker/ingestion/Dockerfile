# Build from repo root (context = .): docker build --platform linux/amd64 -f docker/statcast-ingestion/Dockerfile .
# Stage 1: install dependencies in an image that can build from source
FROM python:3.11-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install
COPY docker/statcast-ingestion/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

# Stage 2: Lambda runtime (no compilers; copy pre-built packages)
FROM public.ecr.aws/lambda/python:3.11

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/s3_parquet.py ${LAMBDA_TASK_ROOT}/
COPY src/ingestion/statcast_ingestion.py ${LAMBDA_TASK_ROOT}/

CMD ["statcast_ingestion.handler"]
