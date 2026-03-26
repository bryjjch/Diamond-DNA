# Build from repo root: docker build --platform linux/amd64 -f docker/processing/Dockerfile .
# Stage 1: install dependencies in an image that can build from source
FROM python:3.11-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install
COPY docker/processing/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

# Stage 2: Lambda runtime (no compilers; copy pre-built packages)
FROM public.ecr.aws/lambda/python:3.11

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.processing.processing_statcast_by_player.handler"]
