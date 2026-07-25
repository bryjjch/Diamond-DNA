# Gold layer — AWS Lambda: silver → gold orchestrator running archetype preprocessing +
# performance-prediction training matrices (src.data_pipeline.gold.gold_build).
# Build from repo root: docker build --platform linux/amd64 --provenance=false -f docker/gold/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/gold/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.data_pipeline.gold.gold_build.handler"]
