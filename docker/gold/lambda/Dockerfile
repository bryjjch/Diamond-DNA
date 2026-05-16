# Gold layer — AWS Lambda: silver → gold preprocessed tables (src.gold.silver_to_gold_preprocessing).
# Build from repo root: docker build --platform linux/amd64 --provenance=false -f docker/gold/lambda/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/gold/lambda/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.gold.silver_to_gold_preprocessing.handler"]
