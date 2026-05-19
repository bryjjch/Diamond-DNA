# Silver layer — AWS Lambda: bronze → silver feature tables (src.data_pipeline.silver.feature_build).
# Build from repo root: docker build --platform linux/amd64 --provenance=false -f docker/silver/Dockerfile .
# Stage 1: install dependencies inside the Lambda runtime image for ABI compatibility
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/silver/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

# Stage 2: Lambda runtime
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.data_pipeline.silver.feature_build.handler"]
