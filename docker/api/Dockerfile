# HTTP API layer — AWS Lambda: serves precomputed gold tables as JSON (src.api.handler).
# Build from repo root: docker build --platform linux/amd64 --provenance=false -f docker/api/Dockerfile -t <ecr_url>:<tag> .
# Push:  aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#        docker push <ecr_url>:<tag>
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/api/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.api.handler.handler"]
