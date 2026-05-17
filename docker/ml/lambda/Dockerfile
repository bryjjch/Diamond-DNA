# ML layer — AWS Lambda: gold archetype clustering GMM/PCA (src.ml.archetype_clustering).
# Build from repo root: docker build --platform linux/amd64 --provenance=false -f docker/ml/lambda/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/ml/lambda/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.ml.archetype_clustering.handler"]
