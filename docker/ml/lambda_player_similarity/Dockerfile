# ML layer — AWS Lambda: gold player KNN similarity in PCA space (src.ml.knn_neighbours.player_similarity).
# Build from repo root:
#   docker build --platform linux/amd64 --provenance=false -f docker/ml/lambda_player_similarity/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /install
COPY docker/ml/lambda/requirements.txt .
RUN pip install --no-cache-dir --target /install -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /install/ ${LAMBDA_TASK_ROOT}/
COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.ml.knn_neighbours.player_similarity.handler"]
