# Build from repo root: docker build -f infrastructure/docker/statcast-ingestion/Dockerfile .
FROM public.ecr.aws/lambda/python:3.11

# Install dependencies
COPY infrastructure/docker/statcast-ingestion/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy Statcast ingestion script (daily: start/end = yesterday)
COPY data-pipeline/ingestion/statcast_ingestion.py ${LAMBDA_TASK_ROOT}/

CMD ["statcast_ingestion.handler"]
