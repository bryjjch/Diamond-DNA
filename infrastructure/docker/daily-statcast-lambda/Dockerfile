# Build from repo root: docker build -f infrastructure/docker/daily-statcast-lambda/Dockerfile .
FROM public.ecr.aws/lambda/python:3.11

# Install dependencies (same as backfill)
COPY infrastructure/docker/daily-statcast-lambda/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy ingestion scripts (lambda_handler invokes daily_statcast)
COPY data-pipeline/ingestion/lambda_handler.py ${LAMBDA_TASK_ROOT}/
COPY data-pipeline/ingestion/daily_statcast.py ${LAMBDA_TASK_ROOT}/

# Lambda calls this handler; it sets argv and runs daily_statcast.main()
CMD ["lambda_handler.lambda_handler"]
