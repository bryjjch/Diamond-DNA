FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir \
    pybaseball \
    pandas \
    pyarrow \
    boto3

# Copy the backfill script
COPY data-pipeline/ingestion/statcast_backfill.py /app/statcast_backfill.py

# Make the script executable
RUN chmod +x /app/statcast_backfill.py

# Set working directory
WORKDIR /app

# Set entrypoint to run the Python script
ENTRYPOINT ["python3", "statcast_backfill.py"]
