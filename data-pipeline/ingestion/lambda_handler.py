"""
Lambda entrypoint for daily Statcast ingestion.
Sets argv from environment and invokes daily_statcast.main().
"""
import os
import sys


def lambda_handler(_event, _context):
    sys.argv = [
        "daily_statcast.py",
        "--s3-bucket", os.environ.get("S3_BUCKET", "diamond-dna-raw-data"),
        "--s3-prefix", os.environ.get("S3_PREFIX", "raw-data/statcast"),
    ]
    import daily_statcast  # noqa: E402
    daily_statcast.main()
    return {"statusCode": 200, "body": "OK"}
