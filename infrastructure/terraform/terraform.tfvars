# AWS Configuration
region      = "us-east-1"
environment = "dev"
name_prefix = "pitch-caller"

# VPC Configuration
vpc_cidr         = "10.0.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b"]

# DynamoDB Configuration
dynamodb_table_name = "BatterProfiles"

# S3 Buckets
# Note: S3 bucket names must be globally unique
# Replace with your own unique bucket names
data_lake_bucket_name      = "your-org-pitch-caller-data-lake-dev"
model_artifacts_bucket_name = "your-org-pitch-caller-models-dev"

# Kinesis Configuration
kinesis_stream_name = "pitch-data-stream"
kinesis_shard_count = 1

# Lambda Configuration
# If you have Lambda deployment packages ready, specify their paths:
# lambda_orchestrator_zip_path = "./lambda/orchestrator.zip"
# lambda_ingestion_zip_path    = "./lambda/ingestion.zip"

lambda_orchestrator_function_name = "pitch-orchestrator"
lambda_ingestion_function_name    = "pitch-ingestion"

# API Gateway Configuration
api_gateway_name     = "pitch-calling-api"
api_gateway_stage_name = "prod"

# EMR Configuration
emr_cluster_name = "pitch-processing-cluster"

# SageMaker Configuration
sagemaker_endpoint_name       = "pitch-prediction-endpoint"
sagemaker_model_artifacts_path = "models/"
sagemaker_instance_type        = "ml.t3.medium"

# Tags
tags = {
  Project     = "pitch-caller"
  Environment = "dev"
  ManagedBy   = "terraform"
  Team        = "data-engineering"
}
