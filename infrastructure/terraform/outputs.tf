# VPC Outputs
output "vpc_id" {
  description = "ID of the VPC"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = module.vpc.private_subnet_ids
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.vpc.public_subnet_ids
}

# DynamoDB Outputs
output "dynamodb_table_name" {
  description = "Name of the DynamoDB table"
  value       = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table"
  value       = module.dynamodb.table_arn
}

# DAX Outputs
output "dax_cluster_endpoint" {
  description = "Endpoint of the DAX cluster"
  value       = module.dax.cluster_endpoint
}

output "dax_cluster_id" {
  description = "ID of the DAX cluster"
  value       = module.dax.cluster_id
}

# S3 Outputs
output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = module.s3.data_lake_bucket_name
}

output "model_artifacts_bucket_name" {
  description = "Name of the model artifacts S3 bucket"
  value       = module.s3.model_artifacts_bucket_name
}

# Kinesis Outputs
output "kinesis_stream_name" {
  description = "Name of the Kinesis stream"
  value       = module.kinesis.stream_name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis stream"
  value       = module.kinesis.stream_arn
}

# Lambda Outputs
output "orchestrator_function_name" {
  description = "Name of the orchestrator Lambda function"
  value       = module.lambda.orchestrator_function_name
}

output "orchestrator_function_arn" {
  description = "ARN of the orchestrator Lambda function"
  value       = module.lambda.orchestrator_function_arn
}

output "ingestion_function_name" {
  description = "Name of the ingestion Lambda function"
  value       = module.lambda.ingestion_function_name
}

output "ingestion_function_arn" {
  description = "ARN of the ingestion Lambda function"
  value       = module.lambda.ingestion_function_arn
}

# API Gateway Outputs
output "api_gateway_endpoint" {
  description = "URL of the API Gateway endpoint"
  value       = module.api_gateway.api_endpoint
}

output "api_gateway_id" {
  description = "ID of the API Gateway"
  value       = module.api_gateway.api_id
}

# EMR Outputs
output "emr_cluster_id" {
  description = "ID of the EMR cluster"
  value       = module.emr.cluster_id
}

# SageMaker Outputs
output "sagemaker_endpoint_name" {
  description = "Name of the SageMaker endpoint"
  value       = module.sagemaker.endpoint_name
}

output "sagemaker_endpoint_arn" {
  description = "ARN of the SageMaker endpoint"
  value       = module.sagemaker.endpoint_arn
}
