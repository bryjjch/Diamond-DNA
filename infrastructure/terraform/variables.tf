variable "region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "pitch-caller"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones for subnets"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table"
  type        = string
  default     = "BatterProfiles"
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for data lake"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket for model artifacts"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis stream"
  type        = string
  default     = "pitch-data-stream"
}

variable "kinesis_shard_count" {
  description = "Number of shards for Kinesis stream"
  type        = number
  default     = 1
}

variable "lambda_orchestrator_function_name" {
  description = "Name of the orchestrator Lambda function"
  type        = string
  default     = "pitch-orchestrator"
}

variable "lambda_ingestion_function_name" {
  description = "Name of the ingestion Lambda function"
  type        = string
  default     = "pitch-ingestion"
}

variable "lambda_orchestrator_zip_path" {
  description = "Path to zip file for orchestrator Lambda (optional)"
  type        = string
  default     = null
}

variable "lambda_ingestion_zip_path" {
  description = "Path to zip file for ingestion Lambda (optional)"
  type        = string
  default     = null
}

variable "api_gateway_name" {
  description = "Name of the API Gateway"
  type        = string
  default     = "pitch-calling-api"
}

variable "api_gateway_stage_name" {
  description = "Stage name for API Gateway"
  type        = string
  default     = "prod"
}

variable "emr_cluster_name" {
  description = "Name of the EMR cluster"
  type        = string
  default     = "pitch-processing-cluster"
}

variable "sagemaker_endpoint_name" {
  description = "Name of the SageMaker endpoint"
  type        = string
  default     = "pitch-prediction-endpoint"
}

variable "sagemaker_model_artifacts_path" {
  description = "S3 path for SageMaker model artifacts"
  type        = string
  default     = "models/"
}

variable "sagemaker_instance_type" {
  description = "Instance type for SageMaker endpoint"
  type        = string
  default     = "ml.t3.medium"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "pitch-caller"
    ManagedBy   = "terraform"
  }
}
