variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for Lambda functions"
  type        = list(string)
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket for model artifacts"
  type        = string
}

variable "scraper_function_name" {
  description = "Name of the scraper Lambda function"
  type        = string
}

variable "scraper_zip_path" {
  description = "Path to zip file for scraper Lambda (optional)"
  type        = string
  default     = null
}

variable "scraper_handler" {
  description = "Lambda handler for scraper function"
  type        = string
  default     = "index.handler"
}

variable "scraper_timeout" {
  description = "Timeout for scraper Lambda (seconds)"
  type        = number
  default     = 900 # 15 minutes
}

variable "scraper_memory_size" {
  description = "Memory size for scraper Lambda (MB)"
  type        = number
  default     = 512
}

variable "mlb_api_key" {
  description = "MLB API key (deprecated: use mlb_api_key_secret_arn instead)"
  type        = string
  sensitive   = true
  default     = null
}

variable "mlb_api_key_secret_arn" {
  description = "ARN of AWS Secrets Manager secret containing MLB API key. Secret should contain 'api_key' field."
  type        = string
  default     = null
}

variable "opensearch_indexer_function_name" {
  description = "Name of the OpenSearch indexer Lambda function"
  type        = string
}

variable "opensearch_indexer_zip_path" {
  description = "Path to zip file for OpenSearch indexer Lambda (optional)"
  type        = string
  default     = null
}

variable "opensearch_indexer_handler" {
  description = "Lambda handler for OpenSearch indexer function"
  type        = string
  default     = "index.handler"
}

variable "opensearch_indexer_timeout" {
  description = "Timeout for OpenSearch indexer Lambda (seconds)"
  type        = number
  default     = 900
}

variable "opensearch_indexer_memory_size" {
  description = "Memory size for OpenSearch indexer Lambda (MB)"
  type        = number
  default     = 1024
}

variable "opensearch_endpoint" {
  description = "OpenSearch domain endpoint"
  type        = string
}

variable "opensearch_domain_arn" {
  description = "ARN of the OpenSearch domain"
  type        = string
}

variable "opensearch_username" {
  description = "OpenSearch master username"
  type        = string
  default     = "admin"
}

variable "opensearch_password" {
  description = "OpenSearch master password (deprecated: use opensearch_credentials_secret_arn instead)"
  type        = string
  sensitive   = true
  default     = null
}

variable "opensearch_credentials_secret_arn" {
  description = "ARN of AWS Secrets Manager secret containing OpenSearch credentials. Secret should contain 'username' and 'password' fields."
  type        = string
  default     = null
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
}

variable "clean_data_path" {
  description = "S3 path for clean data after processing"
  type        = string
  default     = "clean-data/"
}

variable "training_output_path" {
  description = "S3 path for training job outputs"
  type        = string
  default     = "training-output/"
}

variable "player2vec_training_image" {
  description = "Docker image URI for player2vec training"
  type        = string
  default     = null
}

variable "xgboost_training_image" {
  description = "Docker image URI for XGBoost performance projector training"
  type        = string
  default     = null
}

variable "training_instance_type" {
  description = "Instance type for SageMaker training jobs"
  type        = string
  default     = "ml.m5.xlarge"
}

variable "training_instance_count" {
  description = "Number of instances for SageMaker training jobs"
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}