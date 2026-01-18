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
  default     = "deeproster"
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

# S3 Configuration
variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (must be globally unique)"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket for model artifacts (must be globally unique)"
  type        = string
}

# OpenSearch Configuration
variable "opensearch_instance_type" {
  description = "Instance type for OpenSearch nodes"
  type        = string
  default     = "r6g.large.search"
}

variable "opensearch_instance_count" {
  description = "Number of instances in OpenSearch cluster"
  type        = number
  default     = 2
}

variable "opensearch_master_user_name" {
  description = "Master username for OpenSearch"
  type        = string
  default     = "admin"
}

variable "opensearch_master_password" {
  description = "Master password for OpenSearch (should use AWS Secrets Manager in production)"
  type        = string
  sensitive   = true
}

variable "opensearch_zone_awareness_enabled" {
  description = "Enable zone awareness for OpenSearch"
  type        = bool
  default     = true
}

# Lambda Configuration - Scraper (Nightly Lab)
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
  description = "MLB API key (should use Secrets Manager in production)"
  type        = string
  sensitive   = true
  default     = null
}

# Lambda Configuration - OpenSearch Indexer (Nightly Lab)
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
  default     = 900 # 15 minutes
}

variable "opensearch_indexer_memory_size" {
  description = "Memory size for OpenSearch indexer Lambda (MB)"
  type        = number
  default     = 1024
}

# Lambda Configuration - Search (Front Office)
variable "search_zip_path" {
  description = "Path to zip file for search Lambda (optional)"
  type        = string
  default     = null
}

variable "search_handler" {
  description = "Lambda handler for search function"
  type        = string
  default     = "index.handler"
}

variable "search_timeout" {
  description = "Timeout for search Lambda (seconds)"
  type        = number
  default     = 30
}

variable "search_memory_size" {
  description = "Memory size for search Lambda (MB)"
  type        = number
  default     = 512
}

# Lambda Configuration - Simulation (Front Office)
variable "simulation_zip_path" {
  description = "Path to zip file for simulation Lambda (optional)"
  type        = string
  default     = null
}

variable "simulation_handler" {
  description = "Lambda handler for simulation function"
  type        = string
  default     = "index.handler"
}

variable "simulation_timeout" {
  description = "Timeout for simulation Lambda (seconds)"
  type        = number
  default     = 300 # 5 minutes
}

variable "simulation_memory_size" {
  description = "Memory size for simulation Lambda (MB)"
  type        = number
  default     = 2048
}

# Lambda Runtime
variable "lambda_runtime" {
  description = "Lambda runtime for all functions"
  type        = string
  default     = "python3.11"
}

# API Gateway Configuration
variable "api_stage_name" {
  description = "Stage name for API Gateway"
  type        = string
  default     = "prod"
}

# DynamoDB Configuration
variable "dynamodb_partition_key" {
  description = "Partition key for DynamoDB table (UserID)"
  type        = string
  default     = "UserId"
}

variable "dynamodb_sort_key" {
  description = "Sort key for DynamoDB table (RosterID)"
  type        = string
  default     = "RosterId"
}

variable "dynamodb_enable_pitr" {
  description = "Enable point-in-time recovery for DynamoDB"
  type        = bool
  default     = false
}

# SageMaker Training Configuration
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
  description = "Docker image URI for player2vec training (optional)"
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

# XGBoost Model Path for Simulation
variable "xgboost_model_path" {
  description = "S3 path to XGBoost model for simulation (optional)"
  type        = string
  default     = null
}

# CloudWatch Logs
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

# Tags
variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "deeproster"
    ManagedBy = "terraform"
  }
}