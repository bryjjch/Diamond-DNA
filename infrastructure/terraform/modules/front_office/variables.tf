variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for user rosters"
  type        = string
  default     = "DiamondDNA-User-Rosters"
}

variable "dynamodb_partition_key" {
  description = "Partition key for DynamoDB table"
  type        = string
  default     = "UserId"
}

variable "dynamodb_sort_key" {
  description = "Sort key for DynamoDB table"
  type        = string
  default     = "RosterId"
}

variable "dynamodb_enable_pitr" {
  description = "Enable point-in-time recovery for DynamoDB"
  type        = bool
  default     = false
}

variable "search_function_name" {
  description = "Name of the search Lambda function"
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

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

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
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

variable "private_subnet_ids" {
  description = "Private subnet IDs for Lambda functions"
  type        = list(string)
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

variable "opensearch_credentials_secret_arn" {
  description = "ARN of AWS Secrets Manager secret containing OpenSearch credentials. Secret should contain 'username' and 'password' fields."
  type        = string
}

variable "simulation_function_name" {
  description = "Name of the simulation Lambda function"
  type        = string
}

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

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "DiamondDNA"
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "api_name" {
  description = "Name of the API Gateway"
  type        = string
}

variable "api_stage_name" {
  description = "Stage name for API Gateway"
  type        = string
  default     = "prod"
}

variable "xgboost_model_path" {
  description = "S3 path to XGBoost model for simulation"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}