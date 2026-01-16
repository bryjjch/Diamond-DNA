variable "orchestrator_function_name" {
  description = "Name of the orchestrator Lambda function"
  type        = string
}

variable "ingestion_function_name" {
  description = "Name of the ingestion Lambda function"
  type        = string
}

variable "runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.12"
}

variable "orchestrator_handler" {
  description = "Handler for the orchestrator Lambda function"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "ingestion_handler" {
  description = "Handler for the ingestion Lambda function"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "orchestrator_zip_path" {
  description = "Path to the zip file for orchestrator function (optional, for deployment package)"
  type        = string
  default     = null
}

variable "ingestion_zip_path" {
  description = "Path to the zip file for ingestion function (optional, for deployment package)"
  type        = string
  default     = null
}

variable "orchestrator_timeout" {
  description = "Timeout for orchestrator function in seconds"
  type        = number
  default     = 30
}

variable "ingestion_timeout" {
  description = "Timeout for ingestion function in seconds"
  type        = number
  default     = 60
}

variable "orchestrator_memory_size" {
  description = "Memory size for orchestrator function in MB"
  type        = number
  default     = 512
}

variable "ingestion_memory_size" {
  description = "Memory size for ingestion function in MB"
  type        = number
  default     = 256
}

variable "subnet_ids" {
  description = "List of subnet IDs for Lambda VPC configuration"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for Lambda VPC configuration"
  type        = list(string)
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table"
  type        = string
}

variable "dax_cluster_endpoint" {
  description = "Endpoint of the DAX cluster"
  type        = string
}

variable "dax_cluster_arn" {
  description = "ARN of the DAX cluster"
  type        = string
}

variable "sagemaker_endpoint_name" {
  description = "Name of the SageMaker endpoint"
  type        = string
}

variable "sagemaker_endpoint_arn" {
  description = "ARN of the SageMaker endpoint"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the model artifacts S3 bucket"
  type        = string
}

variable "model_artifacts_bucket_arn" {
  description = "ARN of the model artifacts S3 bucket"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis stream"
  type        = string
}

variable "kinesis_stream_arn" {
  description = "ARN of the Kinesis stream"
  type        = string
}

variable "kinesis_batch_size" {
  description = "Maximum number of records to retrieve per batch from Kinesis"
  type        = number
  default     = 10
}

variable "kinesis_maximum_batching_window" {
  description = "Maximum batching window in seconds for Kinesis event source"
  type        = number
  default     = 5
}

variable "kinesis_parallelization_factor" {
  description = "Number of batches to process from each shard concurrently"
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
