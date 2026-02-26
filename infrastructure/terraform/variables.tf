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
  default     = "diamond-dna"
}

# S3 Configuration
variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (must be globally unique)"
  type        = string
}

# Statcast ingestion prefix
variable "statcast_ingestion_s3_prefix" {
  description = "S3 prefix for Statcast data (e.g. raw-data/statcast)"
  type        = string
  default     = "raw-data/statcast"
}

variable "statcast_ingestion_schedule_expression" {
  description = "EventBridge schedule for Statcast ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "statcast_ingestion_memory_size" {
  description = "Lambda memory size in MB for Statcast ingestion"
  type        = number
  default     = 1024
}

variable "statcast_ingestion_timeout" {
  description = "Lambda timeout in seconds for Statcast ingestion"
  type        = number
  default     = 300
}

variable "statcast_ingestion_image_tag" {
  description = "ECR image tag for the Statcast ingestion Lambda container (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days (used by Batch and Lambda)"
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
