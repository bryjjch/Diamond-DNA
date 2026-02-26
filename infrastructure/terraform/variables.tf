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

# Daily Statcast Lambda Configuration
variable "daily_statcast_s3_prefix" {
  description = "S3 prefix for Statcast data (same as backfill; e.g. raw-data/statcast)"
  type        = string
  default     = "raw-data/statcast"
}

variable "daily_statcast_schedule_expression" {
  description = "EventBridge schedule for daily Statcast ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "daily_statcast_memory_size" {
  description = "Lambda memory size in MB for daily Statcast"
  type        = number
  default     = 1024
}

variable "daily_statcast_timeout" {
  description = "Lambda timeout in seconds for daily Statcast"
  type        = number
  default     = 300
}

variable "daily_statcast_image_tag" {
  description = "ECR image tag for the daily Statcast Lambda container (e.g. latest)"
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
