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
  description = "Name of the S3 bucket for medallion data lake objects (must be globally unique)"
  type        = string
}

# Bronze Statcast pitch ingestion → S3 prefix
variable "statcast_ingestion_s3_prefix" {
  description = "S3 prefix for bronze Statcast pitch data (e.g. bronze/statcast)"
  type        = string
  default     = "bronze/statcast"
}

variable "statcast_silver_s3_prefix" {
  description = "S3 prefix for silver player-year feature tables (e.g. silver)"
  type        = string
  default     = "silver"
}

variable "statcast_gold_s3_prefix" {
  description = "S3 prefix for gold ML-ready Statcast datasets (e.g. gold/statcast)"
  type        = string
  default     = "gold/statcast"
}

variable "statcast_ingestion_schedule_expression" {
  description = "EventBridge schedule for bronze Statcast pitch ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "statcast_by_player_schedule_expression" {
  description = "EventBridge schedule for silver feature build (e.g. cron(15 6 * * ? *) for 6:15 AM UTC daily)"
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "statcast_ingestion_memory_size" {
  description = "Lambda memory size in MB for bronze Statcast pitch ingestion"
  type        = number
  default     = 1024
}

variable "statcast_ingestion_timeout" {
  description = "Lambda timeout in seconds for bronze Statcast pitch ingestion"
  type        = number
  default     = 300
}

variable "statcast_by_player_memory_size" {
  description = "Lambda memory size in MB for silver feature build"
  type        = number
  default     = 1024
}

variable "statcast_by_player_timeout" {
  description = "Lambda timeout in seconds for silver feature build"
  type        = number
  default     = 900
}

variable "statcast_ingestion_image_tag" {
  description = "ECR image tag for the bronze Statcast pitch ingestion Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "statcast_by_player_image_tag" {
  description = "ECR image tag for the silver feature Lambda (e.g. latest)"
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
