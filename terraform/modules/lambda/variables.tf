variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (data lake)"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the S3 bucket for raw data"
  type        = string
}

variable "s3_prefix" {
  description = "S3 prefix/path for Statcast data (e.g. raw-data/statcast)"
  type        = string
  default     = "raw-data/statcast"
}

variable "processed_s3_prefix" {
  description = "S3 prefix/path for processed by-player Statcast data (e.g. processed/statcast)"
  type        = string
  default     = "processed/statcast"
}

variable "schedule_expression" {
  description = "EventBridge schedule expression for Statcast ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "by_player_schedule_expression" {
  description = "EventBridge schedule expression for by-player build (e.g. cron(15 6 * * ? *) for 6:15 AM UTC daily)"
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 1024
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "by_player_memory_size" {
  description = "Lambda memory size in MB for by-player build"
  type        = number
  default     = 1024
}

variable "by_player_timeout" {
  description = "Lambda timeout in seconds for by-player build"
  type        = number
  default     = 900
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days for the Lambda log group"
  type        = number
  default     = 14
}

variable "image_tag" {
  description = "Tag of the container image in the module's ECR repository (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
