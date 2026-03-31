variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for medallion data (data lake)"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the S3 bucket for medallion data"
  type        = string
}

variable "s3_prefix" {
  description = "S3 prefix/path for bronze Statcast data (e.g. bronze/statcast)"
  type        = string
  default     = "bronze/statcast"
}

variable "raw_running_s3_prefix" {
  description = "S3 prefix for bronze sprint-speed leaderboard objects read by feature build"
  type        = string
  default     = "bronze/statcast_running"
}

variable "raw_defence_s3_prefix" {
  description = "S3 prefix for bronze defensive metrics read by feature build"
  type        = string
  default     = "bronze/defence"
}

variable "silver_s3_prefix" {
  description = "S3 prefix/path for silver player-year feature tables (e.g. silver/statcast)"
  type        = string
  default     = "silver/statcast"
}

variable "gold_s3_prefix" {
  description = "S3 prefix/path for gold ML-ready Statcast data (e.g. gold/statcast)"
  type        = string
  default     = "gold/statcast"
}

variable "schedule_expression" {
  description = "EventBridge schedule for bronze Statcast pitch ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "by_player_schedule_expression" {
  description = "EventBridge schedule for silver feature build (e.g. cron(15 6 * * ? *) for 6:15 AM UTC daily)"
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
  description = "Lambda memory size in MB for silver feature build"
  type        = number
  default     = 1024
}

variable "by_player_timeout" {
  description = "Lambda timeout in seconds for silver feature build"
  type        = number
  default     = 900
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days for the Lambda log group"
  type        = number
  default     = 14
}

variable "image_tag" {
  description = "Tag of the bronze Statcast pitch ingestion image in ECR (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "by_player_image_tag" {
  description = "Tag of the silver feature Lambda image in ECR (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
