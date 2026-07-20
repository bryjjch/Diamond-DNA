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
  description = "S3 prefix for bronze Statcast pitch data (e.g. bronze/statcast)"
  type        = string
  default     = "bronze/statcast"
}

variable "raw_running_s3_prefix" {
  description = "S3 prefix for bronze sprint-speed leaderboard objects read by silver feature build"
  type        = string
  default     = "bronze/statcast_running"
}

variable "raw_defence_s3_prefix" {
  description = "S3 prefix for bronze defensive metrics read by silver feature build"
  type        = string
  default     = "bronze/defence"
}

variable "raw_bio_s3_prefix" {
  description = "S3 prefix for bronze MLB player bios read by silver feature build"
  type        = string
  default     = "bronze/bio"
}

variable "raw_standard_stats_s3_prefix" {
  description = "S3 prefix for bronze standard season stat lines (batting/pitching)"
  type        = string
  default     = "bronze/standard_stats"
}

variable "silver_s3_prefix" {
  description = "S3 prefix for silver player-year feature tables (e.g. silver)"
  type        = string
  default     = "silver"
}

variable "gold_s3_prefix" {
  description = "S3 prefix for gold model-ready feature datasets (e.g. gold/features)"
  type        = string
  default     = "gold/features"
}

variable "schedule_expression" {
  description = "EventBridge schedule for bronze Statcast pitch ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "silver_schedule_expression" {
  description = "EventBridge schedule for silver feature build (e.g. cron(15 6 * * ? *) for 6:15 AM UTC daily)"
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "gold_schedule_expression" {
  description = "EventBridge schedule for gold preprocessing (e.g. cron(30 6 * * ? *) for 6:30 AM UTC daily)"
  type        = string
  default     = "cron(30 6 * * ? *)"
}

variable "memory_size" {
  description = "Lambda memory size in MB for bronze ingestion"
  type        = number
  default     = 1024
}

variable "timeout" {
  description = "Lambda timeout in seconds for bronze ingestion (statcast + running + defence run serially)"
  type        = number
  default     = 900
}

variable "silver_memory_size" {
  description = "Lambda memory size in MB for silver feature build"
  type        = number
  default     = 1024
}

variable "silver_timeout" {
  description = "Lambda timeout in seconds for silver feature build"
  type        = number
  default     = 900
}

variable "gold_memory_size" {
  description = "Lambda memory size in MB for gold preprocessing"
  type        = number
  default     = 1024
}

variable "gold_timeout" {
  description = "Lambda timeout in seconds for gold preprocessing"
  type        = number
  default     = 900
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "image_tag" {
  description = "ECR image tag for the bronze Statcast pitch ingestion Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "silver_image_tag" {
  description = "ECR image tag for the silver feature build Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "gold_image_tag" {
  description = "ECR image tag for the gold preprocessing Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
