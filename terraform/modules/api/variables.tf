variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket holding the gold parquet tables this API reads"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the S3 bucket holding the gold parquet tables this API reads"
  type        = string
}

variable "gold_s3_prefix" {
  description = "S3 prefix for gold model-ready feature datasets (read by the API Lambda)"
  type        = string
  default     = "gold/features"
}

variable "webapp_year" {
  description = "Season year served by the API; empty string defers to the runtime default (UTC year - 1)"
  type        = string
  default     = ""
}

variable "memory_size" {
  description = "Lambda memory size in MB for the HTTP API (larger memory also gets more CPU, useful for pandas load)"
  type        = number
  default     = 2048
}

variable "timeout" {
  description = "Lambda timeout in seconds for the HTTP API"
  type        = number
  default     = 30
}

variable "image_tag" {
  description = "ECR image tag for the API Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days for the API Lambda"
  type        = number
  default     = 14
}

variable "cors_allow_origins" {
  description = "Origins allowed by API Gateway CORS (use [\"*\"] while iterating; lock to the CloudFront domain in prod)"
  type        = list(string)
  default     = ["*"]
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
