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
  default     = "xwar-engine"
}

# S3 Configuration
variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for medallion data lake objects (must be globally unique)"
  type        = string
}

# S3 Prefixes
variable "bronze_ingestion_s3_prefix" {
  description = "S3 prefix for bronze Statcast pitch data (e.g. bronze/statcast)"
  type        = string
  default     = "bronze/statcast"
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

variable "predictions_s3_prefix" {
  description = "S3 prefix for model inference outputs served by the API (e.g. gold/predictions)"
  type        = string
  default     = "gold/predictions"
}

variable "models_s3_prefix" {
  description = "S3 prefix for fitted estimators and metrics sidecars (e.g. models)"
  type        = string
  default     = "models"
}

# EventBridge schedule for the pipeline state machine (bronze -> silver -> gold)
variable "pipeline_schedule_expression" {
  description = "EventBridge schedule for the daily pipeline state machine (bronze -> silver -> gold); 22:00 UTC so the prior day's data has landed upstream"
  type        = string
  default     = "cron(0 22 * * ? *)"
}

# Lambda memory and timeout: bronze ingestion
variable "bronze_ingestion_memory_size" {
  description = "Lambda memory size in MB for bronze ingestion (all bronze sources)"
  type        = number
  default     = 1024
}

variable "bronze_ingestion_timeout" {
  description = "Lambda timeout in seconds for bronze ingestion (statcast + running + defence + bio + standard run serially)"
  type        = number
  default     = 900
}

# Lambda memory and timeout: silver build
variable "silver_memory_size" {
  description = "Lambda memory size in MB for the silver build (archetype features + standard stats)"
  type        = number
  default     = 1024
}

variable "silver_timeout" {
  description = "Lambda timeout in seconds for the silver build (both steps run serially)"
  type        = number
  default     = 900
}

# Lambda memory and timeout: gold build
variable "gold_memory_size" {
  description = "Lambda memory size in MB for the gold build (archetype preprocessing + performance matrices)"
  type        = number
  default     = 1024
}

variable "gold_timeout" {
  description = "Lambda timeout in seconds for the gold build (both steps run serially)"
  type        = number
  default     = 900
}

# ECR image tags
variable "bronze_ingestion_image_tag" {
  description = "ECR image tag for the bronze ingestion Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "silver_image_tag" {
  description = "ECR image tag for the silver build Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "gold_image_tag" {
  description = "ECR image tag for the gold build Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days (used by Lambda log groups)"
  type        = number
  default     = 14
}

# HTTP API Lambda (serves cluster browser + KNN endpoints to the frontend)
variable "api_memory_size" {
  description = "Lambda memory size in MB for the HTTP API"
  type        = number
  default     = 2048
}

variable "api_timeout" {
  description = "Lambda timeout in seconds for the HTTP API"
  type        = number
  default     = 30
}

variable "api_image_tag" {
  description = "ECR image tag for the HTTP API Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "api_webapp_year" {
  description = "Season year served by the HTTP API; empty string defers to the runtime default (UTC year - 1)"
  type        = string
  default     = ""
}

variable "api_cors_allow_origins" {
  description = "Origins allowed by API Gateway CORS for the HTTP API (lock to the Vercel domain after first deploy, e.g. [\"https://your-app.vercel.app\"])"
  type        = list(string)
  default     = ["*"]
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
