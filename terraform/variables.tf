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

# S3 Replication (gold/ -> MLB-Market-Simulator raw-data bucket)
variable "replication_destination_bucket_arn" {
  description = "ARN of the MLB-Market-Simulator raw-data bucket that gold/ objects replicate into. Leave empty to disable replication."
  type        = string
  default     = ""
}

variable "replication_prefix" {
  description = "Object key prefix replicated to the destination bucket."
  type        = string
  default     = "gold/"
}

# S3 Prefixes
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

# EventBridge schedules
variable "statcast_ingestion_schedule_expression" {
  description = "EventBridge schedule for bronze Statcast pitch ingestion (e.g. cron(0 6 * * ? *) for 6 AM UTC daily)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "statcast_silver_schedule_expression" {
  description = "EventBridge schedule for silver feature build (e.g. cron(15 6 * * ? *) for 6:15 AM UTC daily)"
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "statcast_gold_schedule_expression" {
  description = "EventBridge schedule for gold preprocessing (e.g. cron(30 6 * * ? *) for 6:30 AM UTC daily)"
  type        = string
  default     = "cron(30 6 * * ? *)"
}

# Lambda memory and timeout: bronze ingestion
variable "statcast_ingestion_memory_size" {
  description = "Lambda memory size in MB for bronze Statcast pitch ingestion"
  type        = number
  default     = 1024
}

variable "statcast_ingestion_timeout" {
  description = "Lambda timeout in seconds for bronze ingestion (statcast + running + defence run serially)"
  type        = number
  default     = 900
}

# Lambda memory and timeout: silver feature build
variable "statcast_silver_memory_size" {
  description = "Lambda memory size in MB for silver feature build"
  type        = number
  default     = 1024
}

variable "statcast_silver_timeout" {
  description = "Lambda timeout in seconds for silver feature build"
  type        = number
  default     = 900
}

# Lambda memory and timeout: gold preprocessing
variable "statcast_gold_memory_size" {
  description = "Lambda memory size in MB for gold preprocessing"
  type        = number
  default     = 1024
}

variable "statcast_gold_timeout" {
  description = "Lambda timeout in seconds for gold preprocessing"
  type        = number
  default     = 900
}

# ECR image tags
variable "statcast_ingestion_image_tag" {
  description = "ECR image tag for the bronze Statcast pitch ingestion Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "statcast_silver_image_tag" {
  description = "ECR image tag for the silver feature build Lambda (e.g. latest)"
  type        = string
  default     = "latest"
}

variable "statcast_gold_image_tag" {
  description = "ECR image tag for the gold preprocessing Lambda (e.g. latest)"
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

# CI/CD: GitHub Actions OIDC role for deploying the API Lambda
variable "enable_cicd" {
  description = "Provision the GitHub Actions OIDC role used by .github/workflows/deploy-api.yml"
  type        = bool
  default     = false
}

variable "github_owner" {
  description = "GitHub owner/org that owns this repo (required when enable_cicd = true)"
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repository name (required when enable_cicd = true)"
  type        = string
  default     = ""
}

variable "github_ref_pattern" {
  description = "Git ref allowed to assume the deploy role (default: only the main branch)"
  type        = string
  default     = "refs/heads/main"
}

variable "create_github_oidc_provider" {
  description = "Create the GitHub OIDC provider in IAM. Set to false if your account already has one."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
