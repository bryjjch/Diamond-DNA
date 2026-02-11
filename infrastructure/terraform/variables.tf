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
  default     = "diamonddna"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones for subnets"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

# S3 Configuration
variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (must be globally unique)"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket for model artifacts (must be globally unique)"
  type        = string
}


# Batch Backfill Configuration
variable "batch_backfill_image_uri" {
  description = "ECR container image URI for the Statcast backfill batch job"
  type        = string
  default     = null
}

variable "batch_backfill_start_year" {
  description = "Start year for Statcast data backfill"
  type        = number
  default     = 2019
}

variable "batch_backfill_end_year" {
  description = "End year for Statcast data backfill"
  type        = number
  default     = 2024
}

variable "batch_backfill_s3_prefix" {
  description = "S3 prefix/path for storing Statcast backfill data"
  type        = string
  default     = "raw-data/statcast"
}

variable "batch_backfill_vcpus" {
  description = "Number of vCPUs for Batch backfill job container"
  type        = number
  default     = 2
}

variable "batch_backfill_memory" {
  description = "Memory (in MB) for Batch backfill job container"
  type        = number
  default     = 4096
}