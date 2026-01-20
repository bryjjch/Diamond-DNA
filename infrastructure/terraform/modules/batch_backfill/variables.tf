variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for Batch compute environment"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Batch compute environment (should be private subnets)"
  type        = list(string)
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the S3 bucket for raw data (if not provided, will be constructed from bucket name)"
  type        = string
  default     = null
}

variable "container_image_uri" {
  description = "ECR container image URI for the batch job"
  type        = string
}

variable "job_start_year" {
  description = "Start year for data backfill"
  type        = number
  default     = 2019
}

variable "job_end_year" {
  description = "End year for data backfill"
  type        = number
  default     = 2024
}

variable "s3_prefix" {
  description = "S3 prefix/path for storing Statcast data"
  type        = string
  default     = "raw-data/statcast"
}

variable "vcpus" {
  description = "Number of vCPUs for Batch job container"
  type        = number
  default     = 2
}

variable "memory" {
  description = "Memory (in MB) for Batch job container"
  type        = number
  default     = 4096
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
