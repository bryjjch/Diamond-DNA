variable "endpoint_name" {
  description = "Name of the SageMaker endpoint"
  type        = string
}

variable "model_image" {
  description = "Docker image URI for the model container (required if using a custom container)"
  type        = string
  default     = null
}

variable "model_data_url" {
  description = "S3 URL for model artifacts (optional, will use model_artifacts_bucket_name + model_artifacts_path if not provided)"
  type        = string
  default     = null
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket containing model artifacts"
  type        = string
}

variable "model_artifacts_bucket_arn" {
  description = "ARN of the S3 bucket containing model artifacts"
  type        = string
}

variable "model_artifacts_path" {
  description = "S3 path within the bucket for model artifacts"
  type        = string
  default     = "models/"
}

variable "model_environment_variables" {
  description = "Environment variables for the model container"
  type        = map(string)
  default     = {}
}

variable "instance_type" {
  description = "EC2 instance type for the endpoint"
  type        = string
  default     = "ml.t3.medium"
}

variable "initial_instance_count" {
  description = "Initial number of instances for the endpoint"
  type        = number
  default     = 1
}

variable "subnet_ids" {
  description = "List of subnet IDs for VPC configuration"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for VPC configuration"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID for the endpoint"
  type        = string
}

variable "kms_key_id" {
  description = "KMS key ID for encryption (optional)"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
