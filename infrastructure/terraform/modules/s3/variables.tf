variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for data lake (raw game logs)"
  type        = string
}

variable "model_artifacts_bucket_name" {
  description = "Name of the S3 bucket for model artifacts"
  type        = string
}

variable "data_lake_kms_key_id" {
  description = "KMS key ID for data lake bucket encryption (optional, uses AES256 if not provided)"
  type        = string
  default     = null
}

variable "model_artifacts_kms_key_id" {
  description = "KMS key ID for model artifacts bucket encryption (optional, uses AES256 if not provided)"
  type        = string
  default     = null
}

variable "data_lake_glacier_transition_days" {
  description = "Number of days before transitioning data lake objects to Glacier"
  type        = number
  default     = 90
}

variable "data_lake_deep_archive_transition_days" {
  description = "Number of days before transitioning data lake objects to Deep Archive"
  type        = number
  default     = 365
}

variable "data_lake_enable_version_expiration" {
  description = "Enable expiration of old versions in data lake bucket"
  type        = bool
  default     = true
}

variable "data_lake_version_expiration_days" {
  description = "Number of days before expiring old versions in data lake bucket"
  type        = number
  default     = 90
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
