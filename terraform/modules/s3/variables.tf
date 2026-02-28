variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (must be globally unique)"
  type        = string
}

variable "tags" {
  description = "Tags to apply to S3 buckets"
  type        = map(string)
  default     = {}
}
