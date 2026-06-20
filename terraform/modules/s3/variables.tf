variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket for raw data (must be globally unique)"
  type        = string
}

variable "tags" {
  description = "Tags to apply to S3 buckets"
  type        = map(string)
  default     = {}
}

variable "name_prefix" {
  description = "Prefix for resource names (e.g. the S3 replication IAM role)."
  type        = string
  default     = "diamond-dna"
}

variable "replication_destination_bucket_arn" {
  description = "ARN of the destination bucket that gold/ objects replicate into (the MLB-Market-Simulator raw-data bucket). Leave empty to disable replication."
  type        = string
  default     = ""
}

variable "replication_prefix" {
  description = "Object key prefix replicated to the destination bucket."
  type        = string
  default     = "gold/"
}
