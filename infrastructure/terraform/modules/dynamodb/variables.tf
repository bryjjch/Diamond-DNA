variable "table_name" {
  description = "Name of the DynamoDB table"
  type        = string
}

variable "partition_key" {
  description = "Partition key name for the DynamoDB table"
  type        = string
  default     = "batter_id"
}

variable "sort_key" {
  description = "Sort key name for the DynamoDB table"
  type        = string
  default     = "season"
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption (optional, uses AWS managed key if not provided)"
  type        = string
  default     = null
}

variable "enable_point_in_time_recovery" {
  description = "Enable point-in-time recovery"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
