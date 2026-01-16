variable "stream_name" {
  description = "Name of the Kinesis Data Stream"
  type        = string
}

variable "shard_count" {
  description = "Number of shards for the stream"
  type        = number
  default     = 1
}

variable "retention_period_hours" {
  description = "Retention period in hours (24-168)"
  type        = number
  default     = 24
}

variable "kms_key_id" {
  description = "KMS key ID for encryption (optional, uses no encryption if not provided)"
  type        = string
  default     = null
}

variable "shard_level_metrics" {
  description = "List of shard-level CloudWatch metrics to enable"
  type        = list(string)
  default     = ["IncomingRecords", "OutgoingRecords"]
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
