variable "cluster_name" {
  description = "Name of the DAX cluster"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for the DAX cluster"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID where DAX cluster will be deployed"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table that DAX will cache"
  type        = string
}

variable "allowed_security_group_ids" {
  description = "List of security group IDs allowed to access DAX"
  type        = list(string)
  default     = []
}

variable "node_type" {
  description = "Instance type for DAX nodes"
  type        = string
  default     = "dax.t3.small"
}

variable "replication_factor" {
  description = "Number of nodes in the DAX cluster"
  type        = number
  default     = 1
}

variable "enable_encryption" {
  description = "Enable encryption at rest for DAX"
  type        = bool
  default     = true
}

variable "query_ttl_millis" {
  description = "Query TTL in milliseconds (optional)"
  type        = string
  default     = null
}

variable "record_ttl_millis" {
  description = "Record TTL in milliseconds (optional)"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
