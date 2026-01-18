variable "domain_name" {
  description = "Name of the OpenSearch domain"
  type        = string
}

variable "instance_type" {
  description = "Instance type for OpenSearch nodes"
  type        = string
  default     = "r6g.large.search"
}

variable "instance_count" {
  description = "Number of instances in the cluster"
  type        = number
  default     = 2
}

variable "ebs_enabled" {
  description = "Whether EBS volumes are attached to data nodes"
  type        = bool
  default     = true
}

variable "volume_size" {
  description = "Size of EBS volumes (in GB)"
  type        = number
  default     = 20
}

variable "volume_type" {
  description = "Type of EBS volumes"
  type        = string
  default     = "gp3"
}

variable "subnet_ids" {
  description = "List of subnet IDs for OpenSearch"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID for OpenSearch"
  type        = string
}

variable "security_group_ids" {
  description = "List of security group IDs for OpenSearch"
  type        = list(string)
  default     = []
}

variable "master_user_name" {
  description = "Master username for OpenSearch"
  type        = string
  default     = "admin"
}

variable "master_user_password" {
  description = "Master password for OpenSearch (should use AWS Secrets Manager in production)"
  type        = string
  sensitive   = true
  default     = null
}

variable "encrypt_at_rest" {
  description = "Enable encryption at rest"
  type        = bool
  default     = true
}

variable "node_to_node_encryption" {
  description = "Enable node-to-node encryption"
  type        = bool
  default     = true
}

variable "engine_version" {
  description = "OpenSearch engine version"
  type        = string
  default     = "OpenSearch_2.11"
}

variable "zone_awareness_enabled" {
  description = "Enable zone awareness for high availability"
  type        = bool
  default     = true
}

variable "availability_zone_count" {
  description = "Number of availability zones"
  type        = number
  default     = 2
}

variable "create_service_linked_role" {
  description = "Whether to create the service-linked role for OpenSearch"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply to OpenSearch domain"
  type        = map(string)
  default     = {}
}