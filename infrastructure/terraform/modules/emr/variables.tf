variable "cluster_name" {
  description = "Name of the EMR cluster"
  type        = string
}

variable "release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "applications" {
  description = "List of applications to install on EMR cluster"
  type        = list(string)
  default     = ["Spark", "Hadoop"]
}

variable "subnet_id" {
  description = "Subnet ID where EMR cluster will be launched"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where EMR cluster will be launched"
  type        = string
}

variable "master_instance_type" {
  description = "EC2 instance type for master node"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EC2 instance type for core nodes"
  type        = string
  default     = "m5.large"
}

variable "core_instance_count" {
  description = "Number of core instances"
  type        = number
  default     = 2
}

variable "auto_termination_idle_timeout" {
  description = "Idle timeout in seconds before auto-terminating cluster (0 = disabled)"
  type        = number
  default     = 3600
}

variable "bootstrap_actions" {
  description = "List of bootstrap actions"
  type = list(object({
    path = string
    name = string
    args = list(string)
  }))
  default = []
}

variable "log_uri" {
  description = "S3 URI for EMR logs (optional)"
  type        = string
  default     = null
}

variable "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
