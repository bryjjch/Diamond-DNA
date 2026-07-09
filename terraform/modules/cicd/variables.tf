variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "github_owner" {
  description = "GitHub owner / org that owns the repo (e.g. bryjjch)"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name (e.g. xWAR-Engine)"
  type        = string
}

variable "github_ref_pattern" {
  description = "Git ref pattern allowed to assume the role (default: only main branch)"
  type        = string
  default     = "refs/heads/main"
}

variable "create_oidc_provider" {
  description = "Create the GitHub Actions OIDC provider in IAM. Set to false if it already exists in this account."
  type        = bool
  default     = true
}

variable "api_ecr_repository_arn" {
  description = "ARN of the API ECR repository the workflow pushes to"
  type        = string
}

variable "api_lambda_function_arn" {
  description = "ARN of the API Lambda the workflow updates"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
