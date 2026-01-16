variable "api_name" {
  description = "Name of the API Gateway REST API"
  type        = string
  default     = "pitch-caller-api"
}

variable "api_description" {
  description = "Description of the API Gateway REST API"
  type        = string
  default     = "Pitch Calling API"
}

variable "stage_name" {
  description = "Name of the API Gateway stage"
  type        = string
  default     = "prod"
}

variable "lambda_function_name" {
  description = "Name of the Lambda function to integrate with"
  type        = string
}

variable "lambda_invoke_arn" {
  description = "Invoke ARN of the Lambda function"
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
