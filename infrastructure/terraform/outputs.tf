# ============================================================================
# LAMBDA OUTPUTS (Daily Statcast)
# ============================================================================
output "daily_statcast_lambda_function_name" {
  description = "Name of the daily Statcast Lambda function"
  value       = module.lambda.lambda_function_name
}

output "daily_statcast_lambda_function_arn" {
  description = "ARN of the daily Statcast Lambda function"
  value       = module.lambda.lambda_function_arn
}

output "daily_statcast_ecr_repository_url" {
  description = "ECR repository URL for the daily Statcast Lambda image (build and push image here, then tag as latest)"
  value       = module.lambda.ecr_repository_url
}