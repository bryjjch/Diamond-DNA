# ============================================================================
# LAMBDA OUTPUTS (Statcast ingestion)
# ============================================================================
output "statcast_ingestion_lambda_function_name" {
  description = "Name of the Statcast ingestion Lambda function"
  value       = module.lambda.lambda_function_name
}

output "statcast_ingestion_lambda_function_arn" {
  description = "ARN of the Statcast ingestion Lambda function"
  value       = module.lambda.lambda_function_arn
}

output "statcast_ingestion_ecr_repository_url" {
  description = "ECR repository URL for the Statcast ingestion Lambda image (build and push from infrastructure/docker/statcast-ingestion, then tag as latest)"
  value       = module.lambda.ecr_repository_url
}