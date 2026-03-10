# ============================================================================
# LAMBDA OUTPUTS (Statcast ingestion + by-player)
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
  description = "ECR repository URL for the Statcast ingestion Lambda image (build from repo root: docker build -f docker/statcast-ingestion/Dockerfile ., then tag and push to this URL)"
  value       = module.lambda.ecr_repository_url
}

output "statcast_by_player_lambda_function_name" {
  description = "Name of the Statcast by-player Lambda function"
  value       = module.lambda.by_player_lambda_function_name
}

output "statcast_by_player_lambda_function_arn" {
  description = "ARN of the Statcast by-player Lambda function"
  value       = module.lambda.by_player_lambda_function_arn
}

output "statcast_by_player_ecr_repository_url" {
  description = "ECR repository URL for the Statcast by-player Lambda image (build from repo root: docker build -f docker/statcast-by-player/Dockerfile ., then tag and push to this URL)"
  value       = module.lambda.by_player_ecr_repository_url
}