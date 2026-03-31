# ============================================================================
# LAMBDA OUTPUTS (bronze pitch ingest + silver feature Lambda)
# ============================================================================
output "statcast_ingestion_lambda_function_name" {
  description = "Name of the bronze Statcast pitch ingestion Lambda (legacy resource name: statcast-ingestion)"
  value       = module.lambda.lambda_function_name
}

output "statcast_ingestion_lambda_function_arn" {
  description = "ARN of the bronze Statcast pitch ingestion Lambda"
  value       = module.lambda.lambda_function_arn
}

output "statcast_ingestion_ecr_repository_url" {
  description = "ECR repository URL for the bronze pitch ingestion Lambda image (build: docker build --platform linux/amd64 -f docker/bronze/Dockerfile .)"
  value       = module.lambda.ecr_repository_url
}

output "statcast_by_player_lambda_function_name" {
  description = "Name of the silver feature build Lambda (legacy resource name: statcast-by-player)"
  value       = module.lambda.by_player_lambda_function_name
}

output "statcast_by_player_lambda_function_arn" {
  description = "ARN of the silver feature build Lambda"
  value       = module.lambda.by_player_lambda_function_arn
}

output "statcast_by_player_ecr_repository_url" {
  description = "ECR repository URL for the silver feature Lambda image (build: docker build --platform linux/amd64 -f docker/silver-lambda/Dockerfile .)"
  value       = module.lambda.by_player_ecr_repository_url
}