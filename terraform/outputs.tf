# ============================================================================
# LAMBDA OUTPUTS
# ============================================================================

# Bronze: all-source ingestion
output "bronze_ingestion_lambda_function_name" {
  description = "Name of the bronze ingestion Lambda"
  value       = module.lambda.lambda_function_name
}

output "bronze_ingestion_lambda_function_arn" {
  description = "ARN of the bronze ingestion Lambda"
  value       = module.lambda.lambda_function_arn
}

output "bronze_ingestion_ecr_repository_url" {
  description = "ECR repository URL for the bronze ingestion Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/bronze/Dockerfile .)"
  value       = module.lambda.ecr_repository_url
}

# Silver: bronze-to-silver build (archetype features + standard stats)
output "silver_lambda_function_name" {
  description = "Name of the silver build Lambda"
  value       = module.lambda.silver_lambda_function_name
}

output "silver_lambda_function_arn" {
  description = "ARN of the silver build Lambda"
  value       = module.lambda.silver_lambda_function_arn
}

output "silver_ecr_repository_url" {
  description = "ECR repository URL for the silver build Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/silver/Dockerfile .)"
  value       = module.lambda.silver_ecr_repository_url
}

# Gold: silver-to-gold build (archetype preprocessing + performance matrices)
output "gold_lambda_function_name" {
  description = "Name of the gold build Lambda"
  value       = module.lambda.gold_lambda_function_name
}

output "gold_lambda_function_arn" {
  description = "ARN of the gold build Lambda"
  value       = module.lambda.gold_lambda_function_arn
}

output "gold_ecr_repository_url" {
  description = "ECR repository URL for the gold build Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/gold/Dockerfile .)"
  value       = module.lambda.gold_ecr_repository_url
}

# Step Functions: daily pipeline orchestration
output "data_pipeline_state_machine_arn" {
  description = "ARN of the daily bronze -> silver -> gold pipeline state machine"
  value       = module.lambda.data_pipeline_state_machine_arn
}

# ============================================================================
# API OUTPUTS (HTTP API Lambda + API Gateway v2)
# ============================================================================
output "api_lambda_function_name" {
  description = "Name of the HTTP API Lambda"
  value       = module.api.lambda_function_name
}

output "api_lambda_function_arn" {
  description = "ARN of the HTTP API Lambda"
  value       = module.api.lambda_function_arn
}

output "api_ecr_repository_url" {
  description = "ECR repository URL for the HTTP API Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/api/Dockerfile .)"
  value       = module.api.ecr_repository_url
}

output "api_endpoint" {
  description = "HTTPS base URL for the HTTP API (use as VITE_API_BASE_URL in Vercel)"
  value       = module.api.api_endpoint
}
