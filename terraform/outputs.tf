# ============================================================================
# LAMBDA OUTPUTS
# ============================================================================

# Bronze: Statcast pitch ingestion
output "statcast_ingestion_lambda_function_name" {
  description = "Name of the bronze Statcast pitch ingestion Lambda"
  value       = module.lambda.lambda_function_name
}

output "statcast_ingestion_lambda_function_arn" {
  description = "ARN of the bronze Statcast pitch ingestion Lambda"
  value       = module.lambda.lambda_function_arn
}

output "statcast_ingestion_ecr_repository_url" {
  description = "ECR repository URL for the bronze pitch ingestion Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/bronze/Dockerfile .)"
  value       = module.lambda.ecr_repository_url
}

# Silver: bronze-to-silver feature build
output "statcast_silver_lambda_function_name" {
  description = "Name of the silver feature build Lambda"
  value       = module.lambda.silver_lambda_function_name
}

output "statcast_silver_lambda_function_arn" {
  description = "ARN of the silver feature build Lambda"
  value       = module.lambda.silver_lambda_function_arn
}

output "statcast_silver_ecr_repository_url" {
  description = "ECR repository URL for the silver feature build Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/silver/lambda/Dockerfile .)"
  value       = module.lambda.silver_ecr_repository_url
}

# Gold: silver-to-gold preprocessing
output "statcast_gold_lambda_function_name" {
  description = "Name of the gold preprocessing Lambda"
  value       = module.lambda.gold_lambda_function_name
}

output "statcast_gold_lambda_function_arn" {
  description = "ARN of the gold preprocessing Lambda"
  value       = module.lambda.gold_lambda_function_arn
}

output "statcast_gold_ecr_repository_url" {
  description = "ECR repository URL for the gold preprocessing Lambda image (build: docker build --platform linux/amd64 --provenance=false -f docker/gold/lambda/Dockerfile .)"
  value       = module.lambda.gold_ecr_repository_url
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

# ============================================================================
# CI/CD OUTPUTS
# ============================================================================
output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC (paste into the AWS_DEPLOY_ROLE_ARN repo variable). Null when enable_cicd = false."
  value       = var.enable_cicd ? module.cicd[0].github_actions_role_arn : null
}
