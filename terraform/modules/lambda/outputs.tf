output "lambda_function_name" {
  description = "Name of the bronze Statcast pitch ingestion Lambda"
  value       = aws_lambda_function.statcast_ingestion.function_name
}

output "lambda_function_arn" {
  description = "ARN of the bronze Statcast pitch ingestion Lambda"
  value       = aws_lambda_function.statcast_ingestion.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the bronze pitch ingestion Lambda image"
  value       = aws_ecr_repository.statcast_ingestion.repository_url
}

output "silver_lambda_function_name" {
  description = "Name of the silver feature build Lambda"
  value       = aws_lambda_function.silver_feature_build.function_name
}

output "silver_lambda_function_arn" {
  description = "ARN of the silver feature build Lambda"
  value       = aws_lambda_function.silver_feature_build.arn
}

output "silver_ecr_repository_url" {
  description = "ECR repository URL for the silver feature build Lambda image"
  value       = aws_ecr_repository.silver_feature_build.repository_url
}

output "gold_lambda_function_name" {
  description = "Name of the gold preprocessing Lambda"
  value       = aws_lambda_function.gold_preprocessing.function_name
}

output "gold_lambda_function_arn" {
  description = "ARN of the gold preprocessing Lambda"
  value       = aws_lambda_function.gold_preprocessing.arn
}

output "gold_ecr_repository_url" {
  description = "ECR repository URL for the gold preprocessing Lambda image"
  value       = aws_ecr_repository.gold_preprocessing.repository_url
}
