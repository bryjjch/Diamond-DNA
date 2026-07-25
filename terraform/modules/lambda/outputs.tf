output "lambda_function_name" {
  description = "Name of the bronze ingestion Lambda"
  value       = aws_lambda_function.bronze_ingestion.function_name
}

output "lambda_function_arn" {
  description = "ARN of the bronze ingestion Lambda"
  value       = aws_lambda_function.bronze_ingestion.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the bronze ingestion Lambda image"
  value       = aws_ecr_repository.bronze_ingestion.repository_url
}

output "silver_lambda_function_name" {
  description = "Name of the silver build Lambda"
  value       = aws_lambda_function.silver_feature_build.function_name
}

output "silver_lambda_function_arn" {
  description = "ARN of the silver build Lambda"
  value       = aws_lambda_function.silver_feature_build.arn
}

output "silver_ecr_repository_url" {
  description = "ECR repository URL for the silver build Lambda image"
  value       = aws_ecr_repository.silver_feature_build.repository_url
}

output "gold_lambda_function_name" {
  description = "Name of the gold build Lambda"
  value       = aws_lambda_function.gold_preprocessing.function_name
}

output "gold_lambda_function_arn" {
  description = "ARN of the gold build Lambda"
  value       = aws_lambda_function.gold_preprocessing.arn
}

output "gold_ecr_repository_url" {
  description = "ECR repository URL for the gold build Lambda image"
  value       = aws_ecr_repository.gold_preprocessing.repository_url
}

output "data_pipeline_state_machine_arn" {
  description = "ARN of the daily bronze -> silver -> gold pipeline state machine"
  value       = aws_sfn_state_machine.data_pipeline.arn
}

output "data_pipeline_state_machine_name" {
  description = "Name of the daily bronze -> silver -> gold pipeline state machine"
  value       = aws_sfn_state_machine.data_pipeline.name
}
