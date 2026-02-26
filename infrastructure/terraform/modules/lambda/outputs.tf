output "lambda_function_name" {
  description = "Name of the Statcast ingestion Lambda function"
  value       = aws_lambda_function.statcast_ingestion.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Statcast ingestion Lambda function"
  value       = aws_lambda_function.statcast_ingestion.arn
}

output "ecr_repository_url" {
  description = "URL of the ECR repository for the Statcast ingestion Lambda image"
  value       = aws_ecr_repository.statcast_ingestion.repository_url
}
