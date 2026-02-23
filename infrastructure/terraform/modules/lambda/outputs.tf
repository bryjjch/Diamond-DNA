output "lambda_function_name" {
  description = "Name of the daily Statcast Lambda function"
  value       = aws_lambda_function.daily_statcast.function_name
}

output "lambda_function_arn" {
  description = "ARN of the daily Statcast Lambda function"
  value       = aws_lambda_function.daily_statcast.arn
}

output "ecr_repository_url" {
  description = "URL of the ECR repository for the Lambda image"
  value       = aws_ecr_repository.daily_statcast.repository_url
}
