output "lambda_function_name" {
  description = "Name of the HTTP API Lambda"
  value       = aws_lambda_function.api.function_name
}

output "lambda_function_arn" {
  description = "ARN of the HTTP API Lambda"
  value       = aws_lambda_function.api.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the HTTP API Lambda image"
  value       = aws_ecr_repository.api.repository_url
}

output "api_endpoint" {
  description = "HTTPS endpoint for the HTTP API (use as the API base URL for the frontend / CloudFront origin)"
  value       = aws_apigatewayv2_api.api.api_endpoint
}

output "api_id" {
  description = "ID of the HTTP API Gateway"
  value       = aws_apigatewayv2_api.api.id
}
