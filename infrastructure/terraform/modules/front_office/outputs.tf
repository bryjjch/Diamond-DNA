output "api_endpoint" {
  description = "URL of the API Gateway endpoint"
  value       = "https://${aws_api_gateway_rest_api.main.id}.execute-api.${data.aws_region.current.name}.amazonaws.com/${var.api_stage_name}"
}

output "api_id" {
  description = "ID of the API Gateway"
  value       = aws_api_gateway_rest_api.main.id
}

output "search_function_name" {
  description = "Name of the search Lambda function"
  value       = aws_lambda_function.search.function_name
}

output "search_function_arn" {
  description = "ARN of the search Lambda function"
  value       = aws_lambda_function.search.arn
}

output "simulation_function_name" {
  description = "Name of the simulation Lambda function"
  value       = aws_lambda_function.simulation.function_name
}

output "simulation_function_arn" {
  description = "ARN of the simulation Lambda function"
  value       = aws_lambda_function.simulation.arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table"
  value       = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table"
  value       = module.dynamodb.table_arn
}

output "lambda_security_group_id" {
  description = "Security group ID for Lambda functions accessing OpenSearch"
  value       = aws_security_group.lambda_opensearch.id
}

data "aws_region" "current" {}