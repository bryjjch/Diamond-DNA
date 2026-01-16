output "orchestrator_function_name" {
  description = "Name of the orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.function_name
}

output "orchestrator_function_arn" {
  description = "ARN of the orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.arn
}

output "orchestrator_function_invoke_arn" {
  description = "Invoke ARN of the orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.invoke_arn
}

output "ingestion_function_name" {
  description = "Name of the ingestion Lambda function"
  value       = aws_lambda_function.ingestion.function_name
}

output "ingestion_function_arn" {
  description = "ARN of the ingestion Lambda function"
  value       = aws_lambda_function.ingestion.arn
}

output "ingestion_function_invoke_arn" {
  description = "Invoke ARN of the ingestion Lambda function"
  value       = aws_lambda_function.ingestion.invoke_arn
}
