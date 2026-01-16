output "endpoint_name" {
  description = "Name of the SageMaker endpoint"
  value       = aws_sagemaker_endpoint.main.name
}

output "endpoint_arn" {
  description = "ARN of the SageMaker endpoint"
  value       = aws_sagemaker_endpoint.main.arn
}

output "model_name" {
  description = "Name of the SageMaker model"
  value       = aws_sagemaker_model.main.name
}

output "endpoint_configuration_name" {
  description = "Name of the endpoint configuration"
  value       = aws_sagemaker_endpoint_configuration.main.name
}
