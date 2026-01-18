output "scraper_function_name" {
  description = "Name of the scraper Lambda function"
  value       = aws_lambda_function.scraper.function_name
}

output "scraper_function_arn" {
  description = "ARN of the scraper Lambda function"
  value       = aws_lambda_function.scraper.arn
}

output "opensearch_indexer_function_name" {
  description = "Name of the OpenSearch indexer Lambda function"
  value       = aws_lambda_function.opensearch_indexer.function_name
}

output "opensearch_indexer_function_arn" {
  description = "ARN of the OpenSearch indexer Lambda function"
  value       = aws_lambda_function.opensearch_indexer.arn
}

output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = module.s3.data_lake_bucket_name
}

output "model_artifacts_bucket_name" {
  description = "Name of the model artifacts S3 bucket"
  value       = module.s3.model_artifacts_bucket_name
}

output "sagemaker_processing_role_arn" {
  description = "ARN of the SageMaker processing role"
  value       = aws_iam_role.sagemaker_processing.arn
}

output "sagemaker_training_role_arn" {
  description = "ARN of the SageMaker training role"
  value       = aws_iam_role.sagemaker_training.arn
}

output "lambda_security_group_id" {
  description = "Security group ID for Lambda functions"
  value       = aws_security_group.lambda_opensearch.id
}