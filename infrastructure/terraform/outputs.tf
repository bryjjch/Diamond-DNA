# ============================================================================
# NETWORKING OUTPUTS
# ============================================================================
output "vpc_id" {
  description = "ID of the VPC"
  value       = module.networking.vpc_id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = module.networking.private_subnet_ids
}

# ============================================================================
# OPENSEARCH OUTPUTS
# ============================================================================
output "opensearch_domain_id" {
  description = "ID of the OpenSearch domain"
  value       = module.opensearch.domain_id
}

output "opensearch_domain_name" {
  description = "Name of the OpenSearch domain"
  value       = module.opensearch.domain_name
}

output "opensearch_domain_arn" {
  description = "ARN of the OpenSearch domain"
  value       = module.opensearch.domain_arn
}

output "opensearch_endpoint" {
  description = "OpenSearch domain endpoint"
  value       = module.opensearch.domain_endpoint
}

output "opensearch_kibana_endpoint" {
  description = "OpenSearch Kibana endpoint"
  value       = module.opensearch.kibana_endpoint
}

output "opensearch_credentials_secret_arn" {
  description = "ARN of the Secrets Manager secret containing OpenSearch master user credentials"
  value       = module.opensearch.credentials_secret_arn
}

# ============================================================================
# NIGHTLY_LAB OUTPUTS
# ============================================================================
output "scraper_function_name" {
  description = "Name of the scraper Lambda function"
  value       = module.nightly_lab.scraper_function_name
}

output "scraper_function_arn" {
  description = "ARN of the scraper Lambda function"
  value       = module.nightly_lab.scraper_function_arn
}

output "opensearch_indexer_function_name" {
  description = "Name of the OpenSearch indexer Lambda function"
  value       = module.nightly_lab.opensearch_indexer_function_name
}

output "opensearch_indexer_function_arn" {
  description = "ARN of the OpenSearch indexer Lambda function"
  value       = module.nightly_lab.opensearch_indexer_function_arn
}

output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = module.nightly_lab.data_lake_bucket_name
}

output "model_artifacts_bucket_name" {
  description = "Name of the model artifacts S3 bucket"
  value       = module.nightly_lab.model_artifacts_bucket_name
}

output "sagemaker_processing_role_arn" {
  description = "ARN of the SageMaker processing role"
  value       = module.nightly_lab.sagemaker_processing_role_arn
}

output "sagemaker_training_role_arn" {
  description = "ARN of the SageMaker training role"
  value       = module.nightly_lab.sagemaker_training_role_arn
}

# ============================================================================
# FRONT_OFFICE OUTPUTS
# ============================================================================
output "api_endpoint" {
  description = "URL of the API Gateway endpoint"
  value       = module.front_office.api_endpoint
}

output "api_id" {
  description = "ID of the API Gateway"
  value       = module.front_office.api_id
}

output "search_function_name" {
  description = "Name of the search Lambda function"
  value       = module.front_office.search_function_name
}

output "search_function_arn" {
  description = "ARN of the search Lambda function"
  value       = module.front_office.search_function_arn
}

output "simulation_function_name" {
  description = "Name of the simulation Lambda function"
  value       = module.front_office.simulation_function_name
}

output "simulation_function_arn" {
  description = "ARN of the simulation Lambda function"
  value       = module.front_office.simulation_function_arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table for user teams"
  value       = module.front_office.dynamodb_table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table for user teams"
  value       = module.front_office.dynamodb_table_arn
}