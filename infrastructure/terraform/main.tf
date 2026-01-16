terraform {
  required_version = ">= 1.0"
}

provider "aws" {
  profile = "default"
  region = var.region
}

# VPC Module
module "vpc" {
  source = "./modules/vpc"

  name_prefix          = var.name_prefix
  vpc_cidr            = var.vpc_cidr
  availability_zones  = local.availability_zones
  public_subnet_cidrs = [cidrsubnet(var.vpc_cidr, 8, 1), cidrsubnet(var.vpc_cidr, 8, 2)]
  private_subnet_cidrs = [cidrsubnet(var.vpc_cidr, 8, 10), cidrsubnet(var.vpc_cidr, 8, 20)]

  tags = local.common_tags
}

# DynamoDB Module
module "dynamodb" {
  source = "./modules/dynamodb"

  table_name = "${var.name_prefix}-${var.dynamodb_table_name}-${var.environment}"
  
  tags = local.common_tags
}

# S3 Module
module "s3" {
  source = "./modules/s3"

  data_lake_bucket_name      = var.data_lake_bucket_name
  model_artifacts_bucket_name = var.model_artifacts_bucket_name

  tags = local.common_tags
}

# Kinesis Module
module "kinesis" {
  source = "./modules/kinesis"

  stream_name         = "${var.name_prefix}-${var.kinesis_stream_name}-${var.environment}"
  shard_count         = var.kinesis_shard_count

  tags = local.common_tags
}

# DAX Module
module "dax" {
  source = "./modules/dax"

  cluster_name         = "${var.name_prefix}-dax-${var.environment}"
  subnet_ids           = module.vpc.private_subnet_ids
  vpc_id               = module.vpc.vpc_id
  dynamodb_table_arn   = module.dynamodb.table_arn

  tags = local.common_tags

  depends_on = [
    module.dynamodb,
    module.vpc
  ]
}

# SageMaker Module
module "sagemaker" {
  source = "./modules/sagemaker"

  endpoint_name                = "${var.name_prefix}-${var.sagemaker_endpoint_name}-${var.environment}"
  model_artifacts_bucket_name  = module.s3.model_artifacts_bucket_name
  model_artifacts_bucket_arn   = module.s3.model_artifacts_bucket_arn
  model_artifacts_path         = var.sagemaker_model_artifacts_path
  instance_type                = var.sagemaker_instance_type
  subnet_ids                   = module.vpc.private_subnet_ids
  vpc_id                       = module.vpc.vpc_id
  security_group_ids           = [module.vpc.default_security_group_id]

  tags = local.common_tags

  depends_on = [
    module.s3,
    module.vpc
  ]
}

# Lambda Module
module "lambda" {
  source = "./modules/lambda"

  orchestrator_function_name  = "${var.name_prefix}-${var.lambda_orchestrator_function_name}-${var.environment}"
  ingestion_function_name     = "${var.name_prefix}-${var.lambda_ingestion_function_name}-${var.environment}"
  orchestrator_zip_path       = var.lambda_orchestrator_zip_path
  ingestion_zip_path          = var.lambda_ingestion_zip_path
  subnet_ids                  = module.vpc.private_subnet_ids
  security_group_ids          = [module.vpc.default_security_group_id]
  dynamodb_table_name         = module.dynamodb.table_name
  dynamodb_table_arn          = module.dynamodb.table_arn
  dax_cluster_endpoint        = module.dax.cluster_endpoint
  dax_cluster_arn             = module.dax.cluster_arn
  sagemaker_endpoint_name     = module.sagemaker.endpoint_name
  sagemaker_endpoint_arn      = module.sagemaker.endpoint_arn
  model_artifacts_bucket_name = module.s3.model_artifacts_bucket_name
  model_artifacts_bucket_arn  = module.s3.model_artifacts_bucket_arn
  data_lake_bucket_name       = module.s3.data_lake_bucket_name
  data_lake_bucket_arn        = module.s3.data_lake_bucket_arn
  kinesis_stream_name         = module.kinesis.stream_name
  kinesis_stream_arn          = module.kinesis.stream_arn

  tags = local.common_tags

  depends_on = [
    module.vpc,
    module.dynamodb,
    module.dax,
    module.s3,
    module.kinesis,
    module.sagemaker
  ]
}

# API Gateway Module
module "api_gateway" {
  source = "./modules/api-gateway"

  api_name              = "${var.name_prefix}-${var.api_gateway_name}-${var.environment}"
  stage_name            = var.api_gateway_stage_name
  lambda_function_name  = module.lambda.orchestrator_function_name
  lambda_invoke_arn     = module.lambda.orchestrator_function_invoke_arn

  tags = local.common_tags

  depends_on = [
    module.lambda
  ]
}

# EMR Module
module "emr" {
  source = "./modules/emr"

  cluster_name          = "${var.name_prefix}-${var.emr_cluster_name}-${var.environment}"
  subnet_id             = module.vpc.private_subnet_ids[0]
  vpc_id                = module.vpc.vpc_id
  data_lake_bucket_arn  = module.s3.data_lake_bucket_arn

  tags = local.common_tags

  depends_on = [
    module.vpc,
    module.s3
  ]
}
