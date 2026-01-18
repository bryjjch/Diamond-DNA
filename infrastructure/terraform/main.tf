terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "aws" {
  profile = "default"
  region  = var.region
}

# ============================================================================
# AWS SECRETS MANAGER
# ============================================================================
# Store sensitive credentials securely
resource "aws_secretsmanager_secret" "mlb_api_key" {
  count       = var.mlb_api_key_secret_arn == null && var.mlb_api_key != null ? 1 : 0
  name        = "${var.name_prefix}-mlb-api-key-${var.environment}"
  description = "MLB API key for data scraping"

  tags = merge(
    local.common_tags,
    {
      Name = "${var.name_prefix}-mlb-api-key-${var.environment}"
    }
  )
}

resource "aws_secretsmanager_secret_version" "mlb_api_key" {
  count     = var.mlb_api_key_secret_arn == null && var.mlb_api_key != null ? 1 : 0
  secret_id = aws_secretsmanager_secret.mlb_api_key[0].id
  secret_string = jsonencode({
    api_key = var.mlb_api_key
  })
}

resource "aws_secretsmanager_secret" "opensearch_credentials" {
  count       = var.opensearch_credentials_secret_arn == null && var.opensearch_master_password != null ? 1 : 0
  name        = "${var.name_prefix}-opensearch-credentials-${var.environment}"
  description = "OpenSearch master user credentials"

  tags = merge(
    local.common_tags,
    {
      Name = "${var.name_prefix}-opensearch-credentials-${var.environment}"
    }
  )
}

resource "aws_secretsmanager_secret_version" "opensearch_credentials" {
  count     = var.opensearch_credentials_secret_arn == null && var.opensearch_master_password != null ? 1 : 0
  secret_id = aws_secretsmanager_secret.opensearch_credentials[0].id
  secret_string = jsonencode({
    username = var.opensearch_master_user_name
    password = var.opensearch_master_password
  })
}

# Local values for secret ARNs
locals {
  mlb_api_key_secret_arn = var.mlb_api_key_secret_arn != null ? var.mlb_api_key_secret_arn : (var.mlb_api_key != null ? aws_secretsmanager_secret.mlb_api_key[0].arn : null)
  opensearch_credentials_secret_arn = var.opensearch_credentials_secret_arn != null ? var.opensearch_credentials_secret_arn : (var.opensearch_master_password != null ? aws_secretsmanager_secret.opensearch_credentials[0].arn : null)
}

# ============================================================================
# NETWORKING MODULE
# ============================================================================
# Provides VPC, subnets, NAT gateways, and networking infrastructure
module "networking" {
  source = "./modules/networking"

  name_prefix          = var.name_prefix
  vpc_cidr            = var.vpc_cidr
  availability_zones  = local.availability_zones
  public_subnet_cidrs = [cidrsubnet(var.vpc_cidr, 8, 1), cidrsubnet(var.vpc_cidr, 8, 2)]
  private_subnet_cidrs = [cidrsubnet(var.vpc_cidr, 8, 10), cidrsubnet(var.vpc_cidr, 8, 20)]

  tags = local.common_tags
}

# ============================================================================
# OPENSEARCH SERVICE (Shared by both nightly_lab and front_office)
# ============================================================================
# Stores player vectors for semantic search
module "opensearch" {
  source = "./modules/opensearch"

  domain_name                      = "${var.name_prefix}-opensearch-${var.environment}"
  instance_type                    = var.opensearch_instance_type
  instance_count                   = var.opensearch_instance_count
  subnet_ids                       = module.networking.private_subnet_ids
  vpc_id                           = module.networking.vpc_id
  master_user_name                 = var.opensearch_master_user_name
  master_user_password             = var.opensearch_master_password
  master_user_password_secret_arn  = local.opensearch_credentials_secret_arn
  zone_awareness_enabled           = var.opensearch_zone_awareness_enabled
  availability_zone_count          = length(local.availability_zones)

  tags = local.common_tags

  depends_on = [
    module.networking
  ]
}

# ============================================================================
# NIGHTLY_LAB MODULE
# ============================================================================
# Handles nightly data pipeline: scraping, processing, training, and indexing
module "nightly_lab" {
  source = "./modules/nightly_lab"

  name_prefix                      = var.name_prefix
  vpc_id                           = module.networking.vpc_id
  private_subnet_ids               = module.networking.private_subnet_ids
  data_lake_bucket_name            = var.data_lake_bucket_name
  model_artifacts_bucket_name      = var.model_artifacts_bucket_name
  scraper_function_name            = "${var.name_prefix}-mlb-scraper-${var.environment}"
  scraper_zip_path                 = var.scraper_zip_path
  scraper_handler                  = var.scraper_handler
  scraper_timeout                  = var.scraper_timeout
  scraper_memory_size              = var.scraper_memory_size
  mlb_api_key_secret_arn           = local.mlb_api_key_secret_arn
  opensearch_indexer_function_name = "${var.name_prefix}-opensearch-indexer-${var.environment}"
  opensearch_indexer_zip_path      = var.opensearch_indexer_zip_path
  opensearch_indexer_handler       = var.opensearch_indexer_handler
  opensearch_indexer_timeout       = var.opensearch_indexer_timeout
  opensearch_indexer_memory_size   = var.opensearch_indexer_memory_size
  opensearch_endpoint              = module.opensearch.domain_endpoint
  opensearch_domain_arn            = module.opensearch.domain_arn
  opensearch_username              = var.opensearch_master_user_name
  opensearch_credentials_secret_arn = local.opensearch_credentials_secret_arn
  lambda_runtime                   = var.lambda_runtime
  clean_data_path                  = var.clean_data_path
  training_output_path             = var.training_output_path
  player2vec_training_image        = var.player2vec_training_image
  training_instance_type           = var.training_instance_type
  training_instance_count          = var.training_instance_count
  log_retention_days               = var.log_retention_days

  tags = local.common_tags

  depends_on = [
    module.networking,
    module.opensearch
  ]
}

# ============================================================================
# FRONT_OFFICE MODULE
# ============================================================================
# User-facing application: API Gateway, search/simulation Lambdas, DynamoDB
module "front_office" {
  source = "./modules/front_office"

  name_prefix                  = var.name_prefix
  vpc_id                       = module.networking.vpc_id
  private_subnet_ids           = module.networking.private_subnet_ids
  api_name                     = "${var.name_prefix}-api-${var.environment}"
  api_stage_name               = var.api_stage_name
  search_function_name         = "${var.name_prefix}-search-${var.environment}"
  search_zip_path              = var.search_zip_path
  search_handler               = var.search_handler
  search_timeout               = var.search_timeout
  search_memory_size           = var.search_memory_size
  simulation_function_name     = "${var.name_prefix}-simulation-${var.environment}"
  simulation_zip_path          = var.simulation_zip_path
  simulation_handler           = var.simulation_handler
  simulation_timeout           = var.simulation_timeout
  simulation_memory_size       = var.simulation_memory_size
  dynamodb_table_name          = "${var.name_prefix}-user-teams-${var.environment}"
  dynamodb_partition_key       = var.dynamodb_partition_key
  dynamodb_sort_key            = var.dynamodb_sort_key
  dynamodb_enable_pitr         = var.dynamodb_enable_pitr
  opensearch_endpoint              = module.opensearch.domain_endpoint
  opensearch_domain_arn            = module.opensearch.domain_arn
  opensearch_username              = var.opensearch_master_user_name
  opensearch_credentials_secret_arn = local.opensearch_credentials_secret_arn
  xgboost_model_path           = var.xgboost_model_path
  lambda_runtime               = var.lambda_runtime
  log_retention_days           = var.log_retention_days

  tags = local.common_tags

  depends_on = [
    module.networking,
    module.opensearch
  ]
}

# ============================================================================
# SECURITY GROUP RULES (Connect Lambda to OpenSearch)
# ============================================================================
# Allow Lambda functions in nightly_lab and front_office to access OpenSearch
# These rules allow Lambda security groups to connect to OpenSearch
resource "aws_security_group_rule" "opensearch_allow_lambda_nightly" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  source_security_group_id = module.nightly_lab.lambda_security_group_id
  security_group_id        = module.opensearch.security_group_id
  description              = "Allow nightly_lab Lambda to access OpenSearch"
}

resource "aws_security_group_rule" "opensearch_allow_lambda_front_office" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  source_security_group_id = module.front_office.lambda_security_group_id
  security_group_id        = module.opensearch.security_group_id
  description              = "Allow front_office Lambda to access OpenSearch"
}