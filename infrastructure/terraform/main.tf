provider "aws" {
  profile = "default"
  region  = var.region
}


# ============================================================================
# S3 MODULE
# ============================================================================
# Provides S3 buckets for data lake and model model artifacts
module "s3" {
  source = "./modules/s3"

  data_lake_bucket_name       = var.data_lake_bucket_name

  tags = local.common_tags
}


# ============================================================================
# LAMBDA MODULE (Daily Statcast ingestion)
# ============================================================================
# Lambda function triggered daily by EventBridge to run daily_statcast.py
module "lambda" {
  source = "./modules/lambda"

  name_prefix           = var.name_prefix
  data_lake_bucket_name = module.s3.data_lake_bucket_name
  data_lake_bucket_arn  = module.s3.data_lake_bucket_arn
  s3_prefix             = var.daily_statcast_s3_prefix
  schedule_expression   = var.daily_statcast_schedule_expression
  memory_size           = var.daily_statcast_memory_size
  timeout               = var.daily_statcast_timeout
  log_retention_days    = var.log_retention_days
  image_tag             = var.daily_statcast_image_tag

  tags = local.common_tags

  depends_on = [module.s3]
}