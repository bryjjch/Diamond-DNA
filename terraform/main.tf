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

  tags = var.tags
}


# ============================================================================
# LAMBDA MODULE (bronze Statcast pitches + silver feature build)
# ============================================================================
# Two Lambdas, each with its own container image:
# - statcast-ingestion (ECR/Lambda name): bronze pitch ingest → runs src.bronze.statcast_ingestion.
# - statcast-by-player: silver features → runs src.silver.bronze_to_silver_features (YTD bronze → silver).
module "lambda" {
  source = "./modules/lambda"

  name_prefix                    = var.name_prefix
  data_lake_bucket_name          = module.s3.data_lake_bucket_name
  data_lake_bucket_arn           = module.s3.data_lake_bucket_arn
  s3_prefix                      = var.statcast_ingestion_s3_prefix
  silver_s3_prefix               = var.statcast_silver_s3_prefix
  gold_s3_prefix                 = var.statcast_gold_s3_prefix
  schedule_expression            = var.statcast_ingestion_schedule_expression
  by_player_schedule_expression  = var.statcast_by_player_schedule_expression
  memory_size                    = var.statcast_ingestion_memory_size
  timeout                        = var.statcast_ingestion_timeout
  by_player_memory_size          = var.statcast_by_player_memory_size
  by_player_timeout              = var.statcast_by_player_timeout
  log_retention_days             = var.log_retention_days
  image_tag                      = var.statcast_ingestion_image_tag
  by_player_image_tag            = var.statcast_by_player_image_tag

  tags = var.tags

  depends_on = [module.s3]
}