provider "aws" {
  profile = "default"
  region  = var.region
}


# ============================================================================
# S3 MODULE
# ============================================================================
module "s3" {
  source = "./modules/s3"

  data_lake_bucket_name = var.data_lake_bucket_name

  tags = var.tags
}


# ============================================================================
# LAMBDA MODULE (bronze ingestion + silver feature build + gold preprocessing)
# ============================================================================
# Three Lambdas, each with its own container image:
# - statcast-ingestion:   all bronze ingest      → src.data_pipeline.bronze.bronze_ingestion (statcast + running + defence)
# - silver-feature-build: silver feature build   → src.data_pipeline.silver.archetype_features.silver_features_build (YTD bronze → silver)
# - gold-preprocessing:   gold preprocessing     → src.data_pipeline.gold.gold_archetype_preprocessing (silver → gold)
module "lambda" {
  source = "./modules/lambda"

  name_prefix           = var.name_prefix
  data_lake_bucket_name = module.s3.data_lake_bucket_name
  data_lake_bucket_arn  = module.s3.data_lake_bucket_arn
  s3_prefix             = var.statcast_ingestion_s3_prefix
  silver_s3_prefix      = var.statcast_silver_s3_prefix
  gold_s3_prefix        = var.gold_s3_prefix

  schedule_expression        = var.statcast_ingestion_schedule_expression
  silver_schedule_expression = var.statcast_silver_schedule_expression
  gold_schedule_expression   = var.statcast_gold_schedule_expression

  memory_size        = var.statcast_ingestion_memory_size
  timeout            = var.statcast_ingestion_timeout
  silver_memory_size = var.statcast_silver_memory_size
  silver_timeout     = var.statcast_silver_timeout
  gold_memory_size   = var.statcast_gold_memory_size
  gold_timeout       = var.statcast_gold_timeout

  log_retention_days = var.log_retention_days
  image_tag          = var.statcast_ingestion_image_tag
  silver_image_tag   = var.statcast_silver_image_tag
  gold_image_tag     = var.statcast_gold_image_tag

  tags = var.tags

  depends_on = [module.s3]
}

# ============================================================================
# API MODULE (HTTP API Lambda + API Gateway v2)
# ============================================================================
# Serves the cluster browser and KNN endpoints from precomputed gold parquet
# tables. Replaces the Flask app's API surface; the React frontend (Phase 2/3)
# calls these routes via CloudFront → API Gateway → Lambda.
module "api" {
  source = "./modules/api"

  name_prefix           = var.name_prefix
  data_lake_bucket_name = module.s3.data_lake_bucket_name
  data_lake_bucket_arn  = module.s3.data_lake_bucket_arn
  gold_s3_prefix        = var.gold_s3_prefix
  predictions_s3_prefix = var.predictions_s3_prefix
  models_s3_prefix      = var.models_s3_prefix

  webapp_year        = var.api_webapp_year
  memory_size        = var.api_memory_size
  timeout            = var.api_timeout
  image_tag          = var.api_image_tag
  log_retention_days = var.log_retention_days
  cors_allow_origins = var.api_cors_allow_origins

  tags = var.tags

  depends_on = [module.s3]
}
