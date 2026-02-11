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
# NETWORKING MODULE
# ============================================================================
# Provides VPC, subnets, NAT gateways, and networking infrastructure
module "networking" {
  source = "./modules/networking"

  name_prefix          = var.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = local.availability_zones
  public_subnet_cidrs  = [cidrsubnet(var.vpc_cidr, 8, 1), cidrsubnet(var.vpc_cidr, 8, 2)]
  private_subnet_cidrs = [cidrsubnet(var.vpc_cidr, 8, 10), cidrsubnet(var.vpc_cidr, 8, 20)]

  tags = local.common_tags
}


# ============================================================================
# BATCH BACKFILL MODULE
# ============================================================================
# Handles batch job for backfilling historical Statcast pitch data
module "batch_backfill" {
  source = "./modules/batch_backfill"

  name_prefix           = var.name_prefix
  vpc_id                = module.networking.vpc_id
  subnet_ids            = module.networking.private_subnet_ids
  data_lake_bucket_name = module.s3.data_lake_bucket_name
  data_lake_bucket_arn  = module.s3.data_lake_bucket_arn
  container_image_uri   = var.batch_backfill_image_uri
  job_start_year        = var.batch_backfill_start_year
  job_end_year          = var.batch_backfill_end_year
  s3_prefix             = var.batch_backfill_s3_prefix
  vcpus                 = var.batch_backfill_vcpus
  memory                = var.batch_backfill_memory
  log_retention_days    = var.log_retention_days

  tags = local.common_tags

  depends_on = [
    module.networking,
    module.nightly_lab
  ]
}

# ============================================================================
# S3 MODULE
# ============================================================================
# Provides S3 buckets for data lake and model model artifacts 
module "s3" {
  source = "./modules/s3"

  data_lake_bucket_name       = var.data_lake_bucket_name
  model_artifacts_bucket_name = var.model_artifacts_bucket_name

  tags = local.common_tags
}