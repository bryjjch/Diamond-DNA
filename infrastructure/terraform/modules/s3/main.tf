terraform {
  required_version = ">= 1.0"
}

# Data Lake Bucket for Raw Game Logs
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name

  tags = merge(
    var.tags,
    {
      Name  = var.data_lake_bucket_name
      Type  = "data-lake"
      Usage = "raw-game-logs"
    }
  )
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.data_lake_kms_key_id != null ? "aws:kms" : "AES256"
      kms_master_key_id = var.data_lake_kms_key_id
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition-to-glacier"
    status = "Enabled"

    transition {
      days          = var.data_lake_glacier_transition_days
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "transition-to-deep-archive"
    status = "Enabled"

    transition {
      days          = var.data_lake_deep_archive_transition_days
      storage_class = "DEEP_ARCHIVE"
    }
  }

  rule {
    id     = "expire-old-versions"
    status = var.data_lake_enable_version_expiration ? "Enabled" : "Disabled"

    noncurrent_version_expiration {
      noncurrent_days = var.data_lake_version_expiration_days
    }
  }
}

# Model Artifacts Bucket for SageMaker
resource "aws_s3_bucket" "model_artifacts" {
  bucket = var.model_artifacts_bucket_name

  tags = merge(
    var.tags,
    {
      Name  = var.model_artifacts_bucket_name
      Type  = "model-artifacts"
      Usage = "sagemaker-models"
    }
  )
}

resource "aws_s3_bucket_versioning" "model_artifacts" {
  bucket = aws_s3_bucket.model_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "model_artifacts" {
  bucket = aws_s3_bucket.model_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.model_artifacts_kms_key_id != null ? "aws:kms" : "AES256"
      kms_master_key_id = var.model_artifacts_kms_key_id
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "model_artifacts" {
  bucket = aws_s3_bucket.model_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
