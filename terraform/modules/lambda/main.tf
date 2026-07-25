# ECR repository for the bronze ingestion Lambda image (all bronze sources)
resource "aws_ecr_repository" "bronze_ingestion" {
  name                 = "${var.name_prefix}-bronze-ingestion"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# ECR repository for the silver feature build Lambda image
resource "aws_ecr_repository" "silver_feature_build" {
  name                 = "${var.name_prefix}-silver-feature-build"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# ECR repository for the gold preprocessing Lambda image
resource "aws_ecr_repository" "gold_preprocessing" {
  name                 = "${var.name_prefix}-gold-preprocessing"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# IAM role for the bronze ingestion Lambda
resource "aws_iam_role" "bronze_ingestion" {
  name = "${var.name_prefix}-bronze-ingestion-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Basic Lambda execution for bronze: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "bronze_ingestion_lambda_basic" {
  role       = aws_iam_role.bronze_ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 write access for bronze ingestion Lambda (statcast pitches + running + defence + bio + standard prefixes)
resource "aws_iam_role_policy" "bronze_ingestion_s3_access" {
  name = "${var.name_prefix}-bronze-ingestion-s3"
  role = aws_iam_role.bronze_ingestion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = [
          "${var.data_lake_bucket_arn}/${var.s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_running_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_defence_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_bio_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_standard_stats_s3_prefix}/*",
        ]
      }
    ]
  })
}

# IAM role for the silver feature build Lambda
resource "aws_iam_role" "silver_feature_build" {
  name = "${var.name_prefix}-silver-feature-build-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Basic Lambda execution for silver: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "silver_feature_build_lambda_basic" {
  role       = aws_iam_role.silver_feature_build.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 access for silver Lambda: read bronze, read/write silver, write gold
resource "aws_iam_role_policy" "silver_feature_build_s3_access" {
  name = "${var.name_prefix}-silver-feature-build-s3"
  role = aws_iam_role.silver_feature_build.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = var.data_lake_bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.s3_prefix}/*",
              "${var.raw_running_s3_prefix}/*",
              "${var.raw_defence_s3_prefix}/*",
              "${var.raw_bio_s3_prefix}/*",
              "${var.raw_standard_stats_s3_prefix}/*",
              "${var.silver_s3_prefix}/*",
              "${var.gold_s3_prefix}/*",
            ]
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "${var.data_lake_bucket_arn}/${var.s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_running_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_defence_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_bio_s3_prefix}/*",
          "${var.data_lake_bucket_arn}/${var.raw_standard_stats_s3_prefix}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.silver_s3_prefix}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.gold_s3_prefix}/*"
      }
    ]
  })
}

# IAM role for the gold preprocessing Lambda
resource "aws_iam_role" "gold_preprocessing" {
  name = "${var.name_prefix}-gold-preprocessing-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Basic Lambda execution for gold: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "gold_preprocessing_lambda_basic" {
  role       = aws_iam_role.gold_preprocessing.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 access for gold Lambda: read silver, read/write gold
resource "aws_iam_role_policy" "gold_preprocessing_s3_access" {
  name = "${var.name_prefix}-gold-preprocessing-s3"
  role = aws_iam_role.gold_preprocessing.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = var.data_lake_bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.silver_s3_prefix}/*",
              "${var.gold_s3_prefix}/*",
            ]
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.silver_s3_prefix}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.gold_s3_prefix}/*"
      }
    ]
  })
}

# CloudWatch log group for the bronze ingestion Lambda
resource "aws_cloudwatch_log_group" "bronze_ingestion" {
  name              = "/aws/lambda/${var.name_prefix}-bronze-ingestion"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# CloudWatch log group for the silver feature build Lambda
resource "aws_cloudwatch_log_group" "silver_feature_build" {
  name              = "/aws/lambda/${var.name_prefix}-silver-feature-build"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# CloudWatch log group for the gold preprocessing Lambda
resource "aws_cloudwatch_log_group" "gold_preprocessing" {
  name              = "/aws/lambda/${var.name_prefix}-gold-preprocessing"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# Bronze ingestion Lambda (container: data_pipeline.bronze.bronze_build — all bronze sources)
# Build: docker build --platform linux/amd64 --provenance=false -f docker/bronze/Dockerfile -t <ecr_url>:<tag> .
# Push:  aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#        docker push <ecr_url>:<tag>
resource "aws_lambda_function" "bronze_ingestion" {
  function_name = "${var.name_prefix}-bronze-ingestion"
  role          = aws_iam_role.bronze_ingestion.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.bronze_ingestion.repository_url}:${var.image_tag}"

  memory_size = var.memory_size
  timeout     = var.timeout

  environment {
    variables = {
      S3_BUCKET                 = var.data_lake_bucket_name
      S3_PREFIX                 = var.s3_prefix
      RAW_RUNNING_PREFIX        = var.raw_running_s3_prefix
      RAW_DEFENCE_PREFIX        = var.raw_defence_s3_prefix
      RAW_BIO_PREFIX            = var.raw_bio_s3_prefix
      RAW_STANDARD_STATS_PREFIX = var.raw_standard_stats_s3_prefix
      PYBASEBALL_NO_CACHE       = "true"
      HOME                      = "/tmp"
    }
  }

  depends_on = [aws_cloudwatch_log_group.bronze_ingestion]

  tags = var.tags
}

# Silver build Lambda (container: data_pipeline.silver.silver_build — orchestrates
# archetype features + standard stat-line tables in one run)
# Build: docker build --platform linux/amd64 --provenance=false -f docker/silver/Dockerfile -t <ecr_url>:<tag> .
resource "aws_lambda_function" "silver_feature_build" {
  function_name = "${var.name_prefix}-silver-feature-build"
  role          = aws_iam_role.silver_feature_build.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.silver_feature_build.repository_url}:${var.silver_image_tag}"

  memory_size = var.silver_memory_size
  timeout     = var.silver_timeout

  environment {
    variables = {
      S3_BUCKET                 = var.data_lake_bucket_name
      RAW_PREFIX                = var.s3_prefix
      RAW_RUNNING_PREFIX        = var.raw_running_s3_prefix
      RAW_DEFENCE_PREFIX        = var.raw_defence_s3_prefix
      RAW_BIO_PREFIX            = var.raw_bio_s3_prefix
      RAW_STANDARD_STATS_PREFIX = var.raw_standard_stats_s3_prefix
      FEATURE_PREFIX            = var.silver_s3_prefix
      GOLD_PREFIX               = var.gold_s3_prefix
      YEAR_TO_DATE              = "true"
    }
  }

  depends_on = [aws_cloudwatch_log_group.silver_feature_build]

  tags = var.tags
}

# Gold build Lambda (container: data_pipeline.gold.gold_build — orchestrates archetype
# preprocessing + performance-prediction training matrices in one run)
# Build: docker build --platform linux/amd64 --provenance=false -f docker/gold/Dockerfile -t <ecr_url>:<tag> .
resource "aws_lambda_function" "gold_preprocessing" {
  function_name = "${var.name_prefix}-gold-preprocessing"
  role          = aws_iam_role.gold_preprocessing.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.gold_preprocessing.repository_url}:${var.gold_image_tag}"

  memory_size = var.gold_memory_size
  timeout     = var.gold_timeout

  environment {
    variables = {
      S3_BUCKET      = var.data_lake_bucket_name
      FEATURE_PREFIX = var.silver_s3_prefix
      GOLD_PREFIX    = var.gold_s3_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.gold_preprocessing]

  tags = var.tags
}

