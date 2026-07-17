# ECR repository for the bronze Statcast pitch ingestion Lambda image
resource "aws_ecr_repository" "statcast_ingestion" {
  name                 = "${var.name_prefix}-statcast-ingestion"
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
resource "aws_iam_role" "statcast_ingestion" {
  name = "${var.name_prefix}-statcast-ingestion-lambda"

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
resource "aws_iam_role_policy_attachment" "statcast_ingestion_lambda_basic" {
  role       = aws_iam_role.statcast_ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 write access for bronze ingestion Lambda (statcast pitches + running + defence prefixes)
resource "aws_iam_role_policy" "statcast_ingestion_s3_access" {
  name = "${var.name_prefix}-statcast-ingestion-s3"
  role = aws_iam_role.statcast_ingestion.id

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
resource "aws_cloudwatch_log_group" "statcast_ingestion" {
  name              = "/aws/lambda/${var.name_prefix}-statcast-ingestion"
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

# CloudWatch log group for the silver standard stat-line build Lambda
resource "aws_cloudwatch_log_group" "silver_standard_stats" {
  name              = "/aws/lambda/${var.name_prefix}-silver-standard-stats"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# Bronze ingestion Lambda (container image from ECR)
# Build: docker build --platform linux/amd64 --provenance=false -f docker/bronze/Dockerfile -t <ecr_url>:<tag> .
# Push:  aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#        docker push <ecr_url>:<tag>
resource "aws_lambda_function" "statcast_ingestion" {
  function_name = "${var.name_prefix}-statcast-ingestion"
  role          = aws_iam_role.statcast_ingestion.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.statcast_ingestion.repository_url}:${var.image_tag}"

  memory_size = var.memory_size
  timeout     = var.timeout

  environment {
    variables = {
      S3_BUCKET                = var.data_lake_bucket_name
      S3_PREFIX                = var.s3_prefix
      RAW_RUNNING_PREFIX       = var.raw_running_s3_prefix
      RAW_DEFENCE_PREFIX       = var.raw_defence_s3_prefix
      RAW_BIO_PREFIX           = var.raw_bio_s3_prefix
      RAW_STANDARD_STATS_PREFIX = var.raw_standard_stats_s3_prefix
      PYBASEBALL_NO_CACHE      = "true"
      HOME                     = "/tmp"
    }
  }

  depends_on = [aws_cloudwatch_log_group.statcast_ingestion]

  tags = var.tags
}

# Silver feature build Lambda (container: data_pipeline.silver.archetype_features.silver_features_build)
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
      S3_BUCKET                = var.data_lake_bucket_name
      RAW_PREFIX               = var.s3_prefix
      RAW_RUNNING_PREFIX       = var.raw_running_s3_prefix
      RAW_DEFENCE_PREFIX       = var.raw_defence_s3_prefix
      RAW_BIO_PREFIX           = var.raw_bio_s3_prefix
      RAW_STANDARD_STATS_PREFIX = var.raw_standard_stats_s3_prefix
      FEATURE_PREFIX           = var.silver_s3_prefix
      GOLD_PREFIX              = var.gold_s3_prefix
      YEAR_TO_DATE             = "true"
    }
  }

  depends_on = [aws_cloudwatch_log_group.silver_feature_build]

  tags = var.tags
}

# Silver standard stat-line build Lambda.
# Reuses the silver feature-build container image, overriding the entrypoint command to
# the standard-stats handler. Produces the standalone standard stat-line silver table.
resource "aws_lambda_function" "silver_standard_stats" {
  function_name = "${var.name_prefix}-silver-standard-stats"
  role          = aws_iam_role.silver_feature_build.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.silver_feature_build.repository_url}:${var.silver_image_tag}"

  image_config {
    command = ["src.data_pipeline.silver.standard_stats.silver_standard_player_year_stats.handler"]
  }

  memory_size = var.silver_memory_size
  timeout     = var.silver_timeout

  environment {
    variables = {
      S3_BUCKET                 = var.data_lake_bucket_name
      RAW_STANDARD_STATS_PREFIX = var.raw_standard_stats_s3_prefix
      FEATURE_PREFIX            = var.silver_s3_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.silver_standard_stats]

  tags = var.tags
}

# Gold preprocessing Lambda (container: data_pipeline.gold.gold_archetype_preprocessing)
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
      S3_BUCKET     = var.data_lake_bucket_name
      SILVER_PREFIX = var.silver_s3_prefix
      GOLD_PREFIX   = var.gold_s3_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.gold_preprocessing]

  tags = var.tags
}

# EventBridge rule for bronze ingestion (daily)
resource "aws_cloudwatch_event_rule" "statcast_ingestion" {
  name                = "${var.name_prefix}-statcast-ingestion-schedule"
  description         = "Trigger bronze ingestion (daily: statcast pitches for yesterday UTC + running/defence for the current season)"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "statcast_ingestion" {
  rule      = aws_cloudwatch_event_rule.statcast_ingestion.name
  target_id = "StatcastIngestionLambda"
  arn       = aws_lambda_function.statcast_ingestion.arn
}

resource "aws_lambda_permission" "statcast_ingestion_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.statcast_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.statcast_ingestion.arn
}

# EventBridge rule for silver feature build (daily, offset after bronze)
resource "aws_cloudwatch_event_rule" "silver_feature_build" {
  name                = "${var.name_prefix}-silver-feature-build-schedule"
  description         = "Trigger silver feature build (daily: YTD bronze to silver player-year features)"
  schedule_expression = var.silver_schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "silver_feature_build" {
  rule      = aws_cloudwatch_event_rule.silver_feature_build.name
  target_id = "SilverFeatureBuildLambda"
  arn       = aws_lambda_function.silver_feature_build.arn
}

resource "aws_lambda_permission" "silver_feature_build_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeSilver"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_feature_build.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.silver_feature_build.arn
}

# EventBridge target for silver standard stat-line build (reuses the silver schedule;
# it also reads bronze and writes silver, with no ordering dependency on feature build).
resource "aws_cloudwatch_event_target" "silver_standard_stats" {
  rule      = aws_cloudwatch_event_rule.silver_feature_build.name
  target_id = "SilverStandardStatsLambda"
  arn       = aws_lambda_function.silver_standard_stats.arn
}

resource "aws_lambda_permission" "silver_standard_stats_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeSilverStandardStats"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_standard_stats.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.silver_feature_build.arn
}

# EventBridge rule for gold preprocessing (daily, offset after silver)
resource "aws_cloudwatch_event_rule" "gold_preprocessing" {
  name                = "${var.name_prefix}-gold-preprocessing-schedule"
  description         = "Trigger gold preprocessing (daily: silver to gold ML-ready features)"
  schedule_expression = var.gold_schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "gold_preprocessing" {
  rule      = aws_cloudwatch_event_rule.gold_preprocessing.name
  target_id = "GoldPreprocessingLambda"
  arn       = aws_lambda_function.gold_preprocessing.arn
}

resource "aws_lambda_permission" "gold_preprocessing_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeGold"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gold_preprocessing.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.gold_preprocessing.arn
}
