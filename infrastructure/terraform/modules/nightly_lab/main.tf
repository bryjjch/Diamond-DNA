# S3 Buckets
module "s3" {
  source = "../s3"

  data_lake_bucket_name       = var.data_lake_bucket_name
  model_artifacts_bucket_name = var.model_artifacts_bucket_name
  tags                        = var.tags
}

# CloudWatch Log Group for Scraper Lambda Function
resource "aws_cloudwatch_log_group" "scraper" {
  name              = "/aws/lambda/${var.scraper_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# IAM Role for Scraper Lambda Function
resource "aws_iam_role" "scraper" {
  name = "${var.scraper_function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for Scraper Lambda Function
resource "aws_iam_role_policy" "scraper" {
  name = "${var.scraper_function_name}-policy"
  role = aws_iam_role.scraper.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${module.s3.data_lake_bucket_arn}/*",
          module.s3.data_lake_bucket_arn
        ]
      }
    ]
  })
}


# Lambda Function: Scrape MLB API
resource "aws_lambda_function" "scraper" {
  filename      = var.scraper_zip_path != null ? var.scraper_zip_path : null
  function_name = var.scraper_function_name
  role          = aws_iam_role.scraper.arn
  handler       = var.scraper_handler
  runtime       = var.lambda_runtime
  timeout       = var.scraper_timeout
  memory_size   = var.scraper_memory_size

  source_code_hash = var.scraper_zip_path != null ? filebase64sha256(var.scraper_zip_path) : null

  environment {
    variables = {
      RAW_DATA_BUCKET = module.s3.data_lake_bucket_name
      MLB_API_KEY     = var.mlb_api_key # Should use Secrets Manager in production
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.scraper,
    aws_iam_role_policy.scraper
  ]

  tags = var.tags
}

# EventBridge Rule: Trigger scraper every morning at 8 AM UTC
resource "aws_cloudwatch_event_rule" "nightly_scraper" {
  name                = "${var.scraper_function_name}-nightly-trigger"
  description         = "Trigger MLB scraper every night at 8 AM UTC"
  schedule_expression = "cron(0 8 * * ? *)"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "scraper" {
  rule      = aws_cloudwatch_event_rule.nightly_scraper.name
  target_id = "ScraperLambdaTarget"
  arn       = aws_lambda_function.scraper.arn
}

resource "aws_lambda_permission" "allow_eventbridge_scraper" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.nightly_scraper.arn
}

# IAM Role for SageMaker Processing
resource "aws_iam_role" "sagemaker_processing" {
  name = "${var.name_prefix}-sagemaker-processing-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "sagemaker.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for Full SageMaker Access for Processing
resource "aws_iam_role_policy_attachment" "sagemaker_processing" {
  role       = aws_iam_role.sagemaker_processing.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

# IAM Role Policy for Other Permissions (S3)
resource "aws_iam_role_policy" "sagemaker_processing_s3" {
  name = "${var.name_prefix}-sagemaker-processing-s3-policy"
  role = aws_iam_role.sagemaker_processing.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${module.s3.data_lake_bucket_arn}/*",
          module.s3.data_lake_bucket_arn,
          "${module.s3.model_artifacts_bucket_arn}/*",
          module.s3.model_artifacts_bucket_arn
        ]
      }
    ]
  })
}

# SageMaker Processing Job: Clean data and calculate features
resource "aws_sagemaker_notebook_instance" "processing_notebook" {
  count                 = 0 # Not used, but placeholder for processing job configuration
  name                  = "${var.name_prefix}-processing-notebook"
  instance_type         = "ml.t3.medium"
  role_arn              = aws_iam_role.sagemaker_processing.arn
  lifecycle_config_name = null

  tags = var.tags
}

# IAM Role for SageMaker Training
resource "aws_iam_role" "sagemaker_training" {
  name = "${var.name_prefix}-sagemaker-training-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "sagemaker.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for Full Sagemaker Access for Training
resource "aws_iam_role_policy_attachment" "sagemaker_training" {
  role       = aws_iam_role.sagemaker_training.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

# IAM Role Policy for S3 Access for Sagemaker Training
resource "aws_iam_role_policy" "sagemaker_training_s3" {
  name = "${var.name_prefix}-sagemaker-training-s3-policy"
  role = aws_iam_role.sagemaker_training.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${module.s3.data_lake_bucket_arn}/*",
          module.s3.data_lake_bucket_arn,
          "${module.s3.model_artifacts_bucket_arn}/*",
          module.s3.model_artifacts_bucket_arn
        ]
      }
    ]
  })
}

# SageMaker Training Job: Train player2vec autoencoder
resource "aws_sagemaker_training_job" "player2vec" {
  count             = 0 # Will be triggered by Lambda, placeholder for configuration
  training_job_name = "${var.name_prefix}-player2vec-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  role_arn          = aws_iam_role.sagemaker_training.arn
  algorithm_specification {
    training_input_mode = "File"
    training_image      = var.player2vec_training_image
  }

  input_data_config {
    channel_name = "training"
    data_source {
      s3_data_source {
        s3_data_type              = "S3Prefix"
        s3_uri                    = "s3://${module.s3.data_lake_bucket_name}/${var.clean_data_path}"
        s3_data_distribution_type = "FullyReplicated"
      }
    }
  }

  output_data_config {
    s3_output_path = "s3://${module.s3.model_artifacts_bucket_name}/${var.training_output_path}"
  }

  resource_config {
    instance_count    = var.training_instance_count
    instance_type     = var.training_instance_type
    volume_size_in_gb = 30
  }

  stopping_condition {
    max_runtime_in_seconds = 86400 # 24 hours
  }

  tags = var.tags
}

# SageMaker Training Job: Train XGBoost performance projector
# Similar structure to player2vec, configured separately
resource "aws_sagemaker_training_job" "xgboost_performance" {
  count             = 0 # Will be triggered by Lambda, placeholder for configuration
  training_job_name = "${var.name_prefix}-xgboost-performance-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  role_arn          = aws_iam_role.sagemaker_training.arn
  algorithm_specification {
    training_input_mode = "File"
    training_image      = var.xgboost_training_image
  }

  input_data_config {
    channel_name = "training"
    data_source {
      s3_data_source {
        s3_data_type              = "S3Prefix"
        s3_uri                    = "s3://${module.s3.data_lake_bucket_name}/${var.clean_data_path}"
        s3_data_distribution_type = "FullyReplicated"
      }
    }
  }

  output_data_config {
    s3_output_path = "s3://${module.s3.model_artifacts_bucket_name}/${var.training_output_path}"
  }

  resource_config {
    instance_count    = var.training_instance_count
    instance_type     = var.training_instance_type
    volume_size_in_gb = 30
  }

  stopping_condition {
    max_runtime_in_seconds = 86400 # 24 hours
  }

  tags = var.tags
}

# CloudWatch Log Group: For OpenSearch Indexer Lambda Function
resource "aws_cloudwatch_log_group" "opensearch_indexer" {
  name              = "/aws/lambda/${var.opensearch_indexer_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# IAM Role for OpenSearch Indexer Lambda
resource "aws_iam_role" "opensearch_indexer" {
  name = "${var.opensearch_indexer_function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for OpenSearch Indexer Lambda
resource "aws_iam_role_policy" "opensearch_indexer" {
  name = "${var.opensearch_indexer_function_name}-policy"
  role = aws_iam_role.opensearch_indexer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "es:ESHttpPost",
          "es:ESHttpPut",
          "es:DescribeElasticsearchDomain",
          "es:DescribeDomain",
          "es:ESHttpGet"
        ]
        Resource = "${var.opensearch_domain_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${module.s3.model_artifacts_bucket_arn}/*",
          module.s3.model_artifacts_bucket_arn
        ]
      }
    ]
  })
}

# Lambda Function: Index vectors to OpenSearch
resource "aws_lambda_function" "opensearch_indexer" {
  filename      = var.opensearch_indexer_zip_path != null ? var.opensearch_indexer_zip_path : null
  function_name = var.opensearch_indexer_function_name
  role          = aws_iam_role.opensearch_indexer.arn
  handler       = var.opensearch_indexer_handler
  runtime       = var.lambda_runtime
  timeout       = var.opensearch_indexer_timeout
  memory_size   = var.opensearch_indexer_memory_size

  source_code_hash = var.opensearch_indexer_zip_path != null ? filebase64sha256(var.opensearch_indexer_zip_path) : null

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda_opensearch.id]
  }

  environment {
    variables = {
      OPENSEARCH_ENDPOINT = var.opensearch_endpoint
      OPENSEARCH_USERNAME = var.opensearch_username
      OPENSEARCH_PASSWORD = var.opensearch_password
      VECTORS_S3_BUCKET   = module.s3.model_artifacts_bucket_name
      VECTORS_S3_PATH     = "${var.training_output_path}/vectors/"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.opensearch_indexer,
    aws_iam_role_policy.opensearch_indexer
  ]

  tags = var.tags
}

# Security Group for Lambda accessing OpenSearch
resource "aws_security_group" "lambda_opensearch" {
  name_prefix = "${var.name_prefix}-lambda-opensearch-"
  vpc_id      = var.vpc_id
  description = "Security group for Lambda accessing OpenSearch"

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS to OpenSearch"
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-lambda-opensearch-sg"
    }
  )
}

# EventBridge Rule: Trigger OpenSearch indexing after training completes
# This would typically be triggered by CloudWatch Events from SageMaker
resource "aws_cloudwatch_event_rule" "trigger_indexing" {
  name        = "${var.name_prefix}-trigger-opensearch-indexing"
  description = "Trigger OpenSearch indexing after training completes"

  event_pattern = jsonencode({
    source      = ["aws.sagemaker"]
    detail-type = ["SageMaker Training Job State Change"]
    detail = {
      TrainingJobStatus = ["Completed"]
    }
  })

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "opensearch_indexer" {
  rule      = aws_cloudwatch_event_rule.trigger_indexing.name
  target_id = "OpenSearchIndexerTarget"
  arn       = aws_lambda_function.opensearch_indexer.arn
}

resource "aws_lambda_permission" "allow_eventbridge_indexer" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.opensearch_indexer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_indexing.arn
}