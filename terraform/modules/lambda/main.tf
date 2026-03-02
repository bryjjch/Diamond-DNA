# ECR repository for the Statcast ingestion Lambda container image
resource "aws_ecr_repository" "statcast_ingestion" {
  name                 = "${var.name_prefix}-statcast-ingestion"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# CloudWatch log group for the Lambda
resource "aws_cloudwatch_log_group" "statcast_ingestion" {
  name              = "/aws/lambda/${var.name_prefix}-statcast-ingestion"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# IAM role for the Lambda function
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

# Basic Lambda execution: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.statcast_ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 write access to the data lake bucket (Statcast prefix)
resource "aws_iam_role_policy" "s3_put" {
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
        Resource = "${var.data_lake_bucket_arn}/${var.s3_prefix}/*"
      }
    ]
  })
}

# Lambda function (container image from ECR)
# Image must be built from repo root: docker build -f docker/statcast-ingestion/Dockerfile -t <ecr_repo_url>:<tag> .
# Then push: aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#            docker push <ecr_repo_url>:<tag>
# Use statcast_ingestion_image_tag (e.g. latest) so image_uri matches.
resource "aws_lambda_function" "statcast_ingestion" {
  function_name = "${var.name_prefix}-statcast-ingestion"
  role          = aws_iam_role.statcast_ingestion.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.statcast_ingestion.repository_url}:${var.image_tag}"

  memory_size = var.memory_size
  timeout     = var.timeout

  environment {
    variables = {
      S3_BUCKET = var.data_lake_bucket_name
      S3_PREFIX = var.s3_prefix
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.statcast_ingestion
  ]

  tags = var.tags
}

# EventBridge rule to run daily
resource "aws_cloudwatch_event_rule" "statcast_ingestion" {
  name                = "${var.name_prefix}-statcast-ingestion-schedule"
  description         = "Trigger Statcast ingestion (daily run: yesterday's data)"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

# Target the Lambda
resource "aws_cloudwatch_event_target" "statcast_ingestion" {
  rule      = aws_cloudwatch_event_rule.statcast_ingestion.name
  target_id = "StatcastIngestionLambda"
  arn       = aws_lambda_function.statcast_ingestion.arn
}

# Allow EventBridge to invoke the Lambda
resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.statcast_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.statcast_ingestion.arn
}
