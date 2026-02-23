# ECR repository for the daily Statcast Lambda container image
resource "aws_ecr_repository" "daily_statcast" {
  name                 = "${var.name_prefix}-daily-statcast"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# CloudWatch log group for the Lambda
resource "aws_cloudwatch_log_group" "daily_statcast" {
  name              = "/aws/lambda/${var.name_prefix}-daily-statcast"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# IAM role for the Lambda function
resource "aws_iam_role" "daily_statcast" {
  name = "${var.name_prefix}-daily-statcast-lambda"

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
  role       = aws_iam_role.daily_statcast.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 write access to the data lake bucket (Statcast prefix)
resource "aws_iam_role_policy" "s3_put" {
  name = "${var.name_prefix}-daily-statcast-s3"
  role = aws_iam_role.daily_statcast.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.s3_prefix}/*"
      }
    ]
  })
}

# Lambda function (container image)
resource "aws_lambda_function" "daily_statcast" {
  function_name = "${var.name_prefix}-daily-statcast"
  role          = aws_iam_role.daily_statcast.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.daily_statcast.repository_url}:${var.image_tag}"

  memory_size = var.memory_size
  timeout     = var.timeout

  environment {
    variables = {
      S3_BUCKET = var.data_lake_bucket_name
      S3_PREFIX = var.s3_prefix
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.daily_statcast
  ]

  tags = var.tags
}

# EventBridge rule to run daily
resource "aws_cloudwatch_event_rule" "daily_statcast" {
  name                = "${var.name_prefix}-daily-statcast-schedule"
  description         = "Trigger daily Statcast ingestion at 6 AM UTC"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

# Target the Lambda
resource "aws_cloudwatch_event_target" "daily_statcast" {
  rule      = aws_cloudwatch_event_rule.daily_statcast.name
  target_id = "DailyStatcastLambda"
  arn       = aws_lambda_function.daily_statcast.arn
}

# Allow EventBridge to invoke the Lambda
resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_statcast.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_statcast.arn
}
