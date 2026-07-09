# ECR repository for the HTTP API Lambda image
resource "aws_ecr_repository" "api" {
  name                 = "${var.name_prefix}-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# IAM role for the API Lambda
resource "aws_iam_role" "api" {
  name = "${var.name_prefix}-api-lambda"

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

# CloudWatch Logs basic execution
resource "aws_iam_role_policy_attachment" "api_lambda_basic" {
  role       = aws_iam_role.api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Read-only access to the gold parquet tables this API serves
resource "aws_iam_role_policy" "api_s3_read" {
  name = "${var.name_prefix}-api-s3-read"
  role = aws_iam_role.api.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "${var.data_lake_bucket_arn}/${var.gold_s3_prefix}/*"
      }
    ]
  })
}

# CloudWatch log group for the API Lambda
resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.name_prefix}-api"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# API Lambda (container image from ECR)
# Build: docker build --platform linux/amd64 --provenance=false -f docker/api/Dockerfile -t <ecr_url>:<tag> .
# Push:  aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#        docker push <ecr_url>:<tag>
resource "aws_lambda_function" "api" {
  function_name = "${var.name_prefix}-api"
  role          = aws_iam_role.api.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.api.repository_url}:${var.image_tag}"

  memory_size = var.memory_size
  timeout     = var.timeout

  environment {
    variables = merge(
      {
        S3_BUCKET   = var.data_lake_bucket_name
        GOLD_PREFIX = var.gold_s3_prefix
      },
      var.webapp_year == "" ? {} : { WEBAPP_YEAR = var.webapp_year }
    )
  }

  depends_on = [aws_cloudwatch_log_group.api]

  tags = var.tags
}

# HTTP API Gateway (v2) — cheaper and lower-latency than REST API for this use case
resource "aws_apigatewayv2_api" "api" {
  name          = "${var.name_prefix}-api"
  protocol_type = "HTTP"
  description   = "xWAR Engine HTTP API (cluster browser + KNN neighbors)"

  cors_configuration {
    allow_origins = var.cors_allow_origins
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["content-type"]
    max_age       = 3600
  }

  tags = var.tags
}

# Lambda integration for the HTTP API (proxy mode forwards the full event)
resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

# Catch-all route — the Lambda handler does its own path dispatch
resource "aws_apigatewayv2_route" "default" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

# Auto-deploy stage so changes go live without manual deployment
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  tags = var.tags
}

# Allow API Gateway to invoke the Lambda
resource "aws_lambda_permission" "api_gateway_invoke" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
