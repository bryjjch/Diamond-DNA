terraform {
  required_version = ">= 1.0"
}

# CloudWatch Log Group for Orchestrator
resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/${var.orchestrator_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# Lambda Function - Orchestrator
resource "aws_lambda_function" "orchestrator" {
  filename         = var.orchestrator_zip_path != null ? var.orchestrator_zip_path : null
  function_name    = var.orchestrator_function_name
  role            = aws_iam_role.orchestrator.arn
  handler         = var.orchestrator_handler
  runtime         = var.runtime
  timeout         = var.orchestrator_timeout
  memory_size     = var.orchestrator_memory_size

  source_code_hash = var.orchestrator_zip_path != null ? filebase64sha256(var.orchestrator_zip_path) : null

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  environment {
    variables = {
      DYNAMODB_TABLE_NAME    = var.dynamodb_table_name
      DAX_CLUSTER_ENDPOINT   = var.dax_cluster_endpoint
      SAGEMAKER_ENDPOINT_NAME = var.sagemaker_endpoint_name
      MODEL_ARTIFACTS_BUCKET = var.model_artifacts_bucket_name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.orchestrator,
    aws_iam_role_policy.orchestrator
  ]

  tags = var.tags
}

# CloudWatch Log Group for Ingestion
resource "aws_cloudwatch_log_group" "ingestion" {
  name              = "/aws/lambda/${var.ingestion_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# Lambda Function - Ingestion
resource "aws_lambda_function" "ingestion" {
  filename         = var.ingestion_zip_path != null ? var.ingestion_zip_path : null
  function_name    = var.ingestion_function_name
  role            = aws_iam_role.ingestion.arn
  handler         = var.ingestion_handler
  runtime         = var.runtime
  timeout         = var.ingestion_timeout
  memory_size     = var.ingestion_memory_size

  source_code_hash = var.ingestion_zip_path != null ? filebase64sha256(var.ingestion_zip_path) : null

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
      KINESIS_STREAM_NAME = var.kinesis_stream_name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.ingestion,
    aws_iam_role_policy.ingestion
  ]

  tags = var.tags
}

# Event Source Mapping for Ingestion Lambda (Kinesis)
resource "aws_lambda_event_source_mapping" "kinesis_ingestion" {
  event_source_arn  = var.kinesis_stream_arn
  function_name     = aws_lambda_function.ingestion.arn
  starting_position = "LATEST"

  batch_size                         = var.kinesis_batch_size
  maximum_batching_window_in_seconds = var.kinesis_maximum_batching_window
  parallelization_factor             = var.kinesis_parallelization_factor

  depends_on = [aws_lambda_function.ingestion]
}
