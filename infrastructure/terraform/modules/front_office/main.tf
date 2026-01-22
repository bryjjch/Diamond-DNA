# DynamoDB Table for User Teams
resource "aws_dynamodb_table" "user_rosters" {
  name           = var.dynamodb_table_name
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = var.dynamodb_partition_key
  range_key      = var.dynamodb_sort_key

  attribute {
    name = var.dynamodb_partition_key
    type = "S"
  }

  attribute {
    name = var.dynamodb_sort_key
    type = "S"
  }

  attribute {
    name = "Name"
    type = "S"
  }

  attribute {
    name = "UserID"
    type = "S"
  }

  attribute {
    name = "Pos"
    type = "S"
  }

  # GSI: "Show me User123's rosters"
  global_secondary_index {
    name               = "UserRostersIndex"
    hash_key           = "UserID"     # PK = USER#123
    range_key          = "RosterID"   # SK = ROSTER#uuid
    projection_type    = "INCLUDE"
    non_key_attributes = ["RosterName"]
  }

  point_in_time_recovery {
    enabled = var.dynamodb_enable_pitr
  }

  tags = var.tags
}

# CloudWatch Log group for Search Lambda Function
resource "aws_cloudwatch_log_group" "search" {
  name              = "/aws/lambda/${var.search_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# IAM Role for Search Lambda
resource "aws_iam_role" "search" {
  name = "${var.search_function_name}-role"

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

# IAM Role Policy for Search Lambda
resource "aws_iam_role_policy" "search" {
  name = "${var.search_function_name}-policy"
  role = aws_iam_role.search.id

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
          "es:ESHttpGet",
          "es:DescribeElasticsearchDomain",
          "es:DescribeDomain"
        ]
        Resource = "${var.opensearch_domain_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [var.opensearch_credentials_secret_arn]
      }
    ]
  })
}

# Search Lambda Function (k-NN search in OpenSearch)
resource "aws_lambda_function" "search" {
  filename         = var.search_zip_path != null ? var.search_zip_path : null
  function_name    = var.search_function_name
  role             = aws_iam_role.search.arn
  handler          = var.search_handler
  runtime          = var.lambda_runtime
  timeout          = var.search_timeout
  memory_size      = var.search_memory_size

  source_code_hash = var.search_zip_path != null ? filebase64sha256(var.search_zip_path) : null

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda_opensearch.id]
  }

  environment {
    variables = {
      OPENSEARCH_ENDPOINT               = var.opensearch_endpoint
      OPENSEARCH_USERNAME               = var.opensearch_username
      OPENSEARCH_CREDENTIALS_SECRET_ARN = var.opensearch_credentials_secret_arn
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.search,
    aws_iam_role_policy.search
  ]

  tags = var.tags
}

# CloudWatch Log Group for Simulation Lambda Function
resource "aws_cloudwatch_log_group" "simulation" {
  name              = "/aws/lambda/${var.simulation_function_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# IAM Role for Simulation Lambda
resource "aws_iam_role" "simulation" {
  name = "${var.simulation_function_name}-role"

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

# IAM Role Policy for Simulation Lambda
resource "aws_iam_role_policy" "simulation" {
  name = "${var.simulation_function_name}-policy"
  role = aws_iam_role.simulation.id

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
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.user_rosters.arn,
          "${aws_dynamodb_table.user_rosters.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "es:ESHttpPost",
          "es:ESHttpGet",
          "es:DescribeElasticsearchDomain",
          "es:DescribeDomain"
        ]
        Resource = "${var.opensearch_domain_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [var.opensearch_credentials_secret_arn]
      }
    ]
  })
}

# Lambda Function: Simulation (Monte Carlo engine)
resource "aws_lambda_function" "simulation" {
  filename         = var.simulation_zip_path != null ? var.simulation_zip_path : null
  function_name    = var.simulation_function_name
  role             = aws_iam_role.simulation.arn
  handler          = var.simulation_handler
  runtime          = var.lambda_runtime
  timeout          = var.simulation_timeout
  memory_size      = var.simulation_memory_size

  source_code_hash = var.simulation_zip_path != null ? filebase64sha256(var.simulation_zip_path) : null

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda_opensearch.id]
  }

  environment {
    variables = {
      DYNAMODB_TABLE_NAME              = aws_dynamodb_table.user_rosters.name
      OPENSEARCH_ENDPOINT              = var.opensearch_endpoint
      OPENSEARCH_USERNAME              = var.opensearch_username
      OPENSEARCH_CREDENTIALS_SECRET_ARN = var.opensearch_credentials_secret_arn
      XGBOOST_MODEL_PATH               = var.xgboost_model_path
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.simulation,
    aws_iam_role_policy.simulation
  ]

  tags = var.tags
}

# Security Group for Lambda accessing OpenSearch
resource "aws_security_group" "lambda_opensearch" {
  name_prefix = "${var.name_prefix}-lambda-opensearch-"
  vpc_id      = var.vpc_id
  description = "Security group for Lambda functions accessing OpenSearch"

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS to OpenSearch"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound for general access"
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-lambda-opensearch-sg"
    }
  )
}

# API Gateway
resource "aws_api_gateway_rest_api" "main" {
  name        = var.api_name
  description = "API Gateway for DiamondDNA vector search application"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = var.tags
}

# API Gateway Resource: /find-similar
resource "aws_api_gateway_resource" "find_similar" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "find-similar"
}

# API Gateway Method: GET /find-similar
resource "aws_api_gateway_method" "find_similar" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.find_similar.id
  http_method   = "GET"
  authorization = "NONE"
}

# API Gateway Integration: Search Lambda
resource "aws_api_gateway_integration" "find_similar" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.find_similar.id
  http_method = aws_api_gateway_method.find_similar.http_method

  integration_http_method = "GET"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.search.invoke_arn
}

# API Gateway Method: OPTIONS (CORS)
# (This is needed as our Next.js frontend will be on a different origin 
# than our AWS APi Gateway).
resource "aws_api_gateway_method" "find_similar_options" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.find_similar.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

# API Gateway Integration: OPTIONS
resource "aws_api_gateway_integration" "find_similar_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.find_similar.id
  http_method = aws_api_gateway_method.find_similar_options.http_method

  type = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

# API Gateway Method Response: OPTIONS
resource "aws_api_gateway_method_response" "find_similar_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.find_similar.id
  http_method = aws_api_gateway_method.find_similar_options.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

# API Gateway Integration Response: OPTIONS
resource "aws_api_gateway_integration_response" "find_similar_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.find_similar.id
  http_method = aws_api_gateway_method.find_similar_options.http_method
  status_code = aws_api_gateway_method_response.find_similar_options.status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }

  depends_on = [aws_api_gateway_integration.find_similar_options]
}

# API Gateway Resource: /simulate
resource "aws_api_gateway_resource" "simulate" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "simulate"
}

# API Gateway Method: POST /simulate
resource "aws_api_gateway_method" "simulate" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.simulate.id
  http_method   = "POST"
  authorization = "NONE"
}

# API Gateway Integration: Simulation Lambda
resource "aws_api_gateway_integration" "simulate" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.simulate.id
  http_method = aws_api_gateway_method.simulate.http_method

  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.simulation.invoke_arn
}

# API Gateway Method: OPTIONS (CORS)
# (This is needed as our Next.js frontend will be on a different origin 
# than our AWS API Gateway).
resource "aws_api_gateway_method" "simulate_options" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.simulate.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

# API Gateway Integration: OPTIONS
resource "aws_api_gateway_integration" "simulate_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.simulate.id
  http_method = aws_api_gateway_method.simulate_options.http_method

  type = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

# API Gateway Method Response: OPTIONS
resource "aws_api_gateway_method_response" "simulate_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.simulate.id
  http_method = aws_api_gateway_method.simulate_options.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

# API Gateway Integration Response: OPTIONS
resource "aws_api_gateway_integration_response" "simulate_options" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.simulate.id
  http_method = aws_api_gateway_method.simulate_options.http_method
  status_code = aws_api_gateway_method_response.simulate_options.status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'POST,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }

  depends_on = [aws_api_gateway_integration.simulate_options]
}

# Lambda Permissions for API Gateway
resource "aws_lambda_permission" "search_api" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.search.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/*"
}

resource "aws_lambda_permission" "simulation_api" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.simulation.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/*"
}

# API Gateway Deployment
resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id

  triggers = {
    redeployment = sha1(jsonencode([
      # find_similar methods
      aws_api_gateway_resource.find_similar.id,
      aws_api_gateway_method.find_similar.id,
      aws_api_gateway_integration.find_similar.id,

      # find_similar OPTIONS methods
      aws_api_gateway_method.find_similar_options.id,
      aws_api_gateway_integration.find_similar_options.id,
      aws_api_gateway_method_response.find_similar_options.id,
      aws_api_gateway_integration_response.find_similar_options.id,

      # simulate methods
      aws_api_gateway_resource.simulate.id,
      aws_api_gateway_method.simulate.id,
      aws_api_gateway_integration.simulate.id,

      # simulate OPTIONS methods
      aws_api_gateway_method.simulate_options.id,
      aws_api_gateway_integration.simulate_options.id,
      aws_api_gateway_method_response.simulate_options.id,
      aws_api_gateway_integration_response.simulate_options.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_method.find_similar,
    aws_api_gateway_integration.find_similar,
    aws_api_gateway_method.find_similar_options,
    aws_api_gateway_integration.find_similar_options,
    aws_api_gateway_method_response.find_similar_options,
    aws_api_gateway_integration_response.find_similar_options,
    aws_api_gateway_method.simulate,
    aws_api_gateway_integration.simulate,
    aws_api_gateway_method.simulate_options,
    aws_api_gateway_integration.simulate_options,
    aws_api_gateway_method_response.simulate_options,
    aws_api_gateway_integration_response.simulate_options
  ]
}

# API Gateway Stage
resource "aws_api_gateway_stage" "main" {
  deployment_id = aws_api_gateway_deployment.main.id
  rest_api_id   = aws_api_gateway_rest_api.main.id
  stage_name    = var.api_stage_name

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      requestTime    = "$context.requestTime"
      httpMethod     = "$context.httpMethod"
      routeKey       = "$context.routeKey"
      status         = "$context.status"
      protocol       = "$context.protocol"
      responseLength = "$context.responseLength"
    })
  }

  tags = var.tags
}

# CloudWatch Log Group for API Gateway
resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${var.api_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# API Gateway Account Settings (for CloudWatch Logs)
resource "aws_api_gateway_account" "main" {
  cloudwatch_role_arn = aws_iam_role.api_gateway_cloudwatch.arn
}

# IAM Role for API Gateway CloudWatch Logs
resource "aws_iam_role" "api_gateway_cloudwatch" {
  name = "${var.api_name}-cloudwatch-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "api_gateway_cloudwatch" {
  role       = aws_iam_role.api_gateway_cloudwatch.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}