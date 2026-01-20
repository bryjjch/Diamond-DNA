# Local value for bucket ARN (construct from name if not provided)
locals {
  bucket_arn = var.data_lake_bucket_arn != null ? var.data_lake_bucket_arn : "arn:aws:s3:::${var.data_lake_bucket_name}"
}

# CloudWatch Log Group for Batch Job
resource "aws_cloudwatch_log_group" "batch_job" {
  name              = "/aws/batch/${var.name_prefix}-statcast-backfill"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# Security Group for Batch Job (outbound to Statcast API and S3)
resource "aws_security_group" "batch_job" {
  name_prefix = "${var.name_prefix}-batch-backfill-"
  vpc_id      = var.vpc_id
  description = "Security group for Batch backfill job"

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS to Statcast API and S3"
  }

  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP to ECR and other AWS services"
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.name_prefix}-batch-backfill-sg"
    }
  )
}

# IAM Role for Batch Service (to manage compute environment)
resource "aws_iam_role" "batch_service" {
  name = "${var.name_prefix}-batch-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "batch.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# Attach AWS managed policy for Batch service
resource "aws_iam_role_policy_attachment" "batch_service" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

# IAM Role for Batch Job Execution (for Fargate tasks)
resource "aws_iam_role" "batch_execution" {
  name = "${var.name_prefix}-batch-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for Batch Execution (ECR pull, CloudWatch logs)
resource "aws_iam_role_policy" "batch_execution" {
  name = "${var.name_prefix}-batch-execution-policy"
  role = aws_iam_role.batch_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.batch_job.arn}:*"
      }
    ]
  })
}

# IAM Role for Batch Job (task role - for application permissions)
resource "aws_iam_role" "batch_job" {
  name = "${var.name_prefix}-batch-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Role Policy for Batch Job (S3 write access)
resource "aws_iam_role_policy" "batch_job" {
  name = "${var.name_prefix}-batch-job-policy"
  role = aws_iam_role.batch_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${local.bucket_arn}/*",
          local.bucket_arn
        ]
      }
    ]
  })
}

# AWS Batch Compute Environment (Fargate)
resource "aws_batch_compute_environment" "backfill" {
  compute_environment_name = "${var.name_prefix}-statcast-backfill-compute"
  service_role             = aws_iam_role.batch_service.arn
  type                     = "MANAGED"
  state                    = "ENABLED"

  compute_resources {
    type                = "FARGATE"
    max_vcpus           = 256
    security_group_ids  = [aws_security_group.batch_job.id]
    subnets             = var.subnet_ids

    tags = var.tags
  }

  depends_on = [
    aws_iam_role_policy_attachment.batch_service
  ]

  tags = var.tags
}

# AWS Batch Job Queue
resource "aws_batch_job_queue" "backfill" {
  name                 = "${var.name_prefix}-statcast-backfill-queue"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [aws_batch_compute_environment.backfill.arn]

  tags = var.tags
}

# AWS Batch Job Definition
resource "aws_batch_job_definition" "backfill" {
  name                  = "${var.name_prefix}-statcast-backfill-job"
  type                  = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image      = var.container_image_uri
    jobRoleArn = aws_iam_role.batch_job.arn
    executionRoleArn = aws_iam_role.batch_execution.arn

    resourceRequirements = [
      {
        type  = "VCPU"
        value = tostring(var.vcpus)
      },
      {
        type  = "MEMORY"
        value = tostring(var.memory)
      }
    ]

    networkConfiguration = {
      assignPublicIp = "DISABLED"
    }

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.batch_job.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "batch"
      }
    }

    command = [
      "--start-year", tostring(var.job_start_year),
      "--end-year", tostring(var.job_end_year),
      "--s3-bucket", var.data_lake_bucket_name,
      "--s3-prefix", var.s3_prefix
    ]

    environment = []
  })

  tags = var.tags
}

# Data source for current AWS region
data "aws_region" "current" {}
