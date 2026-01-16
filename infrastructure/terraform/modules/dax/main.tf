terraform {
  required_version = ">= 1.0"
}

# DAX Subnet Group
resource "aws_dax_subnet_group" "main" {
  name       = "${var.cluster_name}-subnet-group"
  subnet_ids = var.subnet_ids

  tags = merge(
    var.tags,
    {
      Name = "${var.cluster_name}-subnet-group"
    }
  )
}

# DAX IAM Role
resource "aws_iam_role" "dax" {
  name = "${var.cluster_name}-dax-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "dax.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# DAX IAM Policy
resource "aws_iam_role_policy" "dax" {
  name = "${var.cluster_name}-dax-policy"
  role = aws_iam_role.dax.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable",
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:ConditionCheckItem"
        ]
        Resource = var.dynamodb_table_arn
      }
    ]
  })
}

# Security Group for DAX
resource "aws_security_group" "dax" {
  name_prefix = "${var.cluster_name}-dax-"
  vpc_id      = var.vpc_id
  description = "Security group for DAX cluster ${var.cluster_name}"

  ingress {
    description     = "DAX cluster port"
    from_port       = 8111
    to_port         = 8111
    protocol        = "tcp"
    security_groups = var.allowed_security_group_ids
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.cluster_name}-dax-sg"
    }
  )
}

# DAX Parameter Group (only created if parameters are specified)
resource "aws_dax_parameter_group" "main" {
  count       = (var.query_ttl_millis != null || var.record_ttl_millis != null) ? 1 : 0
  name        = "${var.cluster_name}-params"
  description = "Parameter group for ${var.cluster_name}"

  dynamic "parameters" {
    for_each = var.query_ttl_millis != null ? [1] : []
    content {
      name  = "query-ttl-millis"
      value = var.query_ttl_millis
    }
  }

  dynamic "parameters" {
    for_each = var.record_ttl_millis != null ? [1] : []
    content {
      name  = "record-ttl-millis"
      value = var.record_ttl_millis
    }
  }
}

# DAX Cluster
resource "aws_dax_cluster" "main" {
  cluster_name       = var.cluster_name
  iam_role_arn       = aws_iam_role.dax.arn
  node_type          = var.node_type
  replication_factor = var.replication_factor
  subnet_group_name  = aws_dax_subnet_group.main.name
  security_group_ids = [aws_security_group.dax.id]
  parameter_group_name = length(aws_dax_parameter_group.main) > 0 ? aws_dax_parameter_group.main[0].name : null

  server_side_encryption {
    enabled = var.enable_encryption
  }

  tags = merge(
    var.tags,
    {
      Name = var.cluster_name
    }
  )
}
