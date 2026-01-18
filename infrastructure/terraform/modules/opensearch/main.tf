# Data source to fetch password from Secrets Manager if secret ARN is provided
data "aws_secretsmanager_secret_version" "master_password" {
  count     = var.master_user_password_secret_arn != null ? 1 : 0
  secret_id = var.master_user_password_secret_arn
}

# Random password for master user if not provided
resource "random_password" "master_password" {
  count   = var.master_user_password == null && var.master_user_password_secret_arn == null ? 1 : 0
  length  = 32
  special = true
}

# Local values to extract password and username from secret
locals {
  secret_data = var.master_user_password_secret_arn != null ? jsondecode(data.aws_secretsmanager_secret_version.master_password[0].secret_string) : {}
  secret_password = var.master_user_password_secret_arn != null ? try(local.secret_data.password, local.secret_data.api_key, null) : null
  secret_username = var.master_user_password_secret_arn != null ? try(local.secret_data.username, null) : null
  
  # Determine which password to use: secret > provided > generated
  final_password = coalesce(
    local.secret_password,
    var.master_user_password,
    try(random_password.master_password[0].result, null)
  )
  
  # Determine which username to use: secret > provided
  final_username = coalesce(
    local.secret_username,
    var.master_user_name
  )
}

resource "aws_opensearch_domain" "main" {
  domain_name    = var.domain_name
  engine_version = var.engine_version

  cluster_config {
    instance_type            = var.instance_type
    instance_count           = var.instance_count
    dedicated_master_enabled = false
    zone_awareness_enabled   = var.zone_awareness_enabled

    dynamic "zone_awareness_config" {
      for_each = var.zone_awareness_enabled ? [1] : []
      content {
        availability_zone_count = var.availability_zone_count
      }
    }
  }

  ebs_options {
    ebs_enabled = var.ebs_enabled
    volume_size = var.volume_size
    volume_type = var.volume_type
  }

  vpc_options {
    subnet_ids         = var.subnet_ids
    security_group_ids = length(var.security_group_ids) > 0 ? var.security_group_ids : [aws_security_group.opensearch.id]
  }

  node_to_node_encryption {
    enabled = var.node_to_node_encryption
  }

  encrypt_at_rest {
    enabled = var.encrypt_at_rest
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  advanced_security_options {
    enabled                        = true
    internal_user_database_enabled = true
    master_user_options {
      master_user_name     = local.final_username
      master_user_password = local.final_password
    }
  }

  log_publishing_options {
    cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch_index_slow.arn
    log_type                 = "INDEX_SLOW_LOGS"
  }

  log_publishing_options {
    cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch_search_slow.arn
    log_type                 = "SEARCH_SLOW_LOGS"
  }

  tags = merge(
    var.tags,
    {
      Name = var.domain_name
    }
  )

  depends_on = [
    aws_iam_service_linked_role.opensearch
  ]
}

# CloudWatch Log Groups for OpenSearch logging
resource "aws_cloudwatch_log_group" "opensearch_index_slow" {
  name              = "/aws/opensearch/${var.domain_name}/index-slow"
  retention_in_days = 7

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "opensearch_search_slow" {
  name              = "/aws/opensearch/${var.domain_name}/search-slow"
  retention_in_days = 7

  tags = var.tags
}

# Security Group for OpenSearch
resource "aws_security_group" "opensearch" {
  name_prefix = "${var.domain_name}-opensearch-"
  vpc_id      = var.vpc_id
  description = "Security group for OpenSearch domain"

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.main.cidr_block]
    description = "HTTPS from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.domain_name}-opensearch-sg"
    }
  )
}

# VPC data source
data "aws_vpc" "main" {
  id = var.vpc_id
}

# IAM Service-Linked Role for OpenSearch
resource "aws_iam_service_linked_role" "opensearch" {
  count            = var.create_service_linked_role ? 1 : 0
  aws_service_name = "opensearchservice.amazonaws.com"
  description      = "Service-linked role for OpenSearch"
}