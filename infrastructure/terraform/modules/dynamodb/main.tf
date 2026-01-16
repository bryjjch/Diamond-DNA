terraform {
  required_version = ">= 1.0"
}

resource "aws_dynamodb_table" "batter_profiles" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = var.partition_key
  range_key = var.sort_key

  attribute {
    name = var.partition_key
    type = "S"
  }

  attribute {
    name = var.sort_key
    type = "S"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn != null ? var.kms_key_arn : null
  }

  point_in_time_recovery {
    enabled = var.enable_point_in_time_recovery
  }

  tags = merge(
    var.tags,
    {
      Name = var.table_name
    }
  )
}
