terraform {
  required_version = ">= 1.0"
}

resource "aws_kinesis_stream" "main" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_period_hours

  encryption_type = var.kms_key_id != null ? "KMS" : "NONE"
  kms_key_id      = var.kms_key_id

  shard_level_metrics = var.shard_level_metrics

  tags = merge(
    var.tags,
    {
      Name = var.stream_name
    }
  )
}
