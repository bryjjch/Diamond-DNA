output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "replication_role_arn" {
  description = "ARN of the IAM role S3 assumes to replicate gold/ objects (null when replication is disabled)."
  value       = local.replication_enabled ? aws_iam_role.replication[0].arn : null
}
