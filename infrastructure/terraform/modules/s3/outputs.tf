output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "model_artifacts_bucket_name" {
  description = "Name of the model artifacts S3 bucket"
  value       = aws_s3_bucket.model_artifacts.id
}

output "model_artifacts_bucket_arn" {
  description = "ARN of the model artifacts S3 bucket"
  value       = aws_s3_bucket.model_artifacts.arn
}
