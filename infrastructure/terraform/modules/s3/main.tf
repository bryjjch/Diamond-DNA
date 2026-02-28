# S3 Bucket for Data Lake (Raw Data)
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name

  tags = var.tags
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}