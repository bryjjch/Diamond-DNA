# S3 Bucket for Data Lake
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

# Versioning is required to enable cross-region/same-region replication later.
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ----------------------------------------------------------------------------
# Replication: gold/ -> MLB-Market-Simulator raw-data bucket
# ----------------------------------------------------------------------------
# Same-account, same-region replication (SRR). The replication rule and the IAM
# role that S3 assumes both live on the SOURCE bucket (this one); the
# destination bucket only needs versioning, which the MLB project already has.
# Enabled only when a destination bucket ARN is supplied.
#
# Note: replication preserves object keys (gold/statcast/x -> gold/statcast/x)
# and only copies objects written AFTER this is applied. Backfill existing gold
# data with an S3 Batch Replication job.

locals {
  replication_enabled = var.replication_destination_bucket_arn != ""
}

data "aws_iam_policy_document" "replication_assume" {
  count = local.replication_enabled ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "replication" {
  count              = local.replication_enabled ? 1 : 0
  name               = "${var.name_prefix}-s3-gold-replication"
  assume_role_policy = data.aws_iam_policy_document.replication_assume[0].json
  tags               = var.tags
}

data "aws_iam_policy_document" "replication" {
  count = local.replication_enabled ? 1 : 0

  # Read the source bucket's replication configuration.
  statement {
    effect = "Allow"
    actions = [
      "s3:GetReplicationConfiguration",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.data_lake.arn]
  }

  # Read source object versions, scoped to the replicated prefix.
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObjectVersionForReplication",
      "s3:GetObjectVersionAcl",
      "s3:GetObjectVersionTagging",
    ]
    resources = ["${aws_s3_bucket.data_lake.arn}/${var.replication_prefix}*"]
  }

  # Write the replicas into the destination bucket.
  statement {
    effect = "Allow"
    actions = [
      "s3:ReplicateObject",
      "s3:ReplicateDelete",
      "s3:ReplicateTags",
    ]
    resources = ["${var.replication_destination_bucket_arn}/*"]
  }
}

resource "aws_iam_policy" "replication" {
  count  = local.replication_enabled ? 1 : 0
  name   = "${var.name_prefix}-s3-gold-replication"
  policy = data.aws_iam_policy_document.replication[0].json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "replication" {
  count      = local.replication_enabled ? 1 : 0
  role       = aws_iam_role.replication[0].name
  policy_arn = aws_iam_policy.replication[0].arn
}

resource "aws_s3_bucket_replication_configuration" "data_lake" {
  count = local.replication_enabled ? 1 : 0

  # Versioning must be active before a replication config can be attached.
  depends_on = [aws_s3_bucket_versioning.data_lake]

  role   = aws_iam_role.replication[0].arn
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "replicate-gold"
    status = "Enabled"

    filter {
      prefix = var.replication_prefix
    }

    delete_marker_replication {
      status = "Enabled"
    }

    destination {
      bucket        = var.replication_destination_bucket_arn
      storage_class = "STANDARD"
    }
  }
}