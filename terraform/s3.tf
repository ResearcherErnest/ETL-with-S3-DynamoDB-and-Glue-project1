# ── S3 Bucket ─────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "pipeline" {
  bucket        = local.bucket_name
  force_destroy = true # allows `terraform destroy` to delete non-empty bucket
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket                  = aws_s3_bucket.pipeline.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable EventBridge notifications so S3 PUT events flow to EventBridge
resource "aws_s3_bucket_notification" "pipeline" {
  bucket      = aws_s3_bucket.pipeline.id
  eventbridge = true
}

# ── Lifecycle Rules ────────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"
    filter { prefix = "archive/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "processed-to-ia"
    status = "Enabled"
    filter { prefix = "processed/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "expire-dead-letter"
    status = "Enabled"
    filter { prefix = "dead-letter/" }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-glue-temp"
    status = "Enabled"
    filter { prefix = "glue-temp/" }
    expiration {
      days = 3
    }
  }
}

# ── Prefix placeholders (logical "folders") ────────────────────────────────────

locals {
  s3_prefixes = [
    "raw/streams/",
    "raw/reference/songs/",
    "raw/reference/users/",
    "processed/",
    "archive/",
    "dead-letter/",
    "glue-scripts/",
    "glue-temp/",
  ]
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(local.s3_prefixes)
  bucket   = aws_s3_bucket.pipeline.id
  key      = "${each.value}.keep"
  content  = ""
}

# ── Glue job scripts uploaded to S3 ───────────────────────────────────────────

resource "aws_s3_object" "validation_script" {
  bucket = aws_s3_bucket.pipeline.id
  key    = "glue-scripts/validation_job.py"
  source = "${path.module}/../glue_jobs/validation_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/validation_job.py")
}

resource "aws_s3_object" "transformation_script" {
  bucket = aws_s3_bucket.pipeline.id
  key    = "glue-scripts/transformation_job.py"
  source = "${path.module}/../glue_jobs/transformation_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/transformation_job.py")
}

resource "aws_s3_object" "ingestion_script" {
  bucket = aws_s3_bucket.pipeline.id
  key    = "glue-scripts/dynamodb_ingestion_job.py"
  source = "${path.module}/../glue_jobs/dynamodb_ingestion_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/dynamodb_ingestion_job.py")
}
