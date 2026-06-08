# ── Glue Catalog Database ──────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "music" {
  name        = "music_streaming_db"
  description = "Glue catalog database for the music streaming pipeline"
}

# ── CloudWatch Log Group for continuous Glue logging ──────────────────────────

resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${var.project_name}"
  retention_in_days = 14
}

# ── Validation Job (Python Shell) ─────────────────────────────────────────────

resource "aws_glue_job" "validation" {
  name        = "${var.project_name}-validation"
  role_arn    = aws_iam_role.glue.arn
  max_retries = 0
  timeout     = 60

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/validation_job.py"
  }

  default_arguments = {
    "--bucket"                    = aws_s3_bucket.pipeline.id
    "--additional-python-modules" = "pandas==1.3.5"
    "--job-language"              = "python"
    "--TempDir"                   = "s3://${aws_s3_bucket.pipeline.id}/glue-temp/"
  }

  max_capacity = 0.0625

  depends_on = [aws_s3_object.validation_script]
}

# ── Transformation Job (PySpark / Glue ETL) ───────────────────────────────────

resource "aws_glue_job" "transformation" {
  name         = "${var.project_name}-transformation"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  max_retries  = 1
  timeout      = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/transformation_job.py"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  default_arguments = {
    "--bucket"                             = aws_s3_bucket.pipeline.id
    "--TempDir"                            = "s3://${aws_s3_bucket.pipeline.id}/glue-temp/"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"   = "true"
    "--continuous-log-logGroup"            = aws_cloudwatch_log_group.glue_jobs.name
    "--enable-spark-ui"                    = "true"
    "--spark-event-logs-path"              = "s3://${aws_s3_bucket.pipeline.id}/glue-temp/spark-logs/"
    "--job-bookmark-option"                = "job-bookmark-enable"
    "--dynamodb_kpis_table"                = var.dynamodb_kpis_table
    "--dynamodb_top_genres_table"          = var.dynamodb_top_genres_table
  }

  depends_on = [aws_s3_object.transformation_script]
}

# ── DynamoDB Ingestion Job (Python Shell) ─────────────────────────────────────

resource "aws_glue_job" "dynamo_ingestion" {
  name        = "${var.project_name}-dynamo-ingestion"
  role_arn    = aws_iam_role.glue.arn
  max_retries = 2
  timeout     = 60

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/dynamodb_ingestion_job.py"
  }

  default_arguments = {
    "--bucket"                    = aws_s3_bucket.pipeline.id
    "--dynamodb_kpis_table"       = var.dynamodb_kpis_table
    "--dynamodb_top_genres_table" = var.dynamodb_top_genres_table
    "--kpi_ttl_days"              = tostring(var.kpi_ttl_days)
    "--TempDir"                   = "s3://${aws_s3_bucket.pipeline.id}/glue-temp/"
  }

  max_capacity = 0.0625

  depends_on = [aws_s3_object.ingestion_script]
}
