# ── CloudWatch log group for Step Functions ────────────────────────────────────

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${var.project_name}"
  retention_in_days = 14
}

# ── State Machine ──────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "pipeline" {
  name     = var.project_name
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  # Render the ASL template with runtime values
  definition = templatefile(
    "${path.module}/templates/state_machine.json",
    {
      bucket_name             = aws_s3_bucket.pipeline.id
      validation_job_name     = aws_glue_job.validation.name
      transformation_job_name = aws_glue_job.transformation.name
      ingestion_job_name      = aws_glue_job.dynamo_ingestion.name
    }
  )

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }

  depends_on = [
    aws_glue_job.validation,
    aws_glue_job.transformation,
    aws_glue_job.dynamo_ingestion,
  ]
}
