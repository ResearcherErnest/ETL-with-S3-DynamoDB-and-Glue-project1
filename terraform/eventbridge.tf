# ── EventBridge rule — fires on every new object in raw/streams/ ───────────────

resource "aws_cloudwatch_event_rule" "s3_trigger" {
  name        = "${var.project_name}-s3-trigger"
  description = "Trigger pipeline when a new file lands in raw/streams/"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.pipeline.id] }
      object = { key = [{ prefix = "raw/streams/" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "sfn" {
  rule     = aws_cloudwatch_event_rule.s3_trigger.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.eventbridge.arn
}
