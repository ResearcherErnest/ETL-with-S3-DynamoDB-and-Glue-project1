output "bucket_name" {
  description = "S3 bucket name for the pipeline"
  value       = aws_s3_bucket.pipeline.id
}

output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.pipeline.arn
}

output "dynamodb_kpis_table_name" {
  description = "DynamoDB KPIs table name"
  value       = aws_dynamodb_table.music_kpis.name
}

output "dynamodb_top_genres_table_name" {
  description = "DynamoDB top genres table name"
  value       = aws_dynamodb_table.music_top_genres.name
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue jobs"
  value       = aws_iam_role.glue.arn
}

output "step_functions_role_arn" {
  description = "IAM role ARN used by Step Functions"
  value       = aws_iam_role.step_functions.arn
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "validation_job_name" {
  description = "Glue validation job name"
  value       = aws_glue_job.validation.name
}

output "transformation_job_name" {
  description = "Glue transformation job name"
  value       = aws_glue_job.transformation.name
}

output "ingestion_job_name" {
  description = "Glue DynamoDB ingestion job name"
  value       = aws_glue_job.dynamo_ingestion.name
}

output "eventbridge_rule_name" {
  description = "EventBridge rule that triggers the pipeline on new stream files"
  value       = aws_cloudwatch_event_rule.s3_trigger.name
}
