variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Base name used across all resource names"
  type        = string
  default     = "music-streaming-pipeline"
}

variable "environment" {
  description = "Deployment environment tag (dev / staging / prod)"
  type        = string
  default     = "dev"
}

variable "glue_worker_type" {
  description = "Glue ETL worker type for the PySpark transformation job"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue ETL workers for the PySpark transformation job"
  type        = number
  default     = 2
}

variable "dynamodb_kpis_table" {
  description = "DynamoDB table name for per-genre KPIs"
  type        = string
  default     = "music_kpis"
}

variable "dynamodb_top_genres_table" {
  description = "DynamoDB table name for daily top-genres snapshot"
  type        = string
  default     = "music_top_genres"
}

variable "kpi_ttl_days" {
  description = "Days before DynamoDB KPI items expire (TTL)"
  type        = number
  default     = 90
}
