aws_region   = "us-east-1"
project_name = "music-streaming-pipeline"
environment  = "dev"

# Glue PySpark job sizing (G.1X × 2 = minimum viable, ~$0.44/hr)
glue_worker_type = "G.1X"
glue_num_workers = 2

# DynamoDB table names
dynamodb_kpis_table       = "music_kpis"
dynamodb_top_genres_table = "music_top_genres"

# KPI item TTL in DynamoDB (days)
kpi_ttl_days = 90
