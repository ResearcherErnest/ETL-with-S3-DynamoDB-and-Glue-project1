# ── music_kpis — per-genre, per-day KPIs ──────────────────────────────────────

resource "aws_dynamodb_table" "music_kpis" {
  name         = var.dynamodb_kpis_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "genre"
  range_key    = "date"

  attribute {
    name = "genre"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  attribute {
    name = "listen_count"
    type = "N"
  }

  # GSI: query all genres on a given date sorted by listen_count desc (Top 5 genres)
  global_secondary_index {
    name            = "date-index"
    hash_key        = "date"
    range_key       = "listen_count"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "ttl_expiry"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

# ── music_top_genres — daily top-5 snapshot (O(1) lookup) ─────────────────────

resource "aws_dynamodb_table" "music_top_genres" {
  name         = var.dynamodb_top_genres_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "record_type"
  range_key    = "date"

  attribute {
    name = "record_type"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  ttl {
    attribute_name = "ttl_expiry"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}
