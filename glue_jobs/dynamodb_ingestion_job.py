"""
Glue Python Shell job — read JSON KPI outputs and batch-write to DynamoDB.

No pyarrow or pandas needed: reads newline-delimited JSON written by the
PySpark transformation job using only boto3 and the standard library.

Arguments:
    --bucket                  S3 bucket name
    --input_key               S3 key of the triggering stream file (derives partition date)
    --dynamodb_kpis_table     DynamoDB table for genre KPIs  (default: music_kpis)
    --dynamodb_top_genres_table  DynamoDB table for top-genres (default: music_top_genres)
    --kpi_ttl_days            Days until TTL expiry          (default: 90)
"""

import io
import json
import sys
import time
import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

import boto3


# ── Argument parsing ───────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input_key", required=True)
    parser.add_argument("--dynamodb_kpis_table", default="music_kpis")
    parser.add_argument("--dynamodb_top_genres_table", default="music_top_genres")
    parser.add_argument("--kpi_ttl_days", type=int, default=90)
    known, _ = parser.parse_known_args()
    return known


# ── Logging ───────────────────────────────────────────────────────────────────

def log(level, message, **kw):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "job": "dynamodb_ingestion_job",
        "message": message,
    }
    record.update(kw)
    print(json.dumps(record), flush=True)


# ── S3 helpers ────────────────────────────────────────────────────────────────

def list_prefix(bucket, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_ndjson(bucket, key):
    """Read a newline-delimited JSON file from S3, return list of dicts."""
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    records = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def read_json_prefix(bucket, prefix):
    """Read all .json part files under a prefix, return merged list of dicts."""
    records = []
    for key in list_prefix(bucket, prefix):
        if key.endswith(".json") and not key.endswith("_SUCCESS"):
            records.extend(read_ndjson(bucket, key))
    return records


def write_report(bucket, key, report):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


# ── DynamoDB batch write ───────────────────────────────────────────────────────

BATCH_SIZE = 25
MAX_RETRIES = 3


def _dec(value):
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0")


def _ttl(days):
    return int((datetime.now(timezone.utc) + timedelta(days=days)).timestamp())


def batch_write(table_name, items):
    dynamo = boto3.client("dynamodb")
    total = 0
    for i in range(0, len(items), BATCH_SIZE):
        chunk = items[i: i + BATCH_SIZE]
        unprocessed = {table_name: [{"PutRequest": {"Item": item}} for item in chunk]}
        for attempt in range(MAX_RETRIES):
            resp = dynamo.batch_write_item(RequestItems=unprocessed)
            unprocessed = resp.get("UnprocessedItems", {})
            if not unprocessed:
                total += len(chunk)
                break
            wait = (2 ** attempt) * 0.5
            log("WARNING", "UnprocessedItems, retrying", attempt=attempt + 1, wait_s=wait)
            time.sleep(wait)
        if unprocessed:
            raise RuntimeError(
                "Failed to write {} items to {} after {} retries".format(
                    len(unprocessed.get(table_name, [])), table_name, MAX_RETRIES
                )
            )
    log("INFO", "Batch write complete", table=table_name, items=total)
    return total


# ── Item builders ──────────────────────────────────────────────────────────────

def build_kpi_item(row, ttl_days):
    top_3 = row.get("top_3_songs", "[]")
    if not isinstance(top_3, str):
        top_3 = json.dumps(top_3)
    return {
        "genre":                {"S": str(row["track_genre"])},
        "date":                 {"S": str(row["listen_date"])},
        "listen_count":         {"N": str(int(row.get("listen_count") or 0))},
        "unique_listeners":     {"N": str(int(row.get("unique_listeners") or 0))},
        "total_listen_time_ms": {"N": str(int(row.get("total_listen_time_ms") or 0))},
        "avg_listen_time_ms":   {"N": str(_dec(row.get("avg_listen_time_ms", 0)))},
        "top_3_songs":          {"S": top_3},
        "processed_at":         {"S": datetime.now(timezone.utc).isoformat()},
        "ttl_expiry":           {"N": str(_ttl(ttl_days))},
    }


def build_top_genres_item(listen_date, rows, ttl_days):
    top_5 = sorted(
        [{"rank": int(r["rank"]), "genre": str(r["track_genre"]), "listen_count": int(r["genre_listen_count"])}
         for r in rows],
        key=lambda x: x["rank"]
    )
    return {
        "record_type":  {"S": "TOP_GENRES"},
        "date":         {"S": listen_date},
        "top_5_genres": {"S": json.dumps(top_5)},
        "processed_at": {"S": datetime.now(timezone.utc).isoformat()},
        "ttl_expiry":   {"N": str(_ttl(ttl_days))},
    }


# ── Ingestion ──────────────────────────────────────────────────────────────────

def ingest_genre_kpis(bucket, partition, table, ttl_days):
    prefix = "processed/{}/genre_kpis/json/".format(partition)
    log("INFO", "Reading genre KPIs", prefix=prefix)
    rows = read_json_prefix(bucket, prefix)
    if not rows:
        log("WARNING", "No genre KPI records found", prefix=prefix)
        return 0
    items = [build_kpi_item(r, ttl_days) for r in rows]
    log("INFO", "Writing genre KPIs", count=len(items), table=table)
    return batch_write(table, items)


def ingest_top_genres(bucket, partition, table, ttl_days):
    prefix = "processed/{}/top_genres/json/".format(partition)
    log("INFO", "Reading top genres", prefix=prefix)
    rows = read_json_prefix(bucket, prefix)
    if not rows:
        log("WARNING", "No top-genres records found", prefix=prefix)
        return 0

    # Group by listen_date
    by_date = {}
    for r in rows:
        d = str(r["listen_date"])
        by_date.setdefault(d, []).append(r)

    items = [build_top_genres_item(d, by_date[d], ttl_days) for d in sorted(by_date)]
    log("INFO", "Writing top genres", count=len(items), table=table)
    return batch_write(table, items)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    bucket          = args.bucket
    input_key       = args.input_key
    kpis_table      = args.dynamodb_kpis_table
    top_genres_table = args.dynamodb_top_genres_table
    ttl_days        = args.kpi_ttl_days

    log("INFO", "Ingestion job started", input_key=input_key)

    # Discover date partitions written by the transformation job
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    partitions = set()
    for page in paginator.paginate(Bucket=bucket, Prefix="processed/", Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            part = p["Prefix"].rstrip("/").split("/")[-1]
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                partitions.add(part)

    if not partitions:
        log("ERROR", "No date partitions found under processed/")
        sys.exit(1)

    total_kpi = 0
    total_top = 0
    for partition in sorted(partitions):
        log("INFO", "Processing partition", partition=partition)
        total_kpi += ingest_genre_kpis(bucket, partition, kpis_table, ttl_days)
        total_top += ingest_top_genres(bucket, partition, top_genres_table, ttl_days)

    summary = {
        "status": "SUCCEEDED",
        "kpi_items_written": total_kpi,
        "top_genre_items_written": total_top,
        "partitions": sorted(partitions),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    fname = input_key.split("/")[-1].replace(".csv", "")
    write_report(bucket, "processed/reports/ingestion_{}.json".format(fname), summary)
    log("INFO", "Ingestion complete", kpi_items=total_kpi, top_genre_items=total_top)


if __name__ == "__main__":
    main()
