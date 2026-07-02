"""
Glue ETL (PySpark) job — join streams + songs + users and compute all 6 KPI sets.

Arguments (Glue job args):
    --bucket      S3 bucket name
    --input_key   S3 key of the validated triggering stream file (the only file read)

Outputs written to S3:
    processed/<listen_date>/genre_kpis/     — per-genre per-day KPI Parquet
    processed/<listen_date>/top_genres/     — top-5 genres per-day Parquet
"""

import sys
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, TimestampType, StringType
from pyspark.sql.window import Window


# ── Init ──────────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "input_key"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket"]
INPUT_KEY = args["input_key"]

logger = glueContext.get_logger()


def log(level: str, msg: str, **kw):
    import json as _json
    record = {"level": level, "job": args["JOB_NAME"], "message": msg, **kw}
    logger.info(_json.dumps(record))


# ── Read ───────────────────────────────────────────────────────────────────────

def read_streams() -> "DataFrame":
    # Read only the validated triggering file — other objects under raw/streams/
    # may not have passed validation (each upload gets its own execution)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"s3://{BUCKET}/{INPUT_KEY}")
    )
    df = (
        df
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("listen_time", F.to_timestamp(F.col("listen_time"), "yyyy-MM-dd HH:mm:ss"))
        .dropDuplicates(["user_id", "track_id", "listen_time"])
    )
    log("INFO", "Streams loaded", count=df.count())
    return df


def read_songs() -> "DataFrame":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"s3://{BUCKET}/raw/reference/songs/songs.csv")
        .withColumn("duration_ms", F.col("duration_ms").cast(LongType()))
        .select("track_id", "track_name", "track_genre", "duration_ms", "artists", "album_name")
    )
    log("INFO", "Songs reference loaded", count=df.count())
    return df


def read_users() -> "DataFrame":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"s3://{BUCKET}/raw/reference/users/users.csv")
        .select("user_id", "user_country")
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
    )
    log("INFO", "Users reference loaded", count=df.count())
    return df


# ── Enrich ─────────────────────────────────────────────────────────────────────

def enrich(streams_df, songs_df, users_df) -> "DataFrame":
    df = (
        streams_df
        .join(songs_df, on="track_id", how="left")
        .join(users_df, on="user_id", how="left")
        # Impute unknown genre — don't drop orphan stream records
        .withColumn("track_genre", F.coalesce(F.col("track_genre"), F.lit("unknown")))
        .withColumn("duration_ms", F.coalesce(F.col("duration_ms"), F.lit(0).cast(LongType())))
        .withColumn("listen_date", F.date_format(F.col("listen_time"), "yyyy-MM-dd"))
    )
    log("INFO", "Enrichment complete", count=df.count())
    return df


# ── KPI computations ───────────────────────────────────────────────────────────

def compute_listen_count(df) -> "DataFrame":
    return (
        df.groupBy("track_genre", "listen_date")
        .agg(F.count("*").alias("listen_count"))
    )


def compute_unique_listeners(df) -> "DataFrame":
    return (
        df.groupBy("track_genre", "listen_date")
        .agg(F.countDistinct("user_id").alias("unique_listeners"))
    )


def compute_total_listen_time(df) -> "DataFrame":
    return (
        df.groupBy("track_genre", "listen_date")
        .agg(F.sum("duration_ms").alias("total_listen_time_ms"))
    )


def compute_avg_listen_time_per_user(df) -> "DataFrame":
    # Step 1: total per user per genre per day
    per_user = (
        df.groupBy("user_id", "track_genre", "listen_date")
        .agg(F.sum("duration_ms").alias("user_total_ms"))
    )
    # Step 2: average across users
    return (
        per_user.groupBy("track_genre", "listen_date")
        .agg(F.avg("user_total_ms").alias("avg_listen_time_ms"))
    )


def compute_top_3_songs(df) -> "DataFrame":
    song_plays = (
        df.groupBy("track_genre", "listen_date", "track_id", "track_name")
        .agg(F.count("*").alias("song_listen_count"))
    )
    # row_number (not dense_rank) caps the list at exactly 3 — ties broken by
    # track_id so the result is deterministic and the item stays small
    w = Window.partitionBy("track_genre", "listen_date").orderBy(
        F.desc("song_listen_count"), F.col("track_id")
    )
    ranked = (
        song_plays
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 3)
    )
    # Collect into a JSON string per genre/date — simpler DynamoDB ingestion
    top3 = (
        ranked
        .withColumn(
            "song_struct",
            F.struct(
                F.col("rank"),
                F.col("track_id"),
                F.col("track_name"),
                F.col("song_listen_count"),
            ),
        )
        .groupBy("track_genre", "listen_date")
        .agg(F.to_json(F.collect_list("song_struct")).alias("top_3_songs"))
    )
    return top3


def compute_top_5_genres(df) -> "DataFrame":
    daily_genre_plays = (
        df.groupBy("listen_date", "track_genre")
        .agg(F.count("*").alias("genre_listen_count"))
    )
    # row_number caps at exactly 5; genre name breaks ties deterministically
    w = Window.partitionBy("listen_date").orderBy(
        F.desc("genre_listen_count"), F.col("track_genre")
    )
    return (
        daily_genre_plays
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 5)
        .orderBy("listen_date", "rank")
    )


# ── Merge KPIs ─────────────────────────────────────────────────────────────────

def merge_kpis(listen_count, unique_listeners, total_time, avg_time, top3_songs) -> "DataFrame":
    keys = ["track_genre", "listen_date"]
    return (
        listen_count
        .join(unique_listeners, keys, "left")
        .join(total_time, keys, "left")
        .join(avg_time, keys, "left")
        .join(top3_songs, keys, "left")
        .withColumn("avg_listen_time_ms", F.round(F.col("avg_listen_time_ms"), 2))
        .fillna({"top_3_songs": "[]"})
    )


# ── Write ──────────────────────────────────────────────────────────────────────

def write_outputs(kpis_df, top_genres_df):
    dates = [r.listen_date for r in kpis_df.select("listen_date").distinct().collect()]
    partition = dates[0] if dates else "unknown"

    kpis_path = f"s3://{BUCKET}/processed/{partition}/genre_kpis/"
    top_path  = f"s3://{BUCKET}/processed/{partition}/top_genres/"

    # Parquet: analytical archive format
    kpis_df.write.mode("overwrite").parquet(kpis_path + "parquet/")
    top_genres_df.write.mode("overwrite").parquet(top_path + "parquet/")

    # JSON: interface format read by the Python Shell ingestion job (no pyarrow needed)
    kpis_df.coalesce(1).write.mode("overwrite").json(kpis_path + "json/")
    top_genres_df.coalesce(1).write.mode("overwrite").json(top_path + "json/")

    log("INFO", "KPIs written", partition=partition, kpis_path=kpis_path, top_path=top_path)
    return partition


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    log("INFO", "Transformation job started", input_key=INPUT_KEY)

    streams_df = read_streams()
    songs_df = read_songs()
    users_df = read_users()

    enriched = enrich(streams_df, songs_df, users_df)

    listen_count   = compute_listen_count(enriched)
    unique_list    = compute_unique_listeners(enriched)
    total_time     = compute_total_listen_time(enriched)
    avg_time       = compute_avg_listen_time_per_user(enriched)
    top3_songs     = compute_top_3_songs(enriched)
    top5_genres    = compute_top_5_genres(enriched)

    kpis_df = merge_kpis(listen_count, unique_list, total_time, avg_time, top3_songs)

    partition = write_outputs(kpis_df, top5_genres)

    log(
        "INFO",
        "Transformation job complete",
        genres=kpis_df.select("track_genre").distinct().count(),
        partition=partition,
    )


main()
job.commit()
