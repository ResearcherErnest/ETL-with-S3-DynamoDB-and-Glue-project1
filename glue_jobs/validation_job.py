"""
Glue Python Shell job — validate an incoming stream CSV before transformation.

Arguments (passed by Step Functions / Glue):
    --bucket     S3 bucket name
    --input_key  S3 key of the incoming stream file (e.g. raw/streams/streams1.csv)

Exit codes:
    0  PASSED — validation report written to processed/reports/
    1  FAILED — validation report written to dead-letter/reports/; Step Functions
                interprets non-zero exit as job failure and routes to dead-letter path
"""

import io
import json
import re
import sys
import boto3
import pandas as pd
from datetime import datetime, timezone


# ── Argument parsing ───────────────────────────────────────────────────────────

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input_key", required=True)
    known, _ = parser.parse_known_args()
    return known


# ── Logging ───────────────────────────────────────────────────────────────────

def log(level: str, message: str, **kw):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "job": "validation_job",
        "message": message,
        **kw,
    }
    print(json.dumps(record), flush=True)


# ── S3 helpers ────────────────────────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def write_report(bucket: str, key: str, report: dict):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


# ── Validators ────────────────────────────────────────────────────────────────

REQUIRED_COLUMNS = {"user_id", "track_id", "listen_time"}
SPOTIFY_ID_RE = re.compile(r"^[A-Za-z0-9]{10,30}$")


def check_schema(df: pd.DataFrame, errors: list):
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"Missing required columns: {sorted(missing)}")
        log("ERROR", "Schema check failed", missing_columns=sorted(missing))
    else:
        log("INFO", "Schema check passed")


def check_nulls(df: pd.DataFrame, errors: list) -> dict:
    null_counts = df[list(REQUIRED_COLUMNS)].isnull().sum().to_dict()
    for col, count in null_counts.items():
        if count > 0:
            errors.append(f"Column '{col}' has {count} null value(s)")
            log("ERROR", "Null check failed", column=col, null_count=int(count))
    if not any(null_counts.values()):
        log("INFO", "Null check passed")
    return {k: int(v) for k, v in null_counts.items()}


def check_data_types(df: pd.DataFrame, errors: list):
    # user_id must be castable to integer
    invalid_user_ids = pd.to_numeric(df["user_id"], errors="coerce").isna().sum()
    if invalid_user_ids > 0:
        errors.append(f"user_id has {invalid_user_ids} non-numeric value(s)")
        log("ERROR", "Type check failed", column="user_id", invalid_count=int(invalid_user_ids))

    # track_id must match Spotify ID pattern
    invalid_track_ids = (~df["track_id"].astype(str).str.match(SPOTIFY_ID_RE)).sum()
    if invalid_track_ids > 0:
        errors.append(f"track_id has {invalid_track_ids} invalid value(s)")
        log("ERROR", "Type check failed", column="track_id", invalid_count=int(invalid_track_ids))

    # listen_time must be parseable as datetime
    parsed = pd.to_datetime(df["listen_time"], errors="coerce")
    nat_count = parsed.isna().sum()
    nat_pct = nat_count / len(df) if len(df) > 0 else 0
    if nat_pct > 0.001:  # >0.1% threshold
        errors.append(f"listen_time has {nat_count} unparseable value(s) ({nat_pct:.2%})")
        log("ERROR", "Type check failed", column="listen_time", nat_count=int(nat_count))

    if invalid_user_ids == 0 and invalid_track_ids == 0 and nat_pct <= 0.001:
        log("INFO", "Data type check passed")


def check_referential_integrity(
    df: pd.DataFrame,
    bucket: str,
    errors: list,
) -> dict:
    try:
        songs_df = read_csv_from_s3(bucket, "raw/reference/songs/songs.csv")
        users_df = read_csv_from_s3(bucket, "raw/reference/users/users.csv")
    except Exception as exc:
        log("WARNING", "Could not load reference data for integrity check", error=str(exc))
        return {"unknown_user_pct": None, "unknown_track_pct": None}

    stream_users = set(df["user_id"].dropna().astype(str))
    known_users = set(users_df["user_id"].astype(str))
    unknown_user_count = len(stream_users - known_users)
    unknown_user_pct = unknown_user_count / len(stream_users) if stream_users else 0

    stream_tracks = set(df["track_id"].dropna().astype(str))
    known_tracks = set(songs_df["track_id"].astype(str))
    unknown_track_count = len(stream_tracks - known_tracks)
    unknown_track_pct = unknown_track_count / len(stream_tracks) if stream_tracks else 0

    if unknown_user_pct > 0.05:
        errors.append(
            f"{unknown_user_pct:.1%} of user_ids not found in users reference ({unknown_user_count} distinct users)"
        )
    if unknown_track_pct > 0.05:
        errors.append(
            f"{unknown_track_pct:.1%} of track_ids not found in songs reference ({unknown_track_count} distinct tracks)"
        )

    log(
        "INFO",
        "Referential integrity check complete",
        unknown_user_pct=round(unknown_user_pct, 4),
        unknown_track_pct=round(unknown_track_pct, 4),
    )
    return {
        "unknown_user_pct": round(unknown_user_pct, 4),
        "unknown_track_pct": round(unknown_track_pct, 4),
    }


def check_duplicates(df: pd.DataFrame) -> int:
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        log("WARNING", "Duplicate rows detected (will be deduped in transformation)", count=dup_count)
    else:
        log("INFO", "No duplicate rows")
    return dup_count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    bucket = args.bucket
    input_key = args.input_key
    filename = input_key.split("/")[-1]

    log("INFO", "Validation started", input_key=input_key)

    df = read_csv_from_s3(bucket, input_key)
    log("INFO", "File loaded", record_count=len(df), columns=list(df.columns))

    errors = []
    check_schema(df, errors)

    # Skip further checks if schema is broken — columns won't exist
    if errors:
        _fail(bucket, input_key, filename, df, errors, {}, 0)

    null_counts = check_nulls(df, errors)
    check_data_types(df, errors)
    integrity = check_referential_integrity(df, bucket, errors)
    dup_count = check_duplicates(df)

    report = {
        "status": "FAILED" if errors else "PASSED",
        "input_key": input_key,
        "record_count": len(df),
        "null_counts": null_counts,
        "unknown_user_pct": integrity.get("unknown_user_pct"),
        "unknown_track_pct": integrity.get("unknown_track_pct"),
        "duplicate_rows": dup_count,
        "errors": errors,
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }

    if errors:
        _fail(bucket, input_key, filename, df, errors, report, dup_count)

    # Write success report
    report_key = f"processed/reports/validation_{filename.replace('.csv','')}.json"
    write_report(bucket, report_key, report)
    log("INFO", "Validation PASSED", report_key=report_key, record_count=len(df))


def _fail(bucket, input_key, filename, df, errors, report, dup_count):
    report["status"] = "FAILED"
    report.setdefault("record_count", len(df) if df is not None else 0)
    report.setdefault("errors", errors)
    report.setdefault("validated_at", datetime.now(timezone.utc).isoformat())

    report_key = f"dead-letter/reports/validation_{filename.replace('.csv','')}.json"
    write_report(bucket, report_key, report)
    log("ERROR", "Validation FAILED", errors=errors, report_key=report_key)
    sys.exit(1)


if __name__ == "__main__":
    main()
