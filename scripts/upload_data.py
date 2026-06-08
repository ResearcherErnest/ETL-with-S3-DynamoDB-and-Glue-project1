"""
Upload reference data (songs, users) and/or stream files to S3.

Usage:
    # Upload only reference data (songs + users)
    python infrastructure/upload_data.py --reference-only

    # Upload a specific stream file (triggers the pipeline via EventBridge)
    python infrastructure/upload_data.py --streams-only --file streams1.csv

    # Upload all stream files one-by-one
    python infrastructure/upload_data.py --streams-only

    # Upload everything
    python infrastructure/upload_data.py
"""

import argparse
import os
import sys
import boto3

# Resolve project root relative to this file
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")


def get_bucket_name() -> str:
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"music-streaming-pipeline-{account_id}"


def upload(bucket: str, local_path: str, s3_key: str, dry_run: bool = False):
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"  {'[DRY RUN] ' if dry_run else ''}s3://{bucket}/{s3_key}  ({size_mb:.1f} MB)")
    if not dry_run:
        boto3.client("s3").upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )


def upload_reference(bucket: str, dry_run: bool):
    print("\n=== Reference data ===")
    upload(
        bucket,
        os.path.join(DATA_DIR, "songs", "songs.csv"),
        "raw/reference/songs/songs.csv",
        dry_run,
    )
    upload(
        bucket,
        os.path.join(DATA_DIR, "users", "users.csv"),
        "raw/reference/users/users.csv",
        dry_run,
    )


def upload_streams(bucket: str, files: list, dry_run: bool):
    print("\n=== Stream files (each upload triggers the pipeline) ===")
    streams_dir = os.path.join(DATA_DIR, "streams")
    available = sorted(f for f in os.listdir(streams_dir) if f.endswith(".csv"))

    targets = files if files else available
    missing = [f for f in targets if f not in available]
    if missing:
        print(f"ERROR: file(s) not found in {streams_dir}: {missing}", file=sys.stderr)
        sys.exit(1)

    for fname in targets:
        upload(bucket, os.path.join(streams_dir, fname), f"raw/streams/{fname}", dry_run)

    if not dry_run:
        print(
            "\nUpload complete. EventBridge will trigger Step Functions for each file."
            "\nMonitor executions at:"
            "\n  https://console.aws.amazon.com/states/home#/statemachines"
        )


def main():
    parser = argparse.ArgumentParser(description="Seed S3 with pipeline data")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--reference-only", action="store_true", help="Upload songs + users only")
    group.add_argument("--streams-only", action="store_true", help="Upload stream files only")
    parser.add_argument(
        "--file",
        metavar="FILENAME",
        nargs="+",
        default=[],
        help="Specific stream file(s) to upload (e.g. streams1.csv). Defaults to all.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be uploaded")
    parser.add_argument("--bucket", default=None, help="Override bucket name")
    args = parser.parse_args()

    bucket = args.bucket or get_bucket_name()
    print(f"Target bucket: s3://{bucket}")

    if args.reference_only:
        upload_reference(bucket, args.dry_run)
    elif args.streams_only:
        upload_streams(bucket, args.file, args.dry_run)
    else:
        upload_reference(bucket, args.dry_run)
        upload_streams(bucket, args.file, args.dry_run)


if __name__ == "__main__":
    main()
