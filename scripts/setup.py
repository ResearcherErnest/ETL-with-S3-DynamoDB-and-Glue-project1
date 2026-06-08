"""
PC setup check — run this FIRST before anything else.

Verifies all prerequisites are installed and AWS credentials are configured.
Installs Python dependencies automatically.

Usage:
    python scripts/setup.py
"""

import shutil
import subprocess
import sys


REQUIRED_PYTHON = (3, 9)
REQUIRED_TF_MAJOR = 1
REQUIRED_TF_MINOR = 5


def header(text: str):
    print(f"\n{'-' * 50}")
    print(f"  {text}")
    print('-' * 50)


def ok(msg: str):
    print(f"  [OK]   {msg}")


def fail(msg: str):
    print(f"  [FAIL] {msg}")


def warn(msg: str):
    print(f"  [WARN] {msg}")


def check_python():
    header("Python")
    v = sys.version_info
    if (v.major, v.minor) >= REQUIRED_PYTHON:
        ok(f"Python {v.major}.{v.minor}.{v.micro}")
    else:
        fail(f"Python {v.major}.{v.minor} found — need >= {REQUIRED_PYTHON[0]}.{REQUIRED_PYTHON[1]}")
        sys.exit(1)


def install_requirements():
    header("Python packages (requirements.txt)")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        ok("boto3, pandas, pyarrow installed")
    else:
        fail("pip install failed")
        print(result.stderr)
        sys.exit(1)


def check_terraform():
    header("Terraform")
    tf = shutil.which("terraform")
    if not tf:
        fail("terraform not found in PATH")
        print("       Install: https://developer.hashicorp.com/terraform/downloads")
        print("       Windows: choco install terraform")
        sys.exit(1)

    result = subprocess.run(["terraform", "version", "-json"], capture_output=True, text=True)
    if result.returncode != 0:
        fail("Could not determine Terraform version")
        sys.exit(1)

    import json
    data = json.loads(result.stdout)
    version_str = data.get("terraform_version", "0.0.0")
    major, minor, *_ = [int(x) for x in version_str.split(".")]
    if (major, minor) >= (REQUIRED_TF_MAJOR, REQUIRED_TF_MINOR):
        ok(f"Terraform {version_str}")
    else:
        fail(f"Terraform {version_str} — need >= {REQUIRED_TF_MAJOR}.{REQUIRED_TF_MINOR}")
        sys.exit(1)


def check_aws_cli():
    header("AWS CLI")
    aws = shutil.which("aws")
    if not aws:
        fail("AWS CLI not found in PATH")
        print("       Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
        sys.exit(1)

    result = subprocess.run(["aws", "--version"], capture_output=True, text=True)
    ok(result.stdout.strip() or result.stderr.strip())


def check_aws_credentials():
    header("AWS credentials")
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError

        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        ok(f"Account : {identity['Account']}")
        ok(f"User ARN: {identity['Arn']}")
    except Exception as exc:
        fail(f"AWS credentials not configured: {exc}")
        print("       Run: aws configure")
        print("       Or set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION")
        sys.exit(1)


def check_aws_region():
    header("AWS region")
    import boto3
    session = boto3.session.Session()
    region = session.region_name
    if region:
        ok(f"Region: {region}")
    else:
        warn("No default region set — set AWS_DEFAULT_REGION or run `aws configure`")


def summary():
    print(f"\n{'=' * 50}")
    print("  All checks passed. You are ready to deploy.")
    print("  Next steps:")
    print("    cd terraform && terraform init")
    print("    terraform plan")
    print("    terraform apply")
    print('=' * 50)


def main():
    print("Music Streaming Pipeline — PC setup check")
    check_python()
    install_requirements()
    check_terraform()
    check_aws_cli()
    check_aws_credentials()
    check_aws_region()
    summary()


if __name__ == "__main__":
    main()
