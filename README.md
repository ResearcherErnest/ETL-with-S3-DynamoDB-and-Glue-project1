# Music Streaming Data Pipeline

>AWS data pipeline that processes user streaming events, computes daily genre-level KPIs, and stores results in DynamoDB. Orchestrated by Step Functions, transformed by AWS Glue, and fully provisioned with Terraform.

### High-Level Architecture

```mermaid
flowchart TD
    CSV[/"CSV Stream Files\nstreams1-3.csv · ~11,347 events each"/]

    subgraph S3["Amazon S3 — music-streaming-pipeline-{account_id}"]
        direction TB
        RAW["raw/streams/"]
        REF["raw/reference/\nsongs.csv · users.csv"]
    end

    EB["Amazon EventBridge\nObject Created Rule\nprefix: raw/streams/"]
    SF["AWS Step Functions\nmusic-streaming-pipeline\nSTANDARD · X-Ray enabled"]

    subgraph Glue["AWS Glue ETL"]
        GV["1  Validation Job\nPython Shell · 0.0625 DPU"]
        GT["2  Transformation Job\nPySpark · G.1X × 2 workers"]
        GI["3  Ingestion Job\nPython Shell · 0.0625 DPU"]
    end

    subgraph DDB["Amazon DynamoDB"]
        KPI[("music_kpis\nPK: genre · SK: date")]
        TOP[("music_top_genres\nPK: record_type · SK: date")]
    end

    ARCH["S3 archive/\nGlacier after 90 days"]
    DL["S3 dead-letter/\nexpires after 7 days"]

    CSV -->|upload| RAW
    RAW -->|"S3 EventBridge notification"| EB
    EB -->|StartExecution| SF
    SF --> GV
    GV -->|PASS| GT
    GT -->|PASS| GI
    GI -->|BatchWriteItem| KPI
    GI -->|BatchWriteItem| TOP
    GI -->|"copy & delete"| ARCH
    GV -->|FAIL| DL
    GT -->|FAIL| DL
    GI -->|FAIL| DL
    REF -.->|"reference lookup"| GV
    REF -.->|"join"| GT
```

---

### Pipeline Orchestration

Step Functions state machine with per-stage retries and dead-letter routing.

```mermaid
stateDiagram-v2
    state "Validate Streams" as ValidateStreams
    state "Transform KPIs" as TransformKPIs
    state "Ingest to DynamoDB" as IngestToDynamo
    state "Archive Source File" as ArchiveSourceFile
    state "Delete Source File" as DeleteSourceFile
    state "Move to Validation DL" as ValidationDL
    state "Move to Transform DL" as TransformDL
    state "Move to Ingest DL" as IngestDL
    state "Pipeline Succeeded" as PipelineSucceeded
    state "Pipeline Failed" as PipelineFailed

    [*] --> ValidateStreams

    ValidateStreams --> TransformKPIs : PASS
    ValidateStreams --> ValidationDL : FAIL (retry 2× · 30s · backoff 2.0)

    TransformKPIs --> IngestToDynamo : PASS
    TransformKPIs --> TransformDL : FAIL (retry 2× · 60s · backoff 2.0)

    IngestToDynamo --> ArchiveSourceFile : PASS
    IngestToDynamo --> IngestDL : FAIL (retry 3× · 30s · backoff 2.0)

    ArchiveSourceFile --> DeleteSourceFile : SUCCESS or CATCH (non-blocking)
    DeleteSourceFile --> PipelineSucceeded

    ValidationDL --> PipelineFailed
    TransformDL --> PipelineFailed
    IngestDL --> PipelineFailed

    PipelineSucceeded --> [*]
    PipelineFailed --> [*]
```

### ETL Data Flow

How data moves through the three Glue jobs from raw CSV to DynamoDB items.

```mermaid
flowchart LR
    subgraph src["Source (S3 raw/)"]
        ST["streams*.csv\nuser_id · track_id · listen_time"]
        SG["songs.csv\ntrack_id · genre · duration_ms"]
        US["users.csv\nuser_id · user_country"]
    end

    subgraph val["Validation Job (Python Shell)"]
        direction TB
        V1["Schema check\nuser_id · track_id · listen_time"]
        V2["Null check"]
        V3["Type check\nnumeric · regex · datetime"]
        V4["Referential integrity\n< 5% unknown threshold"]
        V5["Duplicate detection"]
        V1 --> V2 --> V3 --> V4 --> V5
    end

    subgraph tfm["Transformation Job (PySpark G.1X × 2)"]
        direction TB
        J["Join\nstreams ← songs ← users"]
        K["Compute 6 KPIs\nper genre per day"]
        W["Write output\nParquet + JSON\nprocessed/YYYY-MM-DD/"]
        J --> K --> W
    end

    subgraph ing["Ingestion Job (Python Shell)"]
        direction TB
        R["Read JSON\nfrom processed/"]
        B["Batch write\nBATCH_SIZE = 25"]
        RT["Retry unprocessed\nMAX_RETRIES = 3"]
        R --> B --> RT
    end

    subgraph ddb["DynamoDB"]
        KPI[("music_kpis\ngenre + date KPIs")]
        TG[("music_top_genres\ndaily top-5 snapshot")]
    end

    ST --> val
    SG --> val
    US --> val
    SG --> tfm
    US --> tfm
    val -->|"PASS"| tfm
    tfm --> ing
    RT --> KPI
    RT --> TG
```

### Infrastructure Map (Terraform)

```mermaid
graph TD
    subgraph IaC["terraform/"]
        direction TB
        S3TF["s3.tf"]
        IAMTF["iam.tf"]
        DDBTF["dynamodb.tf"]
        GLUETF["glue.tf"]
        SFTF["step_functions.tf"]
        EBTF["eventbridge.tf"]
        TMPL["templates/state_machine.json"]
    end

    subgraph Storage["Storage"]
        BUCKET["S3 Bucket\nversioning · AES256 · EventBridge"]
        MKPI[("music_kpis\nPAY_PER_REQUEST · GSI · PITR · TTL")]
        MTOP[("music_top_genres\nPAY_PER_REQUEST · PITR · TTL")]
    end

    subgraph Compute["Compute"]
        GVAL["validation_job\nPython Shell · retry 0 · 60s timeout"]
        GTFM["transformation_job\nPySpark Glue 4.0 · retry 1"]
        GING["dynamodb_ingestion_job\nPython Shell · retry 2 · 60s timeout"]
        CDB["Glue Catalog DB\nmusic_streaming_db"]
    end

    subgraph Orchestration["Orchestration & Events"]
        SM["State Machine\nmusic-streaming-pipeline · STANDARD"]
        RULE["EventBridge Rule\nS3 Object Created → StartExecution"]
    end

    subgraph IAMRoles["IAM Roles"]
        GLUER["GlueServiceRole\nS3 + DynamoDB + CWL"]
        SFR["StepFunctionsRole\nGlue + S3 + CWL + X-Ray"]
        EBR["EventBridgeRole\nstates:StartExecution"]
    end

    S3TF --> BUCKET
    DDBTF --> MKPI
    DDBTF --> MTOP
    GLUETF --> GVAL
    GLUETF --> GTFM
    GLUETF --> GING
    GLUETF --> CDB
    SFTF --> SM
    TMPL --> SM
    EBTF --> RULE
    IAMTF --> GLUER
    IAMTF --> SFR
    IAMTF --> EBR

    GLUER -.->|"assumed by"| GVAL
    GLUER -.->|"assumed by"| GTFM
    GLUER -.->|"assumed by"| GING
    SFR -.->|"assumed by"| SM
    EBR -.->|"assumed by"| RULE
    BUCKET -.->|"trigger"| RULE
    RULE -.->|"starts"| SM
```

### S3 Lifecycle Rules

```mermaid
flowchart LR
    subgraph Buckets["S3 Prefixes"]
        ARCH["archive/"]
        PROC["processed/"]
        DL["dead-letter/"]
        TEMP["glue-temp/"]
    end

    subgraph Transitions["Storage Class Transitions"]
        IA["STANDARD_IA"]
        GLACIER["S3 Glacier"]
    end

    subgraph Deletions["Auto-Deletion"]
        DEL7["Deleted  · day 7"]
        DEL3["Deleted  · day 3"]
    end

    ARCH -->|"day 90"| GLACIER
    PROC -->|"day 30"| IA
    DL -->|"day 7"| DEL7
    TEMP -->|"day 3"| DEL3
```


### Project Structure

```
AWSProject1/
├── data/                                   raw input files
│   ├── songs/
│   │   └── songs.csv                       89,742 tracks with audio features
│   ├── users/
│   │   └── users.csv                       50,001 user profiles
│   └── streams/
│       ├── streams1.csv                    ~11,347 streaming events each
│       ├── streams2.csv
│       └── streams3.csv
│
├── terraform/                              all infrastructure-as-code
│   ├── main.tf                             provider + account ID locals
│   ├── variables.tf                        input variable declarations
│   ├── outputs.tf                          resource ARNs printed after apply
│   ├── terraform.tfvars                    region, sizing, table names
│   ├── s3.tf                               bucket, versioning, SSE, lifecycle, script uploads
│   ├── iam.tf                              Glue / Step Functions / EventBridge IAM roles
│   ├── dynamodb.tf                         music_kpis + music_top_genres + GSI + TTL
│   ├── glue.tf                             Glue catalog DB + 3 job definitions
│   ├── step_functions.tf                   state machine + CloudWatch log group
│   ├── eventbridge.tf                      S3 trigger rule -> Step Functions target
│   └── templates/
│       └── state_machine.json              ASL definition (rendered by Terraform templatefile)
│
├── glue_jobs/                              Glue job scripts (uploaded to S3 by Terraform)
│   ├── validation_job.py                   Python Shell: schema, nulls, types, referential integrity
│   ├── transformation_job.py               PySpark: join streams+songs+users, compute 6 KPIs
│   └── dynamodb_ingestion_job.py           Python Shell: JSON -> DynamoDB batch write
│
├── scripts/
│   ├── setup.py                            pre-flight check: Python, Terraform, AWS CLI, credentials
│   └── upload_data.py                      seed S3 with reference data + stream files
│
├── docs/
│   └── 2--ETL with s3, dynamo and Glue [updated].docx   project specification
│
├── .gitignore
├── requirements.txt                        boto3, pandas, pyarrow
└── README.md
```

### S3 Bucket Layout

```
music-streaming-pipeline-<account_id>/
├── raw/
│   ├── streams/                    <-- drop CSV files here to trigger the pipeline
│   └── reference/
│       ├── songs/
│       └── users/
├── processed/
│   └── <YYYY-MM-DD>/
│       ├── genre_kpis/
│       │   ├── parquet/            analytical archive
│       │   └── json/               interface for DynamoDB ingestion
│       ├── top_genres/
│       │   ├── parquet/
│       │   └── json/
│       └── reports/                validation + ingestion JSON summaries
├── archive/                        processed stream files (Glacier transition after 90 days)
├── dead-letter/                    failed files, auto-deleted after 7 days
│   ├── validation-errors/
│   ├── transform-errors/
│   └── ingest-errors/
├── glue-scripts/                   job scripts (managed by Terraform)
└── glue-temp/                      Spark shuffle + logs (deleted after 3 days)
```


### DynamoDB Tables

#### `music_kpis` — per-genre, per-day KPIs

| Key | Attribute | Type | Notes |
|-----|-----------|------|-------|
| PK  | `genre`   | S    |       |
| SK  | `date`    | S    | YYYY-MM-DD |
|     | `listen_count` | N | |
|     | `unique_listeners` | N | |
|     | `total_listen_time_ms` | N | |
|     | `avg_listen_time_ms` | N | |
|     | `top_3_songs` | S | JSON list |
|     | `processed_at` | S | ISO 8601 |
|     | `ttl_expiry` | N | Unix epoch, 90-day TTL |

**GSI `date-index`**: PK = `date`, SK = `listen_count` — enables top-5 genres query sorted by listen count.

#### `music_top_genres` — daily top-5 snapshot (O(1) lookup)

| Key | Attribute | Type | Notes |
|-----|-----------|------|-------|
| PK  | `record_type` | S | Always `"TOP_GENRES"` |
| SK  | `date` | S | YYYY-MM-DD |
|     | `top_5_genres` | S | JSON ordered list |
|     | `processed_at` | S | ISO 8601 |
|     | `ttl_expiry` | N | 90-day TTL |

Both tables use **PAY_PER_REQUEST** billing and have **PITR** enabled.

### KPIs Computed

| KPI | Granularity | Target Table |
|-----|-------------|--------------|
| Listen count | per genre per day | `music_kpis` |
| Unique listeners | per genre per day | `music_kpis` |
| Total listening time (ms) | per genre per day | `music_kpis` |
| Average listening time per user (ms) | per genre per day | `music_kpis` |
| Top 3 songs | per genre per day | `music_kpis` |
| Top 5 genres | per day | `music_top_genres` |


### Setup & Deployment

#### 1. Check your machine

```bash
python scripts/setup.py
```

Verifies Python >= 3.9, Terraform >= 1.5, AWS CLI, and live AWS credentials. Installs Python dependencies automatically.

#### 2. Deploy infrastructure

```bash
cd terraform
terraform init
terraform plan    # review what will be created
terraform apply   # provision everything in AWS
cd ..
```

#### 3. Seed reference data

```bash
python scripts/upload_data.py --reference-only
```

Uploads `songs.csv` and `users.csv` to `raw/reference/` in S3.

#### 4. Run the pipeline

```bash
# Upload one stream file — EventBridge fires and Step Functions starts
python scripts/upload_data.py --streams-only --file streams1.csv

# Upload all 3 files (triggers 3 separate executions)
python scripts/upload_data.py --streams-only
```

#### 5. Monitor

```
AWS Console -> Step Functions -> music-streaming-pipeline -> Executions
AWS Console -> CloudWatch    -> Log groups -> /aws/states/music-streaming-pipeline
AWS Console -> CloudWatch    -> Log groups -> /aws-glue/jobs/music-streaming-pipeline
```

#### Tear down

```bash
cd terraform
terraform destroy
```

Deletes every AWS resource and empties the S3 bucket. Costs stop immediately.

### DynamoDB Query Examples

```python
import boto3, json
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
kpis     = dynamodb.Table("music_kpis")
top      = dynamodb.Table("music_top_genres")

# All KPIs for genre "pop" on a specific date
item = kpis.get_item(Key={"genre": "pop", "date": "2024-06-25"})["Item"]

# Trend: all dates for genre "rock"
rows = kpis.query(KeyConditionExpression=Key("genre").eq("rock"))["Items"]

# Top 5 genres on a date, sorted by listen count (GSI)
rows = kpis.query(
    IndexName="date-index",
    KeyConditionExpression=Key("date").eq("2024-06-25"),
    ScanIndexForward=False,
    Limit=5,
)["Items"]

# Pre-computed top-5 snapshot (single item lookup)
snapshot = top.get_item(Key={"record_type": "TOP_GENRES", "date": "2024-06-25"})["Item"]
print(json.loads(snapshot["top_5_genres"]))

# Genre KPI for a date range
rows = kpis.query(
    KeyConditionExpression=Key("genre").eq("acoustic") & Key("date").between("2024-06-01", "2024-06-30")
)["Items"]
```

---

### Troubleshooting

| Symptom | Fix |
|---------|-----|
| Pipeline not triggered after upload | Run `terraform apply` first — S3 EventBridge notifications must be enabled |
| Glue job `AccessDenied` | Re-run `terraform apply` — IAM changes take ~30s to propagate |
| `UnprocessedItems` in DynamoDB logs | Normal under burst load — the job retries automatically |
| Step Functions `PipelineFailed` | Check `dead-letter/` prefix in S3 and CloudWatch Glue job logs |
| `terraform destroy` fails on S3 | Ensure `force_destroy = true` is set in `terraform/s3.tf` |
| Transformation job OOM | Increase `glue_num_workers` in `terraform.tfvars` and re-apply |
