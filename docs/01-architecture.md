# Pseudo CDC — Architecture Deep-Dive

## Overview

`pseudo_cdc` is a Python pipeline that reads changes from **SQL Server CDC change tables** (`cdc.*_CT`) and writes **Debezium-compatible JSON** to S3 (or the local filesystem). It replaces Kafka Connect + the Debezium connector where SQL Server 2014 SP1 does not support the official connector.

## Pipeline Flow (8 Stages)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              run_pipeline()                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Stage 1: Connect & Version Inspect                                             │
│  ┌──────────────────────────────────────┐                                       │
│  │ pyodbc.connect() → get_server_version│                                       │
│  │ SET NOCOUNT ON, READ COMMITTED,      │                                       │
│  │ DEADLOCK_PRIORITY LOW, LOCK_TIMEOUT  │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 2: CDC Metadata & SQL Server 2014 SP1 Validation                         │
│  ┌──────────────────────────────────────┐                                       │
│  │ sp_cdc_help_change_data_capture      │                                       │
│  │ Check __$command_id existence        │                                       │
│  │ Validate build >= CU10 (12.0.4491)  │                                       │
│  │ Check CDC health DMVs               │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 3: Validate Output Destination                                           │
│  ┌──────────────────────────────────────┐                                       │
│  │ S3: head_bucket() probe              │                                       │
│  │ Local: mkdir()                        │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 4: Load Bookmarks & Fix max_lsn                                          │
│  ┌──────────────────────────────────────┐                                       │
│  │ bookmark_store.load()                │                                       │
│  │ sys.fn_cdc_get_max_lsn() → max_lsn  │                                       │
│  │ (max_lsn fixed for the entire run)  │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 5: Extract CDC Changes (per capture instance)                            │
│  ┌──────────────────────────────────────┐                                       │
│  │ FOR each capture_instance:           │                                       │
│  │   ├─ Optional: snapshot (full read)  │                                       │
│  │   ├─ Determine start_lsn from        │                                       │
│  │   │  bookmark or min_lsn             │                                       │
│  │   └─ Adaptive windowed extraction    │                                       │
│  │      (see 02-lsn-windowing.md)       │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 6: Write Manifest JSON                                                   │
│  ┌──────────────────────────────────────┐                                       │
│  │ summary.to_manifest() → S3/local     │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 7: Commit Bookmarks                                                      │
│  ┌──────────────────────────────────────┐                                       │
│  │ bookmark_store.save(committed)       │                                       │
│  └─────────────────┬────────────────────┘                                       │
│                    ▼                                                             │
│  Stage 8: Done                                                                  │
│  ┌──────────────────────────────────────┐                                       │
│  │ Return summary dict                   │                                       │
│  └──────────────────────────────────────┘                                       │
│                                                                                 │
│  ON FAILURE: rollback → delete all uploaded_keys from S3                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Module Map

```
src/sqlserver_cdc_s3/
├── pipeline.py        # Main orchestration, 8 stages, adaptive window loop
├── sqlserver.py       # CDC reader: metadata, change groups, window queries
├── transform.py       # CDC operation codes → Debezium events
├── debezium.py        # Flattened JSON envelope builder
├── s3_io.py           # S3ArtifactWriter / LocalArtifactWriter
├── state_store.py     # Bookmark persistence (S3 or file)
├── config.py          # Dataclass configuration, environment-variable expansion
├── models.py          # Data models: CaptureInstance, WindowResult, RunSummary
├── lsn.py             # LSN byte ↔ hex ↔ Debezium format conversion
├── versioning.py      # SQL Server 2014 SP1 CU build classification
├── retry.py           # Exponential backoff with jitter
├── errors.py          # Custom exception hierarchy
├── logging_utils.py   # Console + file logging with run_id
└── cli.py             # argparse entrypoint
```

## Data Flow

```
SQL Server CDC            pseudo_cdc                       S3 / Local
───────────────     ─────────────────────────       ─────────────────────
cdc.*_CT tables  →  pyodbc (windowed SELECT)  →  group_rows_to_events()
                                                        │
                                                        ▼
                                               DebeziumEnvelopeBuilder
                                               (flattened JSON record)
                                                        │
                                                        ▼
                                               Temp file (JSONL, opt gzip)
                                                        │
                                                        ▼
                                               ArtifactWriter.upload_file()
                                               → s3://{bucket}/{prefix}/{topic}.{db}.{schema}.{table}/
                                                  year=YYYY/month=MM/day=DD/{from_lsn}_{to_lsn}_{run_id}.json
```

## Output Format (Flattened Debezium)

Each record is one JSON line (JSONL) in a **flattened** format—without the Debezium envelope wrapper (`before`/`after`/`source`). Metadata is added directly to the record instead:

```json
{
  "column1": "value1",
  "column2": 123,
  "__op": "u",
  "__table": "MdCustomer",
  "__deleted": "false",
  "__source_ts_ms": 1718000000000
}
```

| Meta Field | Meaning |
|---|---|
| `__op` | Operation: `c` (create/insert), `u` (update), `d` (delete), `r` (snapshot read) |
| `__table` | Source table name |
| `__deleted` | `"true"` for a delete event |
| `__source_ts_ms` | Commit time in epoch milliseconds |

## Snapshot Mode

| Mode | Behavior |
|---|---|
| `initial` | Take a snapshot on the first run (when the capture instance has no bookmark) |
| `always` | Take a snapshot on every run (for testing/backfills) |
| `never` | Never take a snapshot; process incrementally only |

Snapshots read directly from the source table (`SELECT ... FROM schema.table WITH (NOLOCK)`) and emit events with `op="r"`.

## Error Handling & Rollback

- **On failure**: The pipeline deletes every artifact uploaded during the current run (`cleanup_failed_runs`).
- **Bookmarks are not committed** when the pipeline fails, so the next run retries from the previous position.
- **LSN gap detection**: If a bookmark is older than the CDC retention window (`sys.fn_cdc_get_min_lsn`), the pipeline raises `CDCGapError` or resets the position, depending on configuration.

## Connection Settings

The pipeline configures the connection with special settings to avoid affecting the OLTP workload:

```sql
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET DEADLOCK_PRIORITY LOW;
SET LOCK_TIMEOUT {lock_timeout_ms};
```

- **autocommit=True**: Does not hold long-running transactions.
- **ApplicationIntent=ReadOnly**: Routes to a read replica when an Always On Availability Group is available.
- **DEADLOCK_PRIORITY LOW**: If a deadlock occurs, the pipeline becomes the victim rather than blocking OLTP.
