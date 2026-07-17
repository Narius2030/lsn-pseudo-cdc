# SQL Server CDC to S3

This program reads all CDC changes from CDC-enabled SQL Server tables, converts them to Debezium-style JSON, and stores them in S3 as individual compressed `json.gz` files.

## Highlights

- Automatically discovers every CDC-enabled `capture_instance` in the database.
- Optimized specifically for SQL Server 2014 SP1 instead of assuming the same CDC behavior as newer versions.
- Reads `cdc.<capture_instance>_CT` directly to retain SQL Server 2014 SP1-specific metadata.
- When the change table includes `__$command_id`, uses it to preserve the correct order as recommended by Microsoft.
- Fails early by default when SQL Server 2014 SP1 is older than `CU10` (`12.0.4491.0`, released `2016-12-19`) or lacks `__$command_id`.
- Includes a local test mode that writes files to a local directory instead of uploading to S3.
- Supports the default `boto3` AWS credential chain and optional `profile_name`, `access_key_id`, `secret_access_key`, and `session_token` configuration.
- Fixes `max_lsn` for the entire run to maintain consistency.
- Splits extraction into LSN windows to avoid timeouts and reduce OOM risk.
- Automatically shrinks the LSN window and retries after timeouts or transport errors while reading CDC.
- Streams data directly to a local gzip file before uploading it to S3.
- Updates bookmarks only after the entire run succeeds.
- Performs a best-effort S3 rollback if a run fails.
- Includes a sample Airflow 3.x DAG using `airflow.sdk`.

The provided source code is a custom, highly specialized CDC extractor for SQL Server 2014 SP1. It is designed to overcome the limitations of older SQL Server versions while producing output that is compatible with modern data ecosystems (specifically Debezium).

Here is how it works in detail:

    1. CDC Change Reading Mechanism: Unlike newer SQL Server versions, which provide robust stored procedures for reading CDC changes, SQL Server 2014 SP1 has known issues with event ordering and LSN (Log Sequence Number) management. This code uses direct queries:
    * Direct Change Table Access: The SQLServerCDCReader queries the internal CDC change tables (e.g., cdc.schema_table_CT) directly. This is done to access the __$command_id column, which is critical for maintaining correct transaction order in SQL Server 2014 SP1 (fixed in CU10).
    * Adaptive LSN Windowing: Instead of reading all changes at once (which could cause OOM or timeouts), the pipeline.py uses "LSN Windows." It reads a fixed number of LSNs at a time. If the SQL Server becomes slow or the result set is too large, the system automatically shrinks the window size and retries.
    * Version Validation: On startup, it inspects the SQL Server build number. It specifically checks for CU10 (build 12.0.4491.0) to ensure __$command_id is available. If the server is older, it can either fail or proceed in a "best-effort" mode depending on configuration.
    * Stateful Bookmarking: It tracks progress using the __$start_lsn. These bookmarks are stored externally (in a local file or S3), ensuring that the extractor picks up exactly where it left off in the next run.

    2. Debezium-Style JSON Transformation: Although the source is SQL Server 2014, the output is transformed into a modern Debezium JSON envelope.
    * Envelope Construction: The DebeziumEnvelopeBuilder in debezium.py maps raw SQL rows (Delete=1, Insert=2, UpdateBefore=3, UpdateAfter=4) into Debezium operations (d, c, u).
    * Payload Structure: It generates a JSON object with before, after, and source blocks. The source block includes metadata like change_lsn, commit_lsn, and the table name, mimicking a real Debezium connector.
    * Data Type Mapping: SQL types (Decimal, DateTime, Binary) are converted into JSON-safe formats (strings, ISO timestamps, Base64).

    3. S3 Sink Functionality: The application acts as a scheduled sink connector, often triggered by Airflow:
    * Streaming Upload: As it reads records from SQL Server, it streams them into a local gzip file.
    * S3 Upload: Once a window is complete, it uploads the .json.gz file to S3 using boto3. It applies appropriate metadata (ContentType, ContentEncoding) and supports encryption (SSE-S3 or SSE-KMS).
    * Manifest & Rollback: At the end of a successful run, it writes a manifest.json file. If the process fails halfway, it performs a "best-effort" rollback by deleting the uploaded files from S3 to prevent partial data ingestion in downstream systems.

## Project structure

- `config/config.example.json`: example configuration file.
- `src/sqlserver_cdc_s3/`: main Python package.
- `dags/cdc_sqlserver_to_s3_dag.py`: sample Airflow DAG.
- `tests/`: unit tests for LSN handling and transformations.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp config/config.example.json config/config.json
sqlserver-cdc-s3 --config config/config.json
```

## Test locally before deploying to Airflow

```bash
cp config/config.local.example.json config/config.local.json
sqlserver-cdc-s3 --config config/config.local.json --preflight-only
sqlserver-cdc-s3 --config config/config.local.json --output-mode local --no-commit-bookmarks
```

With `output_mode=local`, CDC files are written under `runtime.local_output_dir`; no S3 configuration is required.

## Run multiple connectors in Docker

The Docker image runs every JSON connector configuration in `/configs`, ordered by filename. Mount the configuration directory as read-only. Each connector needs its own state store and output destination to prevent bookmarks or artifacts from being overwritten.

```bash
docker run --rm \
  --env-file connectors/production.env \
  -v "$PWD/connectors:/configs:ro" \
  pseudo-cdc:latest
```

Each file in `connectors/` is a standard configuration. For example, `connectors/crm-orders.json` reads only the desired capture instances:

```json
{
  "runtime": {
    "server_name": "CRM",
    "include_capture_instances": ["dbo_Orders", "dbo_OrderLines"]
  },
  "state_store": {
    "type": "s3",
    "bucket": "cdc-state",
    "key": "connectors/crm-orders/bookmarks.json"
  }
}
```

`runtime.include_capture_instances` is an optional allowlist. If it is empty, the connector reads every capture instance in the database. `runtime.exclude_capture_instances` is an optional denylist. If one connector fails, the runner stops and returns exit code 1. Use `--continue-on-error` to continue with the remaining connectors; it still returns exit code 1 if any connector fails.

```bash
docker run --rm -v "$PWD/connectors:/configs:ro" pseudo-cdc:latest --continue-on-error
```

## Operational notes

- Bookmarks are stored separately; neither source tables nor CDC change tables are modified.
- The program performs only `SELECT` statements, CDC metadata-reading functions, and session-level `SET` statements. It performs no `INSERT`, `UPDATE`, `DELETE`, or DDL, and does not modify SQL Server source data.
- SQL Server 2014 SP1 has an important distinction: Microsoft states that `__$seqval` should not be used to order a change table, and `__$command_id` was added only after hotfix `KB3030352` in SQL Server 2014 SP1 `CU10`.
- For `SQL Server 2014 SP1 CU13`, Microsoft's build list reports `12.0.4520.0`, but package `KB4019099` often appears as version `12.0.4522.0`; the program recognizes both as `CU13`.
- Before extraction, the program runs preflight checks for the SQL Server build, the presence of `__$command_id`, and CDC health DMVs (`sys.dm_cdc_log_scan_sessions`, `sys.dm_cdc_errors`).
- If the S3 environment does not allow `HeadBucket`, set `runtime.validate_destination_on_startup=false` to defer validation until the actual upload.
- If a cleanup job has removed data and a bookmark is older than the current `min_lsn`, the program fails explicitly by default.
- If a batch file fails during upload or transformation, its bookmark is not committed.
- S3 data is considered complete only after the manifest file at `.../manifests/<run_id>.json` is successfully written.
