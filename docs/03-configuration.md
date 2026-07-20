# Configuration & Deployment Reference

## Config File Structure

The configuration is a JSON file with five sections. It supports **environment-variable expansion** with the `${VAR_NAME}` syntax.

```json
{
  "sqlserver": { ... },
  "s3": { ... },
  "state_store": { ... },
  "runtime": { ... },
  "logging": { ... }
}
```

## Section: `sqlserver`

Configures the SQL Server connection.

| Key | Type | Default | Description |
|---|---|---|---|
| `connection_string` | string | *required* | Full ODBC connection string |
| `login_timeout_seconds` | int | 15 | Connection timeout |
| `query_timeout_seconds` | int | 180 | Timeout for each query |
| `fetch_size` | int | 1000 | Rows fetched per batch (`cursor.arraysize`) |
| `lsn_window_size` | int | 5000 | Distinct LSN values per window |
| `lock_timeout_ms` | int | 5000 | SQL Server LOCK_TIMEOUT (ms) |
| `database_name` | string | null | Overrides the database name (defaults to `DB_NAME()`) |

### Connection String Anatomy

```
DRIVER={ODBC Driver 18 for SQL Server};
SERVER=localhost;
DATABASE=sakila;
UID=sa;
PWD=<password>;
Encrypt=yes;
TrustServerCertificate=yes;
ApplicationIntent=ReadOnly
```

**Important**:
- `ApplicationIntent=ReadOnly` → routes to a read replica in an Always On Availability Group.
- `Encrypt=yes;TrustServerCertificate=yes` → uses TLS but skips certificate validation (for internal networks).
- `ODBC Driver 18` → the latest driver version, with TLS required by default.

## Section: `s3`

Configures the output destination when `runtime.output_mode = "s3"`.

| Key | Type | Default | Description |
|---|---|---|---|
| `bucket` | string | "" | S3 bucket name |
| `data_prefix` | string | "" | Prefix path (for example, `comc-sakila-cdc/sakila`) |
| `region_name` | string | null | AWS region |
| `endpoint_url` | string | null | Custom S3 endpoint (MinIO, LocalStack) |
| `profile_name` | string | null | AWS profile name |
| `access_key_id` | string | null | AWS access key (or use a profile/IAM role) |
| `secret_access_key` | string | null | AWS secret key |
| `session_token` | string | null | Temporary session token (STS) |
| `server_side_encryption` | string | null | `"AES256"` or `"aws:kms"` |
| `ssekms_key_id` | string | null | KMS key ID when using KMS encryption |

## Section: `state_store`

Stores bookmarks (the processed LSN position).

| Key | Type | Description |
|---|---|---|
| `type` | string | `"s3"` or `"file"` |
| `bucket` | string | S3 bucket (required when `type=s3`) |
| `key` | string | S3 key path (required when `type=s3`) |
| `path` | string | Local file path (required when `type=file`) |

### Bookmark S3 Path Convention

```
s3://{bucket}/{data_prefix}/database={db_name}/state/bookmarks.json
```

## Section: `runtime`

Configures the pipeline's main behavior.

| Key | Type | Default | Description |
|---|---|---|---|
| `server_name` | string | *required* | Logical server name (used in Debezium source metadata) |
| `topic_prefix` | string | = server_name | S3 key-path prefix (equivalent to a Kafka topic prefix) |
| `include_capture_instances` | string[] | `[]` | Capture-instance allow-list; leave empty to read all |
| `exclude_capture_instances` | string[] | `[]` | Capture-instance deny-list, applied after the allow-list |
| `source_timezone` | string | `"UTC"` | Source SQL Server timezone (IANA format) |
| `local_work_dir` | string | `/tmp/sqlserver-cdc-s3` | Temporary directory for file staging |
| `output_mode` | string | `"s3"` | `"s3"` or `"local"` |
| `local_output_dir` | string | `./local-output` | Output directory for local mode |
| `enable_compression` | bool | true | Gzip output files (`.json.gz`) |
| `partition_by_date` | bool | false | Add `year=/month=/day=` partitions to the S3 path |
| `snapshot_mode` | string | `"initial"` | `"initial"`, `"always"`, or `"never"` |
| `commit_bookmarks` | bool | true | Whether to save bookmarks after a successful run |
| `validate_destination_on_startup` | bool | true | Check S3 bucket access before extraction |
| `enforce_sqlserver_2014_sp1` | bool | true | Require a SQL Server 2014 SP1 build |
| `allow_best_effort_without_command_id` | bool | false | Allow execution without `__$command_id` |
| `inspect_cdc_health` | bool | true | Check CDC DMV health status |
| `fail_on_cdc_engine_errors` | bool | true | Fail when the CDC engine has a recent error |
| `emit_tombstone_on_delete` | bool | false | Emit a null-value tombstone record after a delete |
| `include_schemas` | bool | true | Include schema information in messages |
| `cleanup_failed_runs` | bool | true | Delete staged S3 artifacts on failure |
| `fail_on_lsn_gap` | bool | true | Fail when bookmark < CDC `min_lsn` (a gap) |
| `fail_on_incomplete_update_pair` | bool | true | Fail when op=3 has no op=4 |
| `flatten_output` | bool | false | (reserved) |
| `min_lsn_window_size` | int | 100 | Lower bound for window shrinking |
| `window_size_reduction_factor` | int | 2 | Divisor used to shrink a window |
| `max_retries` | int | 3 | Maximum retry attempts |
| `retry_backoff_seconds` | float | 2.0 | Base delay for exponential backoff |
| `retry_max_delay_seconds` | float | 30.0 | Delay cap |

## Section: `logging`

| Key | Type | Default | Description |
|---|---|---|---|
| `level` | string | `"INFO"` | Python log level: DEBUG, INFO, WARNING, ERROR |
| `file_path` | string | null | Path for a log file (in addition to stdout) |

## S3 Output Path Structure

### CDC Window Files

```
s3://{bucket}/{data_prefix}/{topic_prefix}.{database}.{schema}.{table}/
    year=YYYY/month=MM/day=DD/
    {from_lsn}_{to_lsn}_{run_id}.json[.gz]
```

Example:
```
s3://datalake-raw/comc-sakila-cdc/sakila/
    SAKILA_MASTER.sakila.dbo.customer/
    year=2026/month=07/day=17/
    0x00000028000001A40003_0x0000002A000002B50001_20260618T013000Z_a1b2c3d4e5f6.json
```

### Snapshot Files

```
s3://{bucket}/{data_prefix}/{topic_prefix}.{database}.{schema}.{table}/
    [year=.../month=.../day=.../]
    snapshot_{run_id}.json[.gz]
```

### Manifest Files

```
s3://{bucket}/{data_prefix}/database={database}/manifests/{run_id}.json
```

The manifest contains the run summary: files, record counts, and committed bookmarks.

## Environment Variable Expansion

The configuration supports `${VAR_NAME}` syntax:

```json
{
  "sqlserver": {
    "connection_string": "...;PWD=${SQLSERVER_PASSWORD};..."
  }
}
```

If a variable does not exist, loading immediately raises `ConfigurationError`.

## Deployment

### Docker

```dockerfile
FROM python:3.11-slim-bookworm
# Install ODBC Driver 18
RUN ... msodbcsql18 unixodbc-dev ...
WORKDIR /app
COPY src/ ./src/
RUN pip install -r requirements.txt
ENTRYPOINT ["sqlserver-cdc-s3"]
```

### CLI Docker mode

The production Docker image runs `pseudo-cdc-connectors --config-dir /configs` by default. The runner reads every top-level `*.json` file in filename order and runs them sequentially. Before it starts, it rejects connectors that share a bookmark store or output destination. A failed connector stops the run and returns exit code `1`; use `--continue-on-error` to run the remaining connectors, while still returning exit code `1` if any errors occur.

```bash
docker run --rm --network local_dev -v "$PWD/configs:/configs:ro,Z" ghcr.io/narius2030/lsn-pseudo-cdc:latest --config /configs/config.local.json
```

Each connector must use its own `state_store` and output prefix/path. Use `include_capture_instances` when tables in the same database need to be divided among connectors.

### CLI Usage

```bash
# Full run
python -m sqlserver_cdc_s3.cli --config config/config.local.json

# Preflight check only (no extraction)
python -m sqlserver_cdc_s3.cli --config config/config.local.json --preflight-only

# Override output mode
python -m sqlserver_cdc_s3.cli --config config/config.local.json --output-mode local

# Dry run (no bookmark commit)
python -m sqlserver_cdc_s3.cli --config config/config.local.json --no-commit-bookmarks
```

### Dependencies

```
pyodbc>=5.1.0      # SQL Server ODBC driver binding
boto3              # AWS S3 client
tzdata             # Timezone database (Windows only)
```

## Production Config Example

```json
{
  "sqlserver": {
    "connection_string": "DRIVER={ODBC Driver 18 for SQL Server};SERVER=${SQLSERVER_HOST},1433;DATABASE=${SQLSERVER_DATABASE};UID=${SQLSERVER_USER};PWD=${SQLSERVER_PASSWORD};Encrypt=yes;TrustServerCertificate=yes;ApplicationIntent=ReadOnly",
    "fetch_size": 500,
    "lsn_window_size": 1000,
    "lock_timeout_ms": 3000
  },
  "s3": {
    "bucket": "datalake-raw",
    "data_prefix": "comc-sakila-cdc/sakila",
    "region_name": "ap-southeast-1",
    "server_side_encryption": "AES256"
  },
  "state_store": {
    "type": "s3",
    "bucket": "datalake-raw",
    "key": "comc-sakila-cdc/sakila/database=sakila/state/bookmarks.json"
  },
  "runtime": {
    "server_name": "SAKILA_MASTER",
    "topic_prefix": "SAKILA_MASTER",
    "output_mode": "s3",
    "enable_compression": false,
    "partition_by_date": true,
    "snapshot_mode": "initial",
    "enforce_sqlserver_2014_sp1": false,
    "allow_best_effort_without_command_id": true,
    "fail_on_lsn_gap": true,
    "fail_on_incomplete_update_pair": true,
    "min_lsn_window_size": 100,
    "max_retries": 3
  },
  "logging": {
    "level": "INFO",
    "file_path": "./logs/sqlserver-cdc-s3.log"
  }
}
```
