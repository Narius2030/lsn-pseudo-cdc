# SQL Server CDC to S3

`sqlserver-cdc-s3` exports changes from SQL Server CDC-enabled tables as Debezium-style JSON events. It reads each capture instance's CDC change table, writes JSON artifacts (optionally gzip-compressed), and stores them in Amazon S3 or a local directory.

The extractor is specialized for SQL Server 2014 SP1 CDC behavior. It reads `cdc.<capture_instance>_CT` directly so that, where available, `__$command_id` can be used to preserve change ordering.

## Features

- Discovers CDC capture instances automatically, with optional include and exclude lists.
- Emits Debezium-style `before`, `after`, and `source` envelopes for inserts, updates, and deletes.
- Processes bounded LSN windows and shrinks/retries a window after retryable SQL errors.
- Keeps the run's maximum LSN fixed, so a run has a consistent upper bound.
- Supports `.json.gz` compression and S3 or local output.
- Persists bookmarks only after a successful run; performs a best-effort S3 cleanup after a failed run.
- Validates SQL Server build, CDC metadata, CDC health, and destination access during preflight.
- Includes commands for one connector and for a directory of independent connector configurations.

## Requirements

- Python 3.10 or later.
- An ODBC driver usable by `pyodbc`. On Linux, install [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server).
- Network access and credentials for the SQL Server database.
- CDC enabled for the database and the tables to export.
- AWS credentials and S3 access when using S3 output or an S3 bookmark store. Credentials can come from the standard boto3 credential chain, an AWS profile, or the `s3` configuration section.

For SQL Server 2014 SP1, use CU10 (`12.0.4491.0`) or later to get `__$command_id`. The application can be configured to stop if that column is absent, or to continue in best-effort mode.

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Narius2030/lsn-pseudo-cdc.git
cd lsn-pseudo-cdc
```

### 2. Create and activate a virtual environment

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install the application

For normal use:

```bash
python -m pip install --upgrade pip
python -m pip install .
```

For local development, including Ruff:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Airflow is optional. Install it only if you intend to use the sample DAG:

```bash
python -m pip install -e ".[airflow]"
```

## Configuration

Start from the provided template:

```bash
cp configs/config.example.json configs/config.json
```

Edit `configs/config.json` and replace every placeholder. Do not commit credentials or the resulting local configuration file.

The top-level sections are:

| Section | Purpose |
| --- | --- |
| `sqlserver` | ODBC connection string, timeouts, fetch size, and LSN window size. |
| `s3` | Output bucket/prefix, AWS credentials or profile, endpoint, and encryption settings. |
| `state_store` | Bookmark storage, either `file` or `s3`. |
| `runtime` | Output mode, directories, capture-instance filtering, snapshots, retries, and safety checks. |
| `logging` | Log level and optional log file path. |

### Local, non-destructive trial

For a first run, use local output and a file bookmark. This avoids S3 uploads and makes the exported files easy to inspect. Set these values in your configuration:

```json
{
  "state_store": {
    "type": "file",
    "path": "runtime/bookmarks.json"
  },
  "runtime": {
    "output_mode": "local",
    "local_work_dir": "runtime/work",
    "local_output_dir": "runtime/output",
    "commit_bookmarks": false
  }
}
```

Keep the other required settings from the template, especially `sqlserver.connection_string`, `runtime.server_name`, `runtime.topic_prefix`, and `runtime.source_timezone`. When `output_mode` is `local`, S3 output settings are not used; an S3 state store still requires S3 settings and credentials.

`runtime.include_capture_instances` may contain an allowlist, and `runtime.exclude_capture_instances` may contain a denylist. An empty allowlist means all discovered capture instances are eligible.

## Usage

### 1. Verify configuration and connectivity

Preflight checks SQL Server connectivity, CDC metadata and health, and (when applicable) destination access without extracting changes:

```bash
sqlserver-cdc-s3 --config configs/config.json --preflight-only
```

### 2. Run locally without saving bookmarks

This is the safest extraction command for validation. It forces local output and prevents bookmark commits, regardless of the values in the configuration file:

```bash
sqlserver-cdc-s3 \
  --config configs/config.json \
  --output-mode local \
  --no-commit-bookmarks
```

Inspect files under `runtime.local_output_dir`. A successful command prints a JSON summary to standard output.

### 3. Run with the configured output and bookmark behavior

After validation, run the connector with the configuration's `runtime.output_mode` and `runtime.commit_bookmarks` settings:

```bash
sqlserver-cdc-s3 --config configs/config.json
```

To run against S3 but deliberately leave bookmark state unchanged, add `--no-commit-bookmarks`:

```bash
sqlserver-cdc-s3 --config configs/config.json --no-commit-bookmarks
```

### Run multiple connectors

Place one JSON configuration per connector in a directory. Connector files are loaded in filename order. Each must use a distinct bookmark location and output destination.

```bash
pseudo-cdc-connectors --config-dir connectors --preflight-only
pseudo-cdc-connectors --config-dir connectors
```

Use `--continue-on-error` to run remaining connectors after one fails; the process still exits with status 1 when any connector fails. The multi-connector command also supports `--output-mode local` and `--no-commit-bookmarks`.

## Docker

Build an image with the SQL Server ODBC driver included:

```bash
docker build -t pseudo-cdc:latest .
```

Run one connector by mounting its configuration:

```bash
docker run --rm \
  --env-file connectors/production.env \
  -v "$PWD/configs:/configs:ro" \
  pseudo-cdc:latest --config /configs/config.json
```

The image entrypoint is `sqlserver-cdc-s3`. For multiple connectors, override the entrypoint:

```bash
docker run --rm \
  -v "$PWD/connectors:/configs:ro" \
  --entrypoint pseudo-cdc-connectors \
  pseudo-cdc:latest --config-dir /configs
```

## Development checks

Run these commands from the repository root after installing `.[dev]`.

### Run the test suite

The tests use Python's standard-library `unittest` runner:

```bash
python -m unittest discover -s tests -v
```

Run one test module while iterating:

```bash
python -m unittest tests.test_transform -v
```

### Run Ruff

Check lint rules:

```bash
ruff check .
```

Apply Ruff's safe automatic fixes, then review the changes:

```bash
ruff check . --fix
```

Check formatting without changing files:

```bash
ruff format . --check
```

Format files:

```bash
ruff format .
```

Before opening a pull request, run:

```bash
python -m unittest discover -s tests -v
ruff check .
ruff format . --check
```

## Operational notes

- The extractor does not write to SQL Server source tables or CDC change tables. It uses reads and session-level settings only.
- Bookmarks advance only after the complete run succeeds. If a stored bookmark is older than the current CDC `min_lsn`, the run fails by default rather than silently skipping data.
- S3 output is complete only after its manifest is written under `manifests/<run_id>.json` beneath the configured data prefix.
- If startup bucket validation is not permitted by your S3 policy, set `runtime.validate_destination_on_startup` to `false`; the upload itself will still require the necessary permissions.
- Set `runtime.allow_best_effort_without_command_id` to `false` to require `__$command_id` and fail on older SQL Server 2014 SP1 environments.
