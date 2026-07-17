"""Configuration loading and validation."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import ConfigurationError

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class SqlServerConfig:
    connection_string: str
    login_timeout_seconds: int = 15
    query_timeout_seconds: int = 180
    fetch_size: int = 1000
    lsn_window_size: int = 5000
    lock_timeout_ms: int = 5000
    database_name: str | None = None


@dataclass(frozen=True)
class S3Config:
    bucket: str = ""
    data_prefix: str = ""
    region_name: str | None = None
    endpoint_url: str | None = None
    profile_name: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    server_side_encryption: str | None = None
    ssekms_key_id: str | None = None


@dataclass(frozen=True)
class StateStoreConfig:
    type: str
    bucket: str | None = None
    key: str | None = None
    path: str | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    server_name: str
    topic_prefix: str
    include_capture_instances: tuple[str, ...] = ()
    exclude_capture_instances: tuple[str, ...] = ()
    source_timezone: str = "UTC"
    local_work_dir: str = "/tmp/sqlserver-cdc-s3"
    output_mode: str = "s3"
    local_output_dir: str = "./local-output"
    flatten_output: bool = False
    enable_compression: bool = True
    partition_by_date: bool = False
    snapshot_mode: str = "initial"  # "initial", "always", or "never"
    commit_bookmarks: bool = True
    validate_destination_on_startup: bool = True
    enforce_sqlserver_2014_sp1: bool = True
    allow_best_effort_without_command_id: bool = False
    inspect_cdc_health: bool = True
    fail_on_cdc_engine_errors: bool = True
    emit_tombstone_on_delete: bool = False
    include_schemas: bool = True
    cleanup_failed_runs: bool = True
    fail_on_lsn_gap: bool = True
    fail_on_incomplete_update_pair: bool = True
    min_lsn_window_size: int = 100
    window_size_reduction_factor: int = 2
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    retry_max_delay_seconds: float = 30.0


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    file_path: str | None = None


@dataclass(frozen=True)
class AppConfig:
    sqlserver: SqlServerConfig
    s3: S3Config
    state_store: StateStoreConfig
    runtime: RuntimeConfig
    logging: LoggingConfig


def load_config(config_path: str | os.PathLike[str]) -> AppConfig:
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationError(f"Config file not found: {path}")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"Invalid JSON in config file {path}: {exc}") from exc

    data = _expand_environment(raw)

    try:
        sqlserver = SqlServerConfig(**data["sqlserver"])
        s3 = S3Config(**data.get("s3", {}))
        state_store = StateStoreConfig(**data["state_store"])
        runtime_payload = dict(data["runtime"])
        runtime_payload.setdefault("topic_prefix", runtime_payload["server_name"])
        runtime_payload["include_capture_instances"] = _normalize_capture_instance_names(
            runtime_payload.get("include_capture_instances", ()),
            "runtime.include_capture_instances",
        )
        runtime_payload["exclude_capture_instances"] = _normalize_capture_instance_names(
            runtime_payload.get("exclude_capture_instances", ()),
            "runtime.exclude_capture_instances",
        )
        runtime = RuntimeConfig(**runtime_payload)
        logging_cfg = LoggingConfig(**data.get("logging", {}))
    except KeyError as exc:
        raise ConfigurationError(f"Missing required configuration section or field: {exc}") from exc
    except TypeError as exc:
        raise ConfigurationError(f"Invalid configuration fields: {exc}") from exc

    if not sqlserver.connection_string.strip():
        raise ConfigurationError("sqlserver.connection_string cannot be empty")
    if sqlserver.fetch_size < 1:
        raise ConfigurationError("sqlserver.fetch_size must be >= 1")
    if sqlserver.lsn_window_size < 1:
        raise ConfigurationError("sqlserver.lsn_window_size must be >= 1")
    if runtime.min_lsn_window_size < 1:
        raise ConfigurationError("runtime.min_lsn_window_size must be >= 1")
    if sqlserver.lsn_window_size < runtime.min_lsn_window_size:
        raise ConfigurationError("sqlserver.lsn_window_size must be >= runtime.min_lsn_window_size")
    if runtime.window_size_reduction_factor < 2:
        raise ConfigurationError("runtime.window_size_reduction_factor must be >= 2")
    if runtime.max_retries < 1:
        raise ConfigurationError("runtime.max_retries must be >= 1")
    if runtime.retry_backoff_seconds <= 0:
        raise ConfigurationError("runtime.retry_backoff_seconds must be > 0")
    if runtime.retry_max_delay_seconds <= 0:
        raise ConfigurationError("runtime.retry_max_delay_seconds must be > 0")
    if runtime.output_mode not in {"s3", "local"}:
        raise ConfigurationError("runtime.output_mode must be either 's3' or 'local'")
    if runtime.snapshot_mode not in {"initial", "always", "never"}:
        raise ConfigurationError("runtime.snapshot_mode must be one of 'initial', 'always', 'never'")
    try:
        ZoneInfo(runtime.source_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ConfigurationError(f"Invalid runtime.source_timezone: {runtime.source_timezone!r}") from exc
    if state_store.type not in {"s3", "file"}:
        raise ConfigurationError("state_store.type must be either 's3' or 'file'")
    if runtime.output_mode == "s3" and (not s3.bucket or not s3.data_prefix):
        raise ConfigurationError("s3.bucket and s3.data_prefix are required when runtime.output_mode='s3'")
    if state_store.type == "s3" and (not state_store.bucket or not state_store.key):
        raise ConfigurationError("state_store.bucket and state_store.key are required for S3 state store")
    if state_store.type == "file" and not state_store.path:
        raise ConfigurationError("state_store.path is required for file state store")
    if runtime.output_mode == "local" and not runtime.local_output_dir:
        raise ConfigurationError("runtime.local_output_dir is required when runtime.output_mode='local'")

    return AppConfig(
        sqlserver=sqlserver,
        s3=s3,
        state_store=state_store,
        runtime=runtime,
        logging=logging_cfg,
    )


def _normalize_capture_instance_names(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        raise ConfigurationError(f"{field_name} must be a list of capture instance names")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ConfigurationError(f"{field_name} must contain non-empty strings")
    names = tuple(item.strip() for item in value)
    if len(set(names)) != len(names):
        raise ConfigurationError(f"{field_name} must not contain duplicates")
    return names


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, str):
        return _ENV_PATTERN.sub(_replace_env_var, value)
    return value


def _replace_env_var(match: re.Match[str]) -> str:
    name = match.group(1)
    if name not in os.environ:
        raise ConfigurationError(f"Environment variable {name!r} was referenced but is not set")
    return os.environ[name]
