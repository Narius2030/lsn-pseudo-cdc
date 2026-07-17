"""Data models used across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ColumnMetadata:
    column_name: str
    column_ordinal: int
    data_type: str
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    datetime_precision: int | None
    is_nullable: bool
    is_primary_key: bool = False


@dataclass(frozen=True)
class CaptureInstance:
    capture_instance: str
    source_schema: str
    source_table: str
    supports_net_changes: bool
    has_command_id: bool
    columns: list[ColumnMetadata]
    primary_key_columns: list[str]
    current_min_lsn: bytes

    @property
    def qualified_table(self) -> str:
        return f"{self.source_schema}.{self.source_table}"


@dataclass
class WindowResult:
    capture_instance: str
    source_schema: str
    source_table: str
    from_lsn: str
    to_lsn: str
    records_written: int
    s3_key: str


@dataclass(frozen=True)
class ServerVersionInfo:
    product_version: str
    product_level: str
    edition: str
    product_update_level: str | None
    build_tuple: tuple[int, ...]
    sqlserver_2014_sp1_cu_label: str | None = None
    sqlserver_2014_sp1_kb: str | None = None
    sqlserver_2014_sp1_release_date: str | None = None
    sqlserver_2014_sp1_notes: str | None = None

    @property
    def major_version(self) -> int:
        return self.build_tuple[0]

    @property
    def is_sql_server_2014_sp1(self) -> bool:
        return self.major_version == 12 and self.product_level.upper() == "SP1"

    @property
    def has_sql_server_2014_sp1_cu10_or_newer(self) -> bool:
        return self.is_sql_server_2014_sp1 and self.build_tuple >= (12, 0, 4491, 0)

    @property
    def build_label(self) -> str:
        return ".".join(str(part) for part in self.build_tuple)


@dataclass(frozen=True)
class CDCHealthStatus:
    latest_scan_start_time: datetime | None
    latest_scan_end_time: datetime | None
    latest_scan_latency_seconds: int | None
    latest_scan_error_count: int | None
    latest_scan_empty_scan_count: int | None
    latest_error_time: datetime | None
    latest_error_number: int | None
    latest_error_message: str | None


@dataclass
class RunSummary:
    run_id: str
    database_name: str
    max_lsn: str
    started_at: datetime
    sqlserver_version: str | None = None
    cdc_extractor_mode: str | None = None
    finished_at: datetime | None = None
    files: list[WindowResult] = field(default_factory=list)
    committed_bookmarks: dict[str, str] = field(default_factory=dict)
    total_records: int = 0

    def to_manifest(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "database_name": self.database_name,
            "max_lsn": self.max_lsn,
            "sqlserver_version": self.sqlserver_version,
            "cdc_extractor_mode": self.cdc_extractor_mode,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "total_records": self.total_records,
            "committed_bookmarks": self.committed_bookmarks,
            "files": [
                {
                    "capture_instance": item.capture_instance,
                    "source_schema": item.source_schema,
                    "source_table": item.source_table,
                    "from_lsn": item.from_lsn,
                    "to_lsn": item.to_lsn,
                    "records_written": item.records_written,
                    "s3_key": item.s3_key,
                }
                for item in self.files
            ],
        }
