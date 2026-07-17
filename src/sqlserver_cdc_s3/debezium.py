"""Debezium-style JSON envelope builder."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from .lsn import lsn_bytes_to_debezium
from .models import CaptureInstance, ColumnMetadata


@dataclass
class DebeziumEnvelopeBuilder:
    capture_instance: CaptureInstance
    topic_prefix: str
    database_name: str
    source_timezone: str
    include_schemas: bool

    def build_message(
        self,
        *,
        op: str,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        commit_time: datetime | None,
        change_lsn: bytes,
        commit_lsn: bytes,
        event_serial_no: int,
        processed_at: datetime,
    ) -> dict[str, Any]:
        # User requested a flattened format with specific metadata fields prefixed with __
        record = after if after is not None else (before or {})
        
        # Add requested metadata
        record["__op"] = op
        record["__table"] = self.capture_instance.source_table
        record["__deleted"] = "true" if op == "d" else "false"
        record["__source_ts_ms"] = self._to_epoch(commit_time, "ms") if commit_time else self._to_epoch(processed_at, "ms")
        
        return record

    def build_tombstone(self, row_state: dict[str, Any] | None) -> dict[str, Any]:
        key_payload = self._build_key_payload(row_state)
        return {
            "key": self._wrap_schema(self._build_key_schema(), key_payload) if key_payload is not None else None,
            "value": None,
        }

    def extract_row_state(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            column.column_name: _json_safe_value(row.get(column.column_name))
            for column in sorted(self.capture_instance.columns, key=lambda item: item.column_ordinal)
        }

    def _build_key_payload(self, row_state: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row_state or not self.capture_instance.primary_key_columns:
            return None
        return {column_name: row_state.get(column_name) for column_name in self.capture_instance.primary_key_columns}

    def _build_source_payload(
        self,
        *,
        commit_time: datetime | None,
        change_lsn: bytes,
        commit_lsn: bytes,
        event_serial_no: int,
    ) -> dict[str, Any]:
        return {
            "version": "3.0.0.Final-simulated",
            "connector": "sqlserver",
            "name": self.topic_prefix,
            "ts_ms": self._to_epoch(commit_time, "ms") if commit_time else None,
            "ts_us": self._to_epoch(commit_time, "us") if commit_time else None,
            "ts_ns": self._to_epoch(commit_time, "ns") if commit_time else None,
            "snapshot": False,
            "db": self.database_name,
            "schema": self.capture_instance.source_schema,
            "table": self.capture_instance.source_table,
            "change_lsn": lsn_bytes_to_debezium(change_lsn),
            "commit_lsn": lsn_bytes_to_debezium(commit_lsn),
            "event_serial_no": str(event_serial_no),
        }

    def _wrap_schema(self, schema: dict[str, Any], payload: Any) -> Any:
        if not self.include_schemas:
            return payload
        return {"schema": schema, "payload": payload}

    def _build_key_schema(self) -> dict[str, Any]:
        if not self.capture_instance.primary_key_columns:
            return {
                "type": "struct",
                "fields": [],
                "optional": True,
                "name": self._schema_name("Key"),
                "version": 1,
            }

        fields = []
        for column in self.capture_instance.columns:
            if column.column_name in self.capture_instance.primary_key_columns:
                fields.append(_field_schema(column, optional=False))

        return {
            "type": "struct",
            "fields": fields,
            "optional": False,
            "name": self._schema_name("Key"),
            "version": 1,
        }

    def _build_row_struct_schema(self) -> dict[str, Any]:
        return {
            "type": "struct",
            "fields": [_field_schema(column, optional=column.is_nullable) for column in self.capture_instance.columns],
            "optional": True,
            "name": self._schema_name("Value"),
            "version": 1,
        }

    def _build_source_schema(self) -> dict[str, Any]:
        return {
            "type": "struct",
            "fields": [
                {"field": "version", "type": "string", "optional": False},
                {"field": "connector", "type": "string", "optional": False},
                {"field": "name", "type": "string", "optional": False},
                {"field": "ts_ms", "type": "int64", "optional": True},
                {"field": "ts_us", "type": "int64", "optional": True},
                {"field": "ts_ns", "type": "int64", "optional": True},
                {"field": "snapshot", "type": "boolean", "optional": False},
                {"field": "db", "type": "string", "optional": False},
                {"field": "schema", "type": "string", "optional": False},
                {"field": "table", "type": "string", "optional": False},
                {"field": "change_lsn", "type": "string", "optional": False},
                {"field": "commit_lsn", "type": "string", "optional": False},
                {"field": "event_serial_no", "type": "string", "optional": False},
            ],
            "optional": False,
            "name": "io.debezium.connector.sqlserver.Source",
            "version": 1,
        }

    def _build_value_schema(self) -> dict[str, Any]:
        row_struct = self._build_row_struct_schema()
        return {
            "type": "struct",
            "fields": [
                {**row_struct, "field": "before", "optional": True},
                {**row_struct, "field": "after", "optional": True},
                {**self._build_source_schema(), "field": "source", "optional": False},
                {"field": "op", "type": "string", "optional": False},
                {"field": "ts_ms", "type": "int64", "optional": True},
                {"field": "ts_us", "type": "int64", "optional": True},
                {"field": "ts_ns", "type": "int64", "optional": True},
            ],
            "optional": False,
            "name": self._schema_name("Envelope"),
            "version": 1,
        }

    def _schema_name(self, suffix: str) -> str:
        return (
            f"{self.topic_prefix}."
            f"{self.capture_instance.source_schema}."
            f"{self.database_name}."
            f"{self.capture_instance.source_table}."
            f"{suffix}"
        )

    def _to_epoch(self, value: datetime | None, precision: str) -> int | None:
        if value is None:
            return None
        tz = ZoneInfo(self.source_timezone)
        dt = value if value.tzinfo else value.replace(tzinfo=tz)
        dt_utc = dt.astimezone(timezone.utc)
        timestamp = dt_utc.timestamp()
        if precision == "ms":
            return int(timestamp * 1_000)
        if precision == "us":
            return int(timestamp * 1_000_000)
        if precision == "ns":
            return int(timestamp * 1_000_000_000)
        raise ValueError(f"Unsupported precision: {precision}")


def _field_schema(column: ColumnMetadata, *, optional: bool) -> dict[str, Any]:
    sql_type = column.data_type.lower()
    field_type = "string"

    if sql_type in {"bigint"}:
        field_type = "int64"
    elif sql_type in {"int"}:
        field_type = "int32"
    elif sql_type in {"smallint", "tinyint"}:
        field_type = "int16"
    elif sql_type in {"bit"}:
        field_type = "boolean"
    elif sql_type in {"real"}:
        field_type = "float"
    elif sql_type in {"float"}:
        field_type = "double"

    schema = {
        "field": column.column_name,
        "type": field_type,
        "optional": optional,
    }

    parameters: dict[str, str] = {"sqlserver_type": column.data_type}
    if column.numeric_precision is not None:
        parameters["numeric_precision"] = str(column.numeric_precision)
    if column.numeric_scale is not None:
        parameters["numeric_scale"] = str(column.numeric_scale)
    if column.character_maximum_length is not None:
        parameters["character_maximum_length"] = str(column.character_maximum_length)
    if parameters:
        schema["parameters"] = parameters

    return schema


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return value
