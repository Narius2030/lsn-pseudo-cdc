"""SQL Server CDC metadata and change readers."""

from __future__ import annotations

import logging
from collections.abc import Generator, Iterable
from typing import Any

import pyodbc

from .config import SqlServerConfig
from .errors import MetadataError
from .lsn import ensure_lsn_bytes
from .models import CDCHealthStatus, CaptureInstance, ColumnMetadata, ServerVersionInfo
from .retry import retry_with_backoff
from .versioning import classify_sqlserver_2014_sp1_build, parse_version_tuple


class SQLServerCDCReader:
    """Thin wrapper around pyodbc for SQL Server CDC reads."""

    def __init__(
        self,
        config: SqlServerConfig,
        *,
        logger: logging.Logger,
        retry_attempts: int,
        retry_backoff_seconds: float,
        retry_max_delay_seconds: float,
    ) -> None:
        self.config = config
        self.logger = logger
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.retry_max_delay_seconds = retry_max_delay_seconds
        self.connection: pyodbc.Connection | None = None

    def connect(self) -> None:
        def _connect() -> pyodbc.Connection:
            conn = pyodbc.connect(
                self.config.connection_string,
                timeout=self.config.login_timeout_seconds,
                autocommit=True,
            )
            conn.timeout = self.config.query_timeout_seconds
            cursor = conn.cursor()
            cursor.execute("SET NOCOUNT ON;")
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            cursor.execute("SET DEADLOCK_PRIORITY LOW;")
            cursor.execute(f"SET LOCK_TIMEOUT {int(self.config.lock_timeout_ms)};")
            cursor.close()
            return conn

        self.connection = retry_with_backoff(
            "connect to SQL Server",
            _connect,
            logger=self.logger,
            attempts=self.retry_attempts,
            base_delay_seconds=self.retry_backoff_seconds,
            max_delay_seconds=self.retry_max_delay_seconds,
            retry_on=(pyodbc.Error,),
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def reconnect(self) -> None:
        self.close()
        self.connect()

    def get_database_name(self) -> str:
        if self.config.database_name:
            return self.config.database_name
        row = self._fetch_one("SELECT DB_NAME() AS database_name")
        return str(row["database_name"])

    def get_server_version_info(self) -> ServerVersionInfo:
        row = self._fetch_one(
            """
            SELECT
                CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version,
                CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)) AS product_level,
                CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) AS edition;
            """
        )
        product_version = str(row["product_version"])
        build_tuple = parse_version_tuple(product_version)
        cu_metadata = classify_sqlserver_2014_sp1_build(build_tuple)
        return ServerVersionInfo(
            product_version=product_version,
            product_level=str(row["product_level"]),
            edition=str(row["edition"]),
            product_update_level=None,
            build_tuple=build_tuple,
            sqlserver_2014_sp1_cu_label=cu_metadata["cu_label"] if cu_metadata else None,
            sqlserver_2014_sp1_kb=cu_metadata["kb"] if cu_metadata else None,
            sqlserver_2014_sp1_release_date=cu_metadata["release_date"] if cu_metadata else None,
            sqlserver_2014_sp1_notes=cu_metadata["notes"] if cu_metadata else None,
        )

    def get_cdc_health_status(self) -> CDCHealthStatus:
        latest_scan_rows = self._fetch_all(
            """
            SELECT TOP (1)
                start_time,
                end_time,
                latency,
                error_count,
                empty_scan_count
            FROM sys.dm_cdc_log_scan_sessions
            ORDER BY session_id DESC;
            """
        )
        latest_error_rows = self._fetch_all(
            """
            SELECT TOP (1)
                entry_time,
                error_number,
                [error_message]
            FROM sys.dm_cdc_errors
            ORDER BY entry_time DESC;
            """
        )

        latest_scan = latest_scan_rows[0] if latest_scan_rows else {}
        latest_error = latest_error_rows[0] if latest_error_rows else {}

        return CDCHealthStatus(
            latest_scan_start_time=latest_scan.get("start_time"),
            latest_scan_end_time=latest_scan.get("end_time"),
            latest_scan_latency_seconds=_maybe_int(latest_scan.get("latency")),
            latest_scan_error_count=_maybe_int(latest_scan.get("error_count")),
            latest_scan_empty_scan_count=_maybe_int(latest_scan.get("empty_scan_count")),
            latest_error_time=latest_error.get("entry_time"),
            latest_error_number=_maybe_int(latest_error.get("error_number")),
            latest_error_message=latest_error.get("message"),
        )

    def get_current_max_lsn(self) -> bytes:
        row = self._fetch_one("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
        return ensure_lsn_bytes(row["max_lsn"]) or b""

    def get_incremented_lsn(self, lsn: bytes) -> bytes:
        row = self._fetch_one("SELECT sys.fn_cdc_increment_lsn(?) AS next_lsn", (lsn,))
        return ensure_lsn_bytes(row["next_lsn"]) or b""

    def get_min_lsn(self, capture_instance: str) -> bytes:
        row = self._fetch_one("SELECT sys.fn_cdc_get_min_lsn(?) AS min_lsn", (capture_instance,))
        min_lsn = ensure_lsn_bytes(row["min_lsn"])
        if not min_lsn or min_lsn == b"\x00" * 10:
            raise MetadataError(
                f"Cannot read min LSN for capture instance {capture_instance!r}. "
                "Verify CDC is enabled and the login has access to the captured columns."
            )
        return min_lsn

    def get_capture_instances(self) -> list[CaptureInstance]:
        rows = self._fetch_all("EXEC sys.sp_cdc_help_change_data_capture")
        instances: list[CaptureInstance] = []

        for row in rows:
            capture_instance = str(row["capture_instance"])
            source_schema = str(row["source_schema"])
            source_table = str(row["source_table"])
            supports_net_changes = bool(row.get("supports_net_changes"))
            has_command_id = self._capture_instance_has_command_id(capture_instance)
            columns = self._get_captured_columns(capture_instance, source_schema, source_table)
            current_min_lsn = self.get_min_lsn(capture_instance)
            primary_key_columns = [column.column_name for column in columns if column.is_primary_key]
            instances.append(
                CaptureInstance(
                    capture_instance=capture_instance,
                    source_schema=source_schema,
                    source_table=source_table,
                    supports_net_changes=supports_net_changes,
                    has_command_id=has_command_id,
                    columns=columns,
                    primary_key_columns=primary_key_columns,
                    current_min_lsn=current_min_lsn,
                )
            )

        return sorted(instances, key=lambda item: (item.source_schema, item.source_table, item.capture_instance))

    def get_window_end_lsn(
        self,
        capture_instance: str,
        from_lsn: bytes,
        to_lsn: bytes,
        *,
        window_size: int,
    ) -> bytes | None:
        change_table_name = self._quote_identifier(f"{capture_instance}_CT")
        query = f"""
        SELECT MAX(window_lsn) AS window_end_lsn
        FROM (
            SELECT DISTINCT TOP (?) __$start_lsn AS window_lsn
            FROM cdc.{change_table_name}
            WHERE __$start_lsn >= ? AND __$start_lsn <= ?
            ORDER BY __$start_lsn
        ) AS windowed;
        """
        row = self._fetch_one(query, (window_size, from_lsn, to_lsn))
        value = ensure_lsn_bytes(row["window_end_lsn"])
        return value

    def iter_table_rows(
        self,
        capture_instance: CaptureInstance,
    ) -> Generator[dict[str, Any], None, None]:
        source_table_name = self._quote_identifier(capture_instance.source_table)
        source_schema_name = self._quote_identifier(capture_instance.source_schema)
        captured_column_list = ",\n            ".join(
            self._quote_identifier(column.column_name) for column in capture_instance.columns
        )

        query = f"""
        SELECT
            {captured_column_list}
        FROM {source_schema_name}.{source_table_name} WITH (NOLOCK);
        """

        cursor = self._cursor()
        cursor.arraysize = self.config.fetch_size
        try:
            cursor.execute(query)
            column_names = [column[0] for column in cursor.description]

            while True:
                batch = cursor.fetchmany(self.config.fetch_size)
                if not batch:
                    break

                for row in batch:
                    yield self._row_to_dict(column_names, row)
        finally:
            cursor.close()

    def iter_change_groups(
        self,
        capture_instance: CaptureInstance,
        *,
        from_lsn: bytes,
        to_lsn: bytes,
    ) -> Generator[list[dict[str, Any]], None, None]:
        change_table_name = self._quote_identifier(f"{capture_instance.capture_instance}_CT")
        captured_column_list = ",\n            ".join(
            f"ct.{self._quote_identifier(column.column_name)}" for column in capture_instance.columns
        )
        command_id_select = (
            "ct.__$command_id AS __$command_id,"
            if capture_instance.has_command_id
            else "CAST(NULL AS int) AS __$command_id,"
        )
        order_parts = ["ct.__$start_lsn"]
        if capture_instance.has_command_id:
            order_parts.append("ct.__$command_id")
        order_parts.extend(["ct.__$seqval", "ct.__$operation"])

        query = f"""
        SELECT
            ct.__$start_lsn,
            ct.__$end_lsn,
            ct.__$seqval,
            ct.__$operation,
            ct.__$update_mask,
            {command_id_select}
            sys.fn_cdc_map_lsn_to_time(ct.__$start_lsn) AS __$commit_time,
            {captured_column_list}
        FROM cdc.{change_table_name} AS ct
        WHERE ct.__$start_lsn >= ? AND ct.__$start_lsn <= ?
        ORDER BY {", ".join(order_parts)};
        """

        cursor = self._cursor()
        cursor.arraysize = self.config.fetch_size
        try:
            cursor.execute(query, from_lsn, to_lsn)
            column_names = [column[0] for column in cursor.description]

            pending_key: tuple[bytes, int, bytes] | None = None
            pending_rows: list[dict[str, Any]] = []

            while True:
                batch = cursor.fetchmany(self.config.fetch_size)
                if not batch:
                    break

                for row in batch:
                    row_dict = self._row_to_dict(column_names, row)
                    key = (
                        ensure_lsn_bytes(row_dict["__$start_lsn"]) or b"",
                        int(row_dict["__$command_id"]) if row_dict.get("__$command_id") is not None else -1,
                        ensure_lsn_bytes(row_dict["__$seqval"]) or b"",
                    )
                    if pending_key is None:
                        pending_key = key
                    if key != pending_key:
                        yield pending_rows
                        pending_rows = []
                        pending_key = key
                    pending_rows.append(row_dict)

            if pending_rows:
                yield pending_rows
        finally:
            cursor.close()

    def _get_captured_columns(
        self,
        capture_instance: str,
        source_schema: str,
        source_table: str,
    ) -> list[ColumnMetadata]:
        captured_columns = self._fetch_all(
            "EXEC sys.sp_cdc_get_captured_columns @capture_instance=?",
            (capture_instance,),
        )
        primary_keys = set(self._get_primary_keys(source_schema, source_table))
        nullability = self._get_column_nullability(source_schema, source_table)

        columns: list[ColumnMetadata] = []
        for row in captured_columns:
            column_name = str(row["column_name"])
            columns.append(
                ColumnMetadata(
                    column_name=column_name,
                    column_ordinal=int(row["column_ordinal"]),
                    data_type=str(row["data_type"]),
                    character_maximum_length=_maybe_int(row.get("character_maximum_length")),
                    numeric_precision=_maybe_int(row.get("numeric_precision")),
                    numeric_scale=_maybe_int(row.get("numeric_scale")),
                    datetime_precision=_maybe_int(row.get("datetime_precision")),
                    is_nullable=bool(nullability.get(column_name, True)),
                    is_primary_key=column_name in primary_keys,
                )
            )
        return sorted(columns, key=lambda item: item.column_ordinal)

    def _capture_instance_has_command_id(self, capture_instance: str) -> bool:
        row = self._fetch_one(
            """
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM sys.columns
                    WHERE object_id = OBJECT_ID(?)
                      AND name = '__$command_id'
                ) THEN 1
                ELSE 0
            END AS has_command_id;
            """,
            (f"cdc.{capture_instance}_CT",),
        )
        return bool(row["has_command_id"])

    def _get_primary_keys(self, source_schema: str, source_table: str) -> list[str]:
        rows = self._fetch_all(
            """
            SELECT c.name AS column_name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            INNER JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.is_primary_key = 1
            INNER JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ? AND t.name = ?
            ORDER BY ic.key_ordinal;
            """,
            (source_schema, source_table),
        )
        return [str(row["column_name"]) for row in rows]

    def _get_column_nullability(self, source_schema: str, source_table: str) -> dict[str, bool]:
        rows = self._fetch_all(
            """
            SELECT c.name AS column_name, c.is_nullable
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            INNER JOIN sys.columns AS c ON c.object_id = t.object_id
            WHERE s.name = ? AND t.name = ?;
            """,
            (source_schema, source_table),
        )
        return {str(row["column_name"]): bool(row["is_nullable"]) for row in rows}

    def _fetch_one(self, query: str, params: Iterable[Any] | None = None) -> dict[str, Any]:
        rows = self._fetch_all(query, params)
        if not rows:
            raise MetadataError(f"Expected one row but query returned nothing: {query}")
        return rows[0]

    def _fetch_all(self, query: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
        def _run_query() -> list[dict[str, Any]]:
            cursor = self._cursor()
            try:
                if params is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, tuple(params))
                column_names = [column[0] for column in cursor.description] if cursor.description else []
                rows = [self._row_to_dict(column_names, row) for row in cursor.fetchall()]
                return rows
            finally:
                cursor.close()

        try:
            return retry_with_backoff(
                "execute SQL Server metadata query",
                _run_query,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(pyodbc.Error,),
            )
        except pyodbc.Error as exc:
            raise MetadataError(f"SQL Server query failed: {exc}") from exc

    def _cursor(self) -> pyodbc.Cursor:
        if self.connection is None:
            raise MetadataError("SQL Server connection is not initialized")
        return self.connection.cursor()

    @staticmethod
    def _row_to_dict(column_names: list[str], row: Any) -> dict[str, Any]:
        return {column_names[index]: row[index] for index in range(len(column_names))}

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f"[{identifier.replace(']', ']]')}]"


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
