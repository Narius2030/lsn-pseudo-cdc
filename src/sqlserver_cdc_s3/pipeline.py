"""Main pipeline orchestration."""

from __future__ import annotations

from dataclasses import replace
import gzip
import json
import logging
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import pyodbc
from botocore.exceptions import BotoCoreError, ClientError

from .config import AppConfig, load_config
from .debezium import DebeziumEnvelopeBuilder
from .errors import CDCGapError, MetadataError, S3WriteError, VersionCompatibilityError
from .logging_utils import setup_logging
from .lsn import compare_lsn, lsn_bytes_to_hex, lsn_hex_to_bytes
from .models import CaptureInstance, RunSummary, ServerVersionInfo, WindowResult
from .s3_io import ArtifactWriter, build_artifact_writer, create_s3_client
from .sqlserver import SQLServerCDCReader
from .state_store import build_bookmark_store
from .transform import group_rows_to_events

SQLSERVER_2014_SP1_CU10_BUILD = (12, 0, 4491, 0)


def run_pipeline(
    config_path: str,
    *,
    preflight_only: bool = False,
    commit_bookmarks_override: bool | None = None,
    output_mode_override: str | None = None,
) -> dict[str, object]:
    """Run the CDC extraction pipeline and return a compact summary."""
    config = load_config(config_path)
    if output_mode_override is not None:
        config = _replace_runtime_config(config, output_mode=output_mode_override)
    if commit_bookmarks_override is not None:
        config = _replace_runtime_config(config, commit_bookmarks=commit_bookmarks_override)
    setup_logging(config.logging.level, config.logging.file_path)

    run_id = _build_run_id()
    logger = logging.LoggerAdapter(logging.getLogger(__name__), {"run_id": run_id})
    started_at = datetime.now(timezone.utc)
    uploaded_keys: list[str] = []

    work_dir = Path(config.runtime.local_work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    s3_client = None
    if config.runtime.output_mode == "s3" or config.state_store.type == "s3":
        s3_client = create_s3_client(config.s3)
    bookmark_store = build_bookmark_store(config, s3_client=s3_client, logger=logger)
    artifact_writer = build_artifact_writer(config, s3_client=s3_client, logger=logger)

    reader = SQLServerCDCReader(
        config.sqlserver,
        logger=logger,
        retry_attempts=config.runtime.max_retries,
        retry_backoff_seconds=config.runtime.retry_backoff_seconds,
        retry_max_delay_seconds=config.runtime.retry_max_delay_seconds,
    )

    try:
        logger.info("Stage 1/8 - connecting to SQL Server and inspecting engine version.")
        reader.connect()
        server_version = reader.get_server_version_info()
        database_name = reader.get_database_name()

        logger.info("Stage 2/8 - loading CDC metadata and validating SQL Server 2014 SP1 rules.")
        capture_instances = reader.get_capture_instances()
        capture_instances = _filter_capture_instances(capture_instances, config)
        if not capture_instances:
            logger.warning("No configured CDC capture instances were found in database %s.", database_name)
            return {
                "run_id": run_id,
                "database_name": database_name,
                "status": "no_capture_instances",
            }
        _validate_sql_server_environment(
            server_version=server_version,
            capture_instances=capture_instances,
            config=config,
            logger=logger,
        )
        _inspect_cdc_health(reader=reader, config=config, logger=logger)

        logger.info("Stage 3/8 - validating output destination access.")
        if config.runtime.validate_destination_on_startup:
            artifact_writer.probe_access()

        logger.info("Stage 4/8 - loading bookmark state and fixing max LSN for the run.")
        bookmarks = bookmark_store.load()
        max_lsn = reader.get_current_max_lsn()
        summary = RunSummary(
            run_id=run_id,
            database_name=database_name,
            max_lsn=lsn_bytes_to_hex(max_lsn),
            started_at=started_at,
            sqlserver_version=_server_version_label(server_version),
            cdc_extractor_mode=_extractor_mode(capture_instances),
        )

        if preflight_only:
            return {
                "run_id": run_id,
                "database_name": database_name,
                "status": "preflight_ok",
                "sqlserver_version": summary.sqlserver_version,
                "cdc_extractor_mode": summary.cdc_extractor_mode,
                "capture_instances": [item.capture_instance for item in capture_instances],
                "output_mode": config.runtime.output_mode,
                "commit_bookmarks": config.runtime.commit_bookmarks,
            }

        logger.info("Stage 5/8 - extracting CDC changes from %s capture instance(s).", len(capture_instances))
        committed_bookmarks = dict(bookmarks)
        for capture_instance in capture_instances:
            _process_capture_instance(
                capture_instance=capture_instance,
                config=config,
                reader=reader,
                artifact_writer=artifact_writer,
                summary=summary,
                uploaded_keys=uploaded_keys,
                committed_bookmarks=committed_bookmarks,
                max_lsn=max_lsn,
                database_name=database_name,
                logger=logger,
            )

        logger.info("Stage 6/8 - writing manifest to destination.")
        summary.finished_at = datetime.now(timezone.utc)
        summary.committed_bookmarks = committed_bookmarks
        manifest_key = _manifest_key(config, run_id, database_name)
        artifact_writer.put_json(manifest_key, summary.to_manifest())
        uploaded_keys.append(manifest_key)

        if config.runtime.commit_bookmarks:
            logger.info("Stage 7/8 - saving committed bookmarks.")
            bookmark_store.save(committed_bookmarks)
        else:
            logger.info("Stage 7/8 - skipping bookmark commit by configuration.")

        logger.info(
            "Stage 8/8 - pipeline completed. files=%s, total_records=%s",
            len(summary.files),
            summary.total_records,
        )
        return {
            "run_id": summary.run_id,
            "database_name": summary.database_name,
            "manifest_key": manifest_key,
            "total_records": summary.total_records,
            "files_written": len(summary.files),
            "max_lsn": summary.max_lsn,
            "sqlserver_version": summary.sqlserver_version,
            "cdc_extractor_mode": summary.cdc_extractor_mode,
            "output_mode": config.runtime.output_mode,
            "commit_bookmarks": config.runtime.commit_bookmarks,
        }
    except Exception:
        logger.exception("Pipeline failed. Starting rollback of staged artifacts.")
        if config.runtime.cleanup_failed_runs and uploaded_keys:
            with suppress(S3WriteError):
                artifact_writer.delete_keys(uploaded_keys)
        raise
    finally:
        reader.close()


def _filter_capture_instances(
    capture_instances: list[CaptureInstance], config: AppConfig
) -> list[CaptureInstance]:
    """Apply the connector's optional capture-instance allow/deny lists."""
    include = set(config.runtime.include_capture_instances)
    exclude = set(config.runtime.exclude_capture_instances)
    if not include and not exclude:
        return capture_instances

    selected = [
        item
        for item in capture_instances
        if (not include or item.capture_instance in include)
        and item.capture_instance not in exclude
    ]
    configured_names = include | exclude
    available_names = {item.capture_instance for item in capture_instances}
    missing_names = configured_names - available_names
    if missing_names:
        logging.getLogger(__name__).warning(
            "Configured capture instances not found in source metadata: %s",
            ", ".join(sorted(missing_names)),
        )
    return selected


def _process_capture_instance(
    *,
    capture_instance: CaptureInstance,
    config: AppConfig,
    reader: SQLServerCDCReader,
    artifact_writer: ArtifactWriter,
    summary: RunSummary,
    uploaded_keys: list[str],
    committed_bookmarks: dict[str, str],
    max_lsn: bytes,
    database_name: str,
    logger: logging.LoggerAdapter,
) -> None:
    bookmark_hex = committed_bookmarks.get(capture_instance.capture_instance)
    start_lsn = capture_instance.current_min_lsn

    should_snapshot = False
    if config.runtime.snapshot_mode == "always":
        should_snapshot = True
    elif config.runtime.snapshot_mode == "initial" and not bookmark_hex:
        should_snapshot = True

    if should_snapshot:
        _perform_snapshot(
            capture_instance=capture_instance,
            config=config,
            reader=reader,
            artifact_writer=artifact_writer,
            summary=summary,
            uploaded_keys=uploaded_keys,
            database_name=database_name,
            logger=logger,
        )

    if bookmark_hex:
        saved_lsn = lsn_hex_to_bytes(bookmark_hex)
        if compare_lsn(saved_lsn, capture_instance.current_min_lsn) < 0:
            message = (
                f"Bookmark for {capture_instance.capture_instance} is older than the current CDC low watermark. "
                f"saved={bookmark_hex}, min={lsn_bytes_to_hex(capture_instance.current_min_lsn)}"
            )
            if config.runtime.fail_on_lsn_gap:
                raise CDCGapError(message)
            logger.warning("%s. Resetting from current min_lsn.", message)
            start_lsn = capture_instance.current_min_lsn
        else:
            start_lsn = reader.get_incremented_lsn(saved_lsn)

    if compare_lsn(start_lsn, max_lsn) > 0:
        logger.info("No new changes for %s up to run max_lsn.", capture_instance.qualified_table)
        committed_bookmarks[capture_instance.capture_instance] = lsn_bytes_to_hex(max_lsn)
        return

    logger.info(
        "Processing %s using capture instance %s from %s to %s.",
        capture_instance.qualified_table,
        capture_instance.capture_instance,
        lsn_bytes_to_hex(start_lsn),
        lsn_bytes_to_hex(max_lsn),
    )

    current_from_lsn = start_lsn
    default_window_size = config.sqlserver.lsn_window_size
    builder = DebeziumEnvelopeBuilder(
        capture_instance=capture_instance,
        topic_prefix=config.runtime.topic_prefix,
        database_name=database_name,
        source_timezone=config.runtime.source_timezone,
        include_schemas=config.runtime.include_schemas,
    )

    windows_written = 0
    while compare_lsn(current_from_lsn, max_lsn) <= 0:
        window_result, current_to_lsn = _extract_window_adaptively(
            capture_instance=capture_instance,
            builder=builder,
            current_from_lsn=current_from_lsn,
            config=config,
            reader=reader,
            artifact_writer=artifact_writer,
            summary=summary,
            logger=logger,
            initial_window_size=default_window_size,
            max_lsn=max_lsn,
            database_name=database_name,
        )
        if current_to_lsn is None:
            break

        if window_result is not None:
            summary.files.append(window_result)
            summary.total_records += window_result.records_written
            uploaded_keys.append(window_result.s3_key)
            windows_written += 1

        if compare_lsn(current_to_lsn, max_lsn) >= 0:
            break
        current_from_lsn = reader.get_incremented_lsn(current_to_lsn)

    committed_bookmarks[capture_instance.capture_instance] = lsn_bytes_to_hex(max_lsn)
    logger.info(
        "Finished %s. windows_written=%s, bookmark=%s",
        capture_instance.qualified_table,
        windows_written,
        lsn_bytes_to_hex(max_lsn),
    )


def _perform_snapshot(
    *,
    capture_instance: CaptureInstance,
    config: AppConfig,
    reader: SQLServerCDCReader,
    artifact_writer: ArtifactWriter,
    summary: RunSummary,
    uploaded_keys: list[str],
    database_name: str,
    logger: logging.LoggerAdapter,
) -> None:
    logger.info("Performing full snapshot for %s.", capture_instance.qualified_table)
    
    builder = DebeziumEnvelopeBuilder(
        capture_instance=capture_instance,
        topic_prefix=config.runtime.topic_prefix,
        database_name=database_name,
        source_timezone=config.runtime.source_timezone,
        include_schemas=config.runtime.include_schemas,
    )

    temp_file_path: Path | None = None
    try:
        suffix = ".json.gz" if config.runtime.enable_compression else ".json"
        with NamedTemporaryFile(
            mode="wb",
            delete=False,
            suffix=suffix,
            dir=config.runtime.local_work_dir,
        ) as handle:
            temp_file_path = Path(handle.name)

        record_count = 0
        
        def _open_temp():
            if config.runtime.enable_compression:
                return gzip.open(temp_file_path, mode="wt", encoding="utf-8")
            return open(temp_file_path, mode="w", encoding="utf-8")

        processed_at = datetime.now(timezone.utc)
        # Snapshot uses a fixed LSN (the current min LSN of the capture instance is a safe bet for metadata)
        dummy_lsn = capture_instance.current_min_lsn

        with _open_temp() as handle:
            for row in reader.iter_table_rows(capture_instance):
                row_state = builder.extract_row_state(row)
                event = builder.build_message(
                    op="r",
                    before=None,
                    after=row_state,
                    commit_time=None,
                    change_lsn=dummy_lsn,
                    commit_lsn=dummy_lsn,
                    event_serial_no=record_count + 1,
                    processed_at=processed_at,
                )
                json.dump(event, handle, ensure_ascii=False)
                handle.write("\n")
                record_count += 1

        if record_count == 0:
            logger.info("Source table %s is empty. No snapshot file created.", capture_instance.qualified_table)
            return

        # Use a special key for snapshot
        s3_key = _snapshot_key(
            config,
            summary.run_id,
            capture_instance,
            summary.started_at,
            database_name,
        )
        artifact_writer.upload_file(temp_file_path, s3_key)

        logger.info(
            "Uploaded %s records for %s snapshot to %s",
            record_count,
            capture_instance.capture_instance,
            artifact_writer.describe_destination(s3_key),
        )

        summary.files.append(WindowResult(
            capture_instance=capture_instance.capture_instance,
            source_schema=capture_instance.source_schema,
            source_table=capture_instance.source_table,
            from_lsn="snapshot",
            to_lsn="snapshot",
            records_written=record_count,
            s3_key=s3_key,
        ))
        summary.total_records += record_count
        uploaded_keys.append(s3_key)

    finally:
        if temp_file_path is not None:
            with suppress(OSError):
                temp_file_path.unlink(missing_ok=True)


def _extract_window_adaptively(
    *,
    capture_instance: CaptureInstance,
    builder: DebeziumEnvelopeBuilder,
    current_from_lsn: bytes,
    config: AppConfig,
    reader: SQLServerCDCReader,
    artifact_writer: ArtifactWriter,
    summary: RunSummary,
    logger: logging.LoggerAdapter,
    initial_window_size: int,
    max_lsn: bytes,
    database_name: str,
) -> tuple[WindowResult | None, bytes | None]:
    window_size = initial_window_size
    sql_attempt = 0

    while True:
        current_to_lsn = reader.get_window_end_lsn(
            capture_instance.capture_instance,
            current_from_lsn,
            max_lsn,
            window_size=window_size,
        )
        if current_to_lsn is None:
            return None, None

        try:
            window_result = _extract_single_window_with_retry(
                capture_instance=capture_instance,
                builder=builder,
                current_from_lsn=current_from_lsn,
                current_to_lsn=current_to_lsn,
                config=config,
                reader=reader,
                artifact_writer=artifact_writer,
                summary=summary,
                logger=logger,
                database_name=database_name,
            )
            return window_result, current_to_lsn
        except pyodbc.Error as exc:
            sql_attempt += 1
            message = _sql_error_message(exc)
            reconnect_needed = _is_connection_sql_error(exc)
            shrinkable = _is_shrinkable_sql_error(exc)

            if reconnect_needed:
                logger.warning(
                    "SQL transport error while reading %s window %s..%s: %s. Reconnecting.",
                    capture_instance.capture_instance,
                    lsn_bytes_to_hex(current_from_lsn),
                    lsn_bytes_to_hex(current_to_lsn),
                    message,
                )
                reader.reconnect()

            if shrinkable and window_size > config.runtime.min_lsn_window_size:
                new_window_size = max(
                    config.runtime.min_lsn_window_size,
                    window_size // config.runtime.window_size_reduction_factor,
                )
                if new_window_size < window_size:
                    logger.warning(
                        "Shrinking CDC window for %s from %s to %s after SQL error: %s",
                        capture_instance.capture_instance,
                        window_size,
                        new_window_size,
                        message,
                    )
                    window_size = new_window_size
                    sql_attempt = 0
                    continue

            if sql_attempt >= config.runtime.max_retries or not _is_retryable_sql_error(exc):
                raise

            sleep_seconds = min(
                config.runtime.retry_max_delay_seconds,
                config.runtime.retry_backoff_seconds * (2 ** (sql_attempt - 1)),
            )
            logger.warning(
                "Retrying SQL window read for %s attempt %s/%s in %.2f seconds after error: %s",
                capture_instance.capture_instance,
                sql_attempt,
                config.runtime.max_retries,
                sleep_seconds,
                message,
            )
            time.sleep(sleep_seconds)


def _extract_single_window_with_retry(
    *,
    capture_instance: CaptureInstance,
    builder: DebeziumEnvelopeBuilder,
    current_from_lsn: bytes,
    current_to_lsn: bytes,
    config: AppConfig,
    reader: SQLServerCDCReader,
    artifact_writer: ArtifactWriter,
    summary: RunSummary,
    logger: logging.LoggerAdapter,
    database_name: str,
) -> WindowResult | None:
    sql_attempt = 0
    while True:
        try:
            return _extract_single_window(
                capture_instance=capture_instance,
                builder=builder,
                current_from_lsn=current_from_lsn,
                current_to_lsn=current_to_lsn,
                config=config,
                reader=reader,
                artifact_writer=artifact_writer,
                summary=summary,
                logger=logger,
                database_name=database_name,
            )
        except (BotoCoreError, ClientError, OSError, MetadataError) as exc:
            sql_attempt += 1
            if sql_attempt >= config.runtime.max_retries:
                raise
            sleep_seconds = min(
                config.runtime.retry_max_delay_seconds,
                config.runtime.retry_backoff_seconds * (2 ** (sql_attempt - 1)),
            )
            logger.warning(
                "Retrying destination/window processing for %s attempt %s/%s in %.2f seconds after error: %s",
                capture_instance.capture_instance,
                sql_attempt,
                config.runtime.max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def _extract_single_window(
    *,
    capture_instance: CaptureInstance,
    builder: DebeziumEnvelopeBuilder,
    current_from_lsn: bytes,
    current_to_lsn: bytes,
    config: AppConfig,
    reader: SQLServerCDCReader,
    artifact_writer: ArtifactWriter,
    summary: RunSummary,
    logger: logging.LoggerAdapter,
    database_name: str,
) -> WindowResult | None:
    temp_file_path: Path | None = None
    try:
        suffix = ".json.gz" if config.runtime.enable_compression else ".json"
        with NamedTemporaryFile(
            mode="wb",
            delete=False,
            suffix=suffix,
            dir=config.runtime.local_work_dir,
        ) as handle:
            temp_file_path = Path(handle.name)

        record_count = 0
        
        def _open_temp():
            if config.runtime.enable_compression:
                return gzip.open(temp_file_path, mode="wt", encoding="utf-8")
            return open(temp_file_path, mode="w", encoding="utf-8")

        with _open_temp() as handle:
            for group in reader.iter_change_groups(
                capture_instance,
                from_lsn=current_from_lsn,
                to_lsn=current_to_lsn,
            ):
                processed_at = datetime.now(timezone.utc)
                events = group_rows_to_events(
                    group,
                    builder=builder,
                    processed_at=processed_at,
                    emit_tombstone_on_delete=config.runtime.emit_tombstone_on_delete,
                    fail_on_incomplete_update_pair=config.runtime.fail_on_incomplete_update_pair,
                )
                for event in events:
                    json.dump(event, handle, ensure_ascii=False)
                    handle.write("\n")
                    record_count += 1

        if record_count == 0:
            logger.info(
                "No rows found for %s between %s and %s.",
                capture_instance.capture_instance,
                lsn_bytes_to_hex(current_from_lsn),
                lsn_bytes_to_hex(current_to_lsn),
            )
            return None

        s3_key = _window_key(
            config,
            summary.run_id,
            capture_instance,
            current_from_lsn,
            current_to_lsn,
            summary.started_at,
            database_name,
        )
        artifact_writer.upload_file(temp_file_path, s3_key)

        logger.info(
            "Uploaded %s records for %s to %s",
            record_count,
            capture_instance.capture_instance,
            artifact_writer.describe_destination(s3_key),
        )

        return WindowResult(
            capture_instance=capture_instance.capture_instance,
            source_schema=capture_instance.source_schema,
            source_table=capture_instance.source_table,
            from_lsn=lsn_bytes_to_hex(current_from_lsn),
            to_lsn=lsn_bytes_to_hex(current_to_lsn),
            records_written=record_count,
            s3_key=s3_key,
        )
    finally:
        if temp_file_path is not None:
            with suppress(OSError):
                temp_file_path.unlink(missing_ok=True)


def _build_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:12]}"


def _replace_runtime_config(config: AppConfig, **updates: object) -> AppConfig:
    return replace(config, runtime=replace(config.runtime, **updates))


def _validate_sql_server_environment(
    *,
    server_version: ServerVersionInfo,
    capture_instances: list[CaptureInstance],
    config: AppConfig,
    logger: logging.LoggerAdapter,
) -> None:
    logger.info(
        "Connected to SQL Server build=%s, level=%s, edition=%s, classified_sp1_build=%s, kb=%s",
        server_version.build_label,
        server_version.product_level,
        server_version.edition,
        server_version.sqlserver_2014_sp1_cu_label or "-",
        server_version.sqlserver_2014_sp1_kb or "-",
    )

    # if config.runtime.enforce_sqlserver_2014_sp1 and not server_version.is_sql_server_2014_sp1:
    #     raise VersionCompatibilityError(
    #         "This pipeline is configured for SQL Server 2014 SP1 CDC semantics, "
    #         f"but the connected instance is build {server_version.build_label} level {server_version.product_level}."
    #     )

    # if not server_version.is_sql_server_2014_sp1:
    #     return

    if server_version.build_tuple < SQLSERVER_2014_SP1_CU10_BUILD:
        message = (
            "SQL Server 2014 SP1 build "
            f"{server_version.build_label} is older than CU10 build 12.0.4491.0 "
            "(released December 19, 2016). Microsoft documents a CDC ordering bug in KB3030352; "
            "strict ordering cannot be guaranteed on pre-CU10 builds."
        )
        if not config.runtime.allow_best_effort_without_command_id:
            raise VersionCompatibilityError(message)
        logger.warning("%s Continuing in best-effort mode.", message)

    missing_command_id = [item.capture_instance for item in capture_instances if not item.has_command_id]
    if missing_command_id:
        message = (
            "The following capture instances do not expose __$command_id: "
            f"{', '.join(missing_command_id)}. Microsoft recommends __$command_id for ordering after the fix. "
            "This can happen on pre-CU10 builds or if CDC metadata was not upgraded cleanly."
        )
        if not config.runtime.allow_best_effort_without_command_id:
            raise VersionCompatibilityError(message)
        logger.warning("%s Continuing in best-effort mode.", message)
        return

    logger.info(
        "All capture instances expose __$command_id. Using change-table extraction with command-aware ordering."
    )


def _inspect_cdc_health(
    *,
    reader: SQLServerCDCReader,
    config: AppConfig,
    logger: logging.LoggerAdapter,
) -> None:
    if not config.runtime.inspect_cdc_health:
        return

    try:
        health = reader.get_cdc_health_status()
    except MetadataError as exc:
        logger.warning("Could not inspect CDC health DMVs: %s", exc)
        return

    logger.info(
        "CDC scan health: latency=%s, error_count=%s, empty_scan_count=%s, latest_scan_end=%s",
        health.latest_scan_latency_seconds,
        health.latest_scan_error_count,
        health.latest_scan_empty_scan_count,
        health.latest_scan_end_time,
    )

    if health.latest_scan_error_count and health.latest_scan_error_count > 0:
        message = (
            "The latest CDC log scan session reported errors. "
            f"latest_error_number={health.latest_error_number}, latest_error_time={health.latest_error_time}, "
            f"latest_error_message={health.latest_error_message!r}"
        )
        if config.runtime.fail_on_cdc_engine_errors:
            raise MetadataError(message)
        logger.warning(message)
    elif health.latest_error_message:
        logger.warning(
            "CDC has historical errors recorded. latest_error_number=%s latest_error_time=%s latest_error_message=%r",
            health.latest_error_number,
            health.latest_error_time,
            health.latest_error_message,
        )


def _server_version_label(server_version: ServerVersionInfo) -> str:
    parts = [server_version.build_label, server_version.product_level]
    if server_version.sqlserver_2014_sp1_cu_label:
        parts.append(server_version.sqlserver_2014_sp1_cu_label)
    if server_version.sqlserver_2014_sp1_kb:
        parts.append(server_version.sqlserver_2014_sp1_kb)
    return " | ".join(parts)


def _extractor_mode(capture_instances: list[CaptureInstance]) -> str:
    if capture_instances and all(item.has_command_id for item in capture_instances):
        return "change_table_command_id"
    return "change_table_best_effort"


def _manifest_key(config: AppConfig, run_id: str, database_name: str) -> str:
    return _join_key_prefix(
        config.s3.data_prefix,
        f"database={database_name}",
        "manifests",
        f"{run_id}.json",
    )


def _snapshot_key(
    config: AppConfig,
    run_id: str,
    capture_instance: CaptureInstance,
    started_at: datetime,
    database_name: str,
) -> str:
    table_folder = f"{config.runtime.topic_prefix}.{database_name}.{capture_instance.source_schema}.{capture_instance.source_table}"
    extension = ".json.gz" if config.runtime.enable_compression else ".json"
    parts = [config.s3.data_prefix, table_folder]
    
    if config.runtime.partition_by_date:
        parts.extend([
            f"year={started_at.strftime('%Y')}",
            f"month={started_at.strftime('%m')}",
            f"day={started_at.strftime('%d')}"
        ])
    
    filename = f"snapshot_{run_id}{extension}"
    parts.append(filename)
    return _join_key_prefix(*parts)


def _window_key(
    config: AppConfig,
    run_id: str,
    capture_instance: CaptureInstance,
    from_lsn: bytes,
    to_lsn: bytes,
    started_at: datetime,
    database_name: str,
) -> str:
    # table folder syntax: topic_prefix.database.schema.tablename
    table_folder = f"{config.runtime.topic_prefix}.{database_name}.{capture_instance.source_schema}.{capture_instance.source_table}"
    
    extension = ".json.gz" if config.runtime.enable_compression else ".json"
    parts = [config.s3.data_prefix, table_folder]
    
    if config.runtime.partition_by_date:
        parts.extend([
            f"year={started_at.strftime('%Y')}",
            f"month={started_at.strftime('%m')}",
            f"day={started_at.strftime('%d')}"
        ])
    
    # Filename includes LSN range and run_id for uniqueness
    filename = f"{lsn_bytes_to_hex(from_lsn)}_{lsn_bytes_to_hex(to_lsn)}_{run_id}{extension}"
    parts.append(filename)
    
    return _join_key_prefix(*parts)


def _join_key_prefix(*parts: str) -> str:
    cleaned = [part.strip("/") for part in parts if part and part.strip("/")]
    return "/".join(cleaned)


def _sql_error_message(exc: pyodbc.Error) -> str:
    return " ".join(str(item) for item in exc.args) or str(exc)


def _sql_error_state(exc: pyodbc.Error) -> str | None:
    if exc.args:
        first = exc.args[0]
        if isinstance(first, str) and len(first) >= 5:
            return first[:5]
    return None


def _is_retryable_sql_error(exc: pyodbc.Error) -> bool:
    state = _sql_error_state(exc)
    message = _sql_error_message(exc).lower()
    retryable_states = {"HYT00", "HYT01", "08S01", "08001", "40001"}
    retryable_terms = (
        "timeout",
        "timed out",
        "deadlock",
        "communication link failure",
        "transport-level error",
        "connection is busy",
        "connection was terminated",
    )
    return (state in retryable_states) or any(term in message for term in retryable_terms)


def _is_shrinkable_sql_error(exc: pyodbc.Error) -> bool:
    message = _sql_error_message(exc).lower()
    return any(term in message for term in ("timeout", "timed out", "memory", "deadlock"))


def _is_connection_sql_error(exc: pyodbc.Error) -> bool:
    state = _sql_error_state(exc)
    message = _sql_error_message(exc).lower()
    return state in {"08S01", "08001"} or any(
        term in message for term in ("communication link failure", "transport-level error", "connection was terminated")
    )
