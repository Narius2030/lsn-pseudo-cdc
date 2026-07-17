"""Transform CDC rows into Debezium-style events."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from .debezium import DebeziumEnvelopeBuilder
from .errors import TransformError


def group_rows_to_events(
    rows: Iterable[dict[str, Any]],
    *,
    builder: DebeziumEnvelopeBuilder,
    processed_at: datetime,
    emit_tombstone_on_delete: bool,
    fail_on_incomplete_update_pair: bool,
) -> list[dict[str, Any]]:
    ordered_rows = sorted(rows, key=lambda item: int(item["__$operation"]))
    if not ordered_rows:
        return []

    operations = [int(row["__$operation"]) for row in ordered_rows]
    commit_lsn = ordered_rows[0]["__$start_lsn"]
    change_lsn = ordered_rows[0]["__$seqval"]
    commit_time = ordered_rows[0].get("__$commit_time")

    if operations == [3, 4]:
        before = builder.extract_row_state(ordered_rows[0])
        after = builder.extract_row_state(ordered_rows[1])
        return [
            builder.build_message(
                op="u",
                before=before,
                after=after,
                commit_time=commit_time,
                change_lsn=change_lsn,
                commit_lsn=commit_lsn,
                event_serial_no=2,
                processed_at=processed_at,
            )
        ]

    if operations == [1, 2]:
        before = builder.extract_row_state(ordered_rows[0])
        after = builder.extract_row_state(ordered_rows[1])
        if _primary_key_changed(before, after, builder.capture_instance.primary_key_columns):
            events = [
                builder.build_message(
                    op="d",
                    before=before,
                    after=None,
                    commit_time=commit_time,
                    change_lsn=change_lsn,
                    commit_lsn=commit_lsn,
                    event_serial_no=1,
                    processed_at=processed_at,
                )
            ]
            if emit_tombstone_on_delete:
                events.append(builder.build_tombstone(before))
            events.append(
                builder.build_message(
                    op="c",
                    before=None,
                    after=after,
                    commit_time=commit_time,
                    change_lsn=change_lsn,
                    commit_lsn=commit_lsn,
                    event_serial_no=2,
                    processed_at=processed_at,
                )
            )
            return events

        return [
            builder.build_message(
                op="u",
                before=before,
                after=after,
                commit_time=commit_time,
                change_lsn=change_lsn,
                commit_lsn=commit_lsn,
                event_serial_no=2,
                processed_at=processed_at,
            )
        ]

    if (3 in operations or 4 in operations) and fail_on_incomplete_update_pair:
        raise TransformError(
            f"Incomplete update pair for commit_lsn={commit_lsn!r}, change_lsn={change_lsn!r}, operations={operations}"
        )

    events: list[dict[str, Any]] = []
    for index, row in enumerate(ordered_rows, start=1):
        operation = int(row["__$operation"])
        row_state = builder.extract_row_state(row)
        if operation == 1:
            events.append(
                builder.build_message(
                    op="d",
                    before=row_state,
                    after=None,
                    commit_time=row.get("__$commit_time"),
                    change_lsn=row["__$seqval"],
                    commit_lsn=row["__$start_lsn"],
                    event_serial_no=index,
                    processed_at=processed_at,
                )
            )
            if emit_tombstone_on_delete:
                events.append(builder.build_tombstone(row_state))
        elif operation == 2:
            events.append(
                builder.build_message(
                    op="c",
                    before=None,
                    after=row_state,
                    commit_time=row.get("__$commit_time"),
                    change_lsn=row["__$seqval"],
                    commit_lsn=row["__$start_lsn"],
                    event_serial_no=index,
                    processed_at=processed_at,
                )
            )
        elif operation in {3, 4}:
            continue
        else:
            raise TransformError(f"Unsupported CDC operation code: {operation}")
    return events


def _primary_key_changed(
    before: dict[str, Any],
    after: dict[str, Any],
    primary_key_columns: list[str],
) -> bool:
    if not primary_key_columns:
        return False
    return any(before.get(column_name) != after.get(column_name) for column_name in primary_key_columns)
