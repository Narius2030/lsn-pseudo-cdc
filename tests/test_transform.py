from __future__ import annotations

import unittest
from datetime import datetime, timezone

from sqlserver_cdc_s3.debezium import DebeziumEnvelopeBuilder
from sqlserver_cdc_s3.models import CaptureInstance, ColumnMetadata
from sqlserver_cdc_s3.transform import group_rows_to_events


def _sample_capture_instance() -> CaptureInstance:
    return CaptureInstance(
        capture_instance="dbo_customers",
        source_schema="dbo",
        source_table="customers",
        supports_net_changes=False,
        has_command_id=True,
        columns=[
            ColumnMetadata("id", 1, "int", None, None, None, None, False, True),
            ColumnMetadata("name", 2, "nvarchar", 200, None, None, None, True, False),
            ColumnMetadata("email", 3, "nvarchar", 200, None, None, None, True, False),
        ],
        primary_key_columns=["id"],
        current_min_lsn=bytes.fromhex("00000001000000010001"),
    )


class TransformTests(unittest.TestCase):
    def test_update_pair_becomes_single_update_event(self) -> None:
        builder = DebeziumEnvelopeBuilder(
            capture_instance=_sample_capture_instance(),
            topic_prefix="server1",
            database_name="sales",
            source_timezone="UTC",
            include_schemas=True,
        )

        rows = [
            {
                "__$start_lsn": bytes.fromhex("0000002700000AC00007"),
                "__$seqval": bytes.fromhex("0000002700000AC00002"),
                "__$operation": 3,
                "__$commit_time": datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "old@example.com",
            },
            {
                "__$start_lsn": bytes.fromhex("0000002700000AC00007"),
                "__$seqval": bytes.fromhex("0000002700000AC00002"),
                "__$operation": 4,
                "__$commit_time": datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "new@example.com",
            },
        ]

        events = group_rows_to_events(
            rows,
            builder=builder,
            processed_at=datetime(2026, 4, 12, 10, 1, tzinfo=timezone.utc),
            emit_tombstone_on_delete=False,
            fail_on_incomplete_update_pair=True,
        )

        self.assertEqual(len(events), 1)
        payload = events[0]
        self.assertEqual(payload["__op"], "u")
        self.assertEqual(payload["email"], "new@example.com")

    def test_delete_can_emit_tombstone(self) -> None:
        builder = DebeziumEnvelopeBuilder(
            capture_instance=_sample_capture_instance(),
            topic_prefix="server1",
            database_name="sales",
            source_timezone="UTC",
            include_schemas=False,
        )

        rows = [
            {
                "__$start_lsn": bytes.fromhex("0000002700000DB00007"),
                "__$seqval": bytes.fromhex("0000002700000DB00005"),
                "__$operation": 1,
                "__$commit_time": datetime(2026, 4, 12, 10, 5, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "old@example.com",
            }
        ]

        events = group_rows_to_events(
            rows,
            builder=builder,
            processed_at=datetime(2026, 4, 12, 10, 6, tzinfo=timezone.utc),
            emit_tombstone_on_delete=True,
            fail_on_incomplete_update_pair=True,
        )

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["__op"], "d")

    def test_deferred_update_pair_becomes_single_update_when_key_unchanged(self) -> None:
        builder = DebeziumEnvelopeBuilder(
            capture_instance=_sample_capture_instance(),
            topic_prefix="server1",
            database_name="sales",
            source_timezone="UTC",
            include_schemas=False,
        )

        rows = [
            {
                "__$start_lsn": bytes.fromhex("0000002800000AA00003"),
                "__$seqval": bytes.fromhex("0000002800000AA00001"),
                "__$operation": 1,
                "__$commit_time": datetime(2026, 4, 12, 11, 0, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "old@example.com",
            },
            {
                "__$start_lsn": bytes.fromhex("0000002800000AA00003"),
                "__$seqval": bytes.fromhex("0000002800000AA00001"),
                "__$operation": 2,
                "__$commit_time": datetime(2026, 4, 12, 11, 0, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "new@example.com",
            },
        ]

        events = group_rows_to_events(
            rows,
            builder=builder,
            processed_at=datetime(2026, 4, 12, 11, 1, tzinfo=timezone.utc),
            emit_tombstone_on_delete=False,
            fail_on_incomplete_update_pair=True,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["__op"], "u")
        self.assertEqual(events[0]["email"], "new@example.com")

    def test_deferred_update_pair_with_key_change_becomes_delete_plus_create(self) -> None:
        builder = DebeziumEnvelopeBuilder(
            capture_instance=_sample_capture_instance(),
            topic_prefix="server1",
            database_name="sales",
            source_timezone="UTC",
            include_schemas=False,
        )

        rows = [
            {
                "__$start_lsn": bytes.fromhex("0000002800000BB00004"),
                "__$seqval": bytes.fromhex("0000002800000BB00002"),
                "__$operation": 1,
                "__$commit_time": datetime(2026, 4, 12, 11, 5, tzinfo=timezone.utc),
                "id": 7,
                "name": "Alice",
                "email": "old@example.com",
            },
            {
                "__$start_lsn": bytes.fromhex("0000002800000BB00004"),
                "__$seqval": bytes.fromhex("0000002800000BB00002"),
                "__$operation": 2,
                "__$commit_time": datetime(2026, 4, 12, 11, 5, tzinfo=timezone.utc),
                "id": 8,
                "name": "Alice",
                "email": "new@example.com",
            },
        ]

        events = group_rows_to_events(
            rows,
            builder=builder,
            processed_at=datetime(2026, 4, 12, 11, 6, tzinfo=timezone.utc),
            emit_tombstone_on_delete=True,
            fail_on_incomplete_update_pair=True,
        )

        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]["__op"], "d")
        self.assertEqual(events[2]["__op"], "c")


if __name__ == "__main__":
    unittest.main()
