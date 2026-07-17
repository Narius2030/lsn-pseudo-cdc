from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sqlserver_cdc_s3.connector_runner import discover_connectors, run_connectors
from sqlserver_cdc_s3.errors import ConfigurationError


def _config(state_key: str, data_prefix: str) -> dict[str, object]:
    return {
        "sqlserver": {"connection_string": "DRIVER={ODBC Driver 18 for SQL Server};SERVER=test"},
        "s3": {"bucket": "data", "data_prefix": data_prefix},
        "state_store": {"type": "s3", "bucket": "state", "key": state_key},
        "runtime": {
            "server_name": "server",
            "output_mode": "s3",
            "include_capture_instances": ["dbo_orders"],
        },
    }


class ConnectorRunnerTests(unittest.TestCase):
    def _write_config(self, directory: Path, name: str, state_key: str, data_prefix: str) -> None:
        (directory / name).write_text(json.dumps(_config(state_key, data_prefix)), encoding="utf-8")

    def test_discovers_connectors_in_lexical_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            self._write_config(directory, "b.json", "state/b.json", "cdc/b")
            self._write_config(directory, "a.json", "state/a.json", "cdc/a")

            connectors = discover_connectors(directory)

        self.assertEqual([item.connector_id for item in connectors], ["a", "b"])
        self.assertEqual(connectors[0].config.runtime.include_capture_instances, ("dbo_orders",))

    def test_rejects_shared_bookmark_store(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            self._write_config(directory, "a.json", "state/shared.json", "cdc/a")
            self._write_config(directory, "b.json", "state/shared.json", "cdc/b")

            with self.assertRaisesRegex(ConfigurationError, "share bookmark state store"):
                discover_connectors(directory)

    def test_continues_after_failure_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            self._write_config(directory, "a.json", "state/a.json", "cdc/a")
            self._write_config(directory, "b.json", "state/b.json", "cdc/b")
            calls: list[str] = []

            def fake_pipeline(path: str, **_: object) -> dict[str, object]:
                calls.append(Path(path).stem)
                if Path(path).stem == "a":
                    raise RuntimeError("source unavailable")
                return {"run_id": "run-b"}

            result = run_connectors(directory, continue_on_error=True, pipeline_runner=fake_pipeline)

        self.assertEqual(calls, ["a", "b"])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["connectors_executed"], 2)
        self.assertEqual(result["results"][0]["status"], "failed")
        self.assertEqual(result["results"][1]["status"], "success")


if __name__ == "__main__":
    unittest.main()
