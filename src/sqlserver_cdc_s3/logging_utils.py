"""Logging setup helpers."""

from __future__ import annotations

import logging
from pathlib import Path


class _DefaultRunIdFilter(logging.Filter):
    """Ensure every log record has a run_id field."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = "-"
        return True


def setup_logging(level: str, file_path: str | None = None) -> None:
    """Configure console logging and an optional file handler."""
    root_logger = logging.getLogger()
    if getattr(root_logger, "_sqlserver_cdc_logging_ready", False):
        return

    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] [run_id=%(run_id)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(_DefaultRunIdFilter())
    root_logger.addHandler(console_handler)

    if file_path:
        file_target = Path(file_path)
        file_target.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_target, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.addFilter(_DefaultRunIdFilter())
        root_logger.addHandler(file_handler)

    root_logger._sqlserver_cdc_logging_ready = True  # type: ignore[attr-defined]
