"""Bookmark state storage backends."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from .config import AppConfig
from .errors import StateError
from .retry import retry_with_backoff


class BookmarkStore:
    """Load and save per-capture-instance bookmarks."""

    def load(self) -> dict[str, str]:
        raise NotImplementedError

    def save(self, bookmarks: dict[str, str]) -> None:
        raise NotImplementedError


class FileBookmarkStore(BookmarkStore):
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StateError(f"Cannot load local bookmark state from {self.path}: {exc}") from exc
        return dict(payload.get("capture_instances", {}))

    def save(self, bookmarks: dict[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"capture_instances": bookmarks}
        try:
            self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            raise StateError(f"Cannot write local bookmark state to {self.path}: {exc}") from exc


class S3BookmarkStore(BookmarkStore):
    def __init__(
        self,
        *,
        s3_client: Any,
        bucket: str,
        key: str,
        retry_attempts: int,
        retry_backoff_seconds: float,
        retry_max_delay_seconds: float,
        logger: Any,
    ) -> None:
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.retry_max_delay_seconds = retry_max_delay_seconds
        self.logger = logger

    def load(self) -> dict[str, str]:
        def _load() -> dict[str, str]:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code")
                if error_code in {"NoSuchKey", "404"}:
                    return {}
                raise
            payload = response["Body"].read().decode("utf-8")
            data = json.loads(payload)
            return dict(data.get("capture_instances", {}))

        try:
            return retry_with_backoff(
                "load bookmark state from S3",
                _load,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(BotoCoreError, ClientError, json.JSONDecodeError, OSError),
            )
        except (BotoCoreError, ClientError, json.JSONDecodeError, OSError) as exc:
            raise StateError(f"Cannot load bookmark state from s3://{self.bucket}/{self.key}: {exc}") from exc

    def save(self, bookmarks: dict[str, str]) -> None:
        payload = json.dumps({"capture_instances": bookmarks}, indent=2, sort_keys=True).encode("utf-8")

        def _save() -> None:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=payload,
                ContentType="application/json",
            )

        try:
            retry_with_backoff(
                "save bookmark state to S3",
                _save,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(BotoCoreError, ClientError, OSError),
            )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise StateError(f"Cannot save bookmark state to s3://{self.bucket}/{self.key}: {exc}") from exc


def build_bookmark_store(config: AppConfig, *, s3_client: Any | None, logger: Any) -> BookmarkStore:
    if config.state_store.type == "file":
        return FileBookmarkStore(config.state_store.path or "")
    if s3_client is None:
        raise StateError("S3 state store requires an initialized S3 client")
    return S3BookmarkStore(
        s3_client=s3_client,
        bucket=config.state_store.bucket or "",
        key=config.state_store.key or "",
        retry_attempts=config.runtime.max_retries,
        retry_backoff_seconds=config.runtime.retry_backoff_seconds,
        retry_max_delay_seconds=config.runtime.retry_max_delay_seconds,
        logger=logger,
    )
