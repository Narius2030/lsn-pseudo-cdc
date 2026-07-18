"""Artifact writers for S3 and local filesystem outputs."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import AppConfig, S3Config
from .errors import ConfigurationError, S3WriteError
from .retry import retry_with_backoff


def create_s3_client(config: S3Config) -> Any:
    session = boto3.session.Session(
        profile_name=config.profile_name,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        aws_session_token=config.session_token,
        region_name=config.region_name,
    )
    return session.client("s3", region_name=config.region_name, endpoint_url=config.endpoint_url)


class ArtifactWriter:
    """Common interface for output destinations."""

    def upload_file(self, local_path: Path, key: str) -> None:
        raise NotImplementedError

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_keys(self, keys: list[str]) -> None:
        raise NotImplementedError

    def probe_access(self) -> None:
        raise NotImplementedError

    def describe_destination(self, key: str) -> str:
        raise NotImplementedError


class S3ArtifactWriter(ArtifactWriter):
    def __init__(
        self,
        *,
        s3_client: Any,
        config: S3Config,
        retry_attempts: int,
        retry_backoff_seconds: float,
        retry_max_delay_seconds: float,
        logger: Any,
    ) -> None:
        self.s3_client = s3_client
        self.config = config
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.retry_max_delay_seconds = retry_max_delay_seconds
        self.logger = logger

    def upload_file(self, local_path: Path, key: str) -> None:
        extra_args: dict[str, str] = {
            "ContentType": "application/json",
        }
        if key.lower().endswith(".gz"):
            extra_args["ContentEncoding"] = "gzip"

        if self.config.server_side_encryption:
            extra_args["ServerSideEncryption"] = self.config.server_side_encryption
        if self.config.ssekms_key_id:
            extra_args["SSEKMSKeyId"] = self.config.ssekms_key_id

        def _upload() -> None:
            self.s3_client.upload_file(str(local_path), self.config.bucket, key, ExtraArgs=extra_args)

        try:
            retry_with_backoff(
                "upload CDC file to S3",
                _upload,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(BotoCoreError, ClientError, OSError),
            )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise S3WriteError(f"Failed to upload {local_path} to {self.describe_destination(key)}: {exc}") from exc

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")

        def _put() -> None:
            self.s3_client.put_object(
                Bucket=self.config.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                **self._encryption_args(),
            )

        try:
            retry_with_backoff(
                "write JSON artifact to S3",
                _put,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(BotoCoreError, ClientError, OSError),
            )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise S3WriteError(f"Failed to write {self.describe_destination(key)}: {exc}") from exc

    def delete_keys(self, keys: list[str]) -> None:
        if not keys:
            return

        def _delete_batch(batch: list[str]) -> None:
            self.s3_client.delete_objects(
                Bucket=self.config.bucket,
                Delete={"Objects": [{"Key": item} for item in batch], "Quiet": True},
            )

        try:
            for offset in range(0, len(keys), 1000):
                batch = keys[offset : offset + 1000]
                retry_with_backoff(
                    "delete failed run artifacts from S3",
                    lambda batch=batch: _delete_batch(batch),
                    logger=self.logger,
                    attempts=self.retry_attempts,
                    base_delay_seconds=self.retry_backoff_seconds,
                    max_delay_seconds=self.retry_max_delay_seconds,
                    retry_on=(BotoCoreError, ClientError, OSError),
                )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise S3WriteError(f"Failed to delete S3 objects after rollback: {exc}") from exc

    def probe_access(self) -> None:
        def _probe() -> None:
            self.s3_client.head_bucket(Bucket=self.config.bucket)

        try:
            retry_with_backoff(
                "probe S3 bucket access",
                _probe,
                logger=self.logger,
                attempts=self.retry_attempts,
                base_delay_seconds=self.retry_backoff_seconds,
                max_delay_seconds=self.retry_max_delay_seconds,
                retry_on=(BotoCoreError, ClientError, OSError),
            )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise S3WriteError(
                f"Unable to access destination bucket s3://{self.config.bucket}. "
                "Check bucket name, region, endpoint URL, and AWS credentials."
            ) from exc

    def describe_destination(self, key: str) -> str:
        return f"s3://{self.config.bucket}/{key}"

    def _encryption_args(self) -> dict[str, str]:
        payload: dict[str, str] = {}
        if self.config.server_side_encryption:
            payload["ServerSideEncryption"] = self.config.server_side_encryption
        if self.config.ssekms_key_id:
            payload["SSEKMSKeyId"] = self.config.ssekms_key_id
        return payload


class LocalArtifactWriter(ArtifactWriter):
    def __init__(self, *, root_dir: str, logger: Any) -> None:
        self.root_dir = Path(root_dir)
        self.logger = logger

    def upload_file(self, local_path: Path, key: str) -> None:
        destination = self.root_dir / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, destination)

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        destination = self.root_dir / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def delete_keys(self, keys: list[str]) -> None:
        for key in keys:
            target = self.root_dir / key
            if target.exists():
                target.unlink()

    def probe_access(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def describe_destination(self, key: str) -> str:
        return str((self.root_dir / key).resolve())


def build_artifact_writer(
    config: AppConfig,
    *,
    s3_client: Any | None,
    logger: Any,
) -> ArtifactWriter:
    if config.runtime.output_mode == "local":
        return LocalArtifactWriter(root_dir=config.runtime.local_output_dir, logger=logger)
    if s3_client is None:
        raise ConfigurationError("S3 output mode requires an initialized S3 client")
    return S3ArtifactWriter(
        s3_client=s3_client,
        config=config.s3,
        retry_attempts=config.runtime.max_retries,
        retry_backoff_seconds=config.runtime.retry_backoff_seconds,
        retry_max_delay_seconds=config.runtime.retry_max_delay_seconds,
        logger=logger,
    )
