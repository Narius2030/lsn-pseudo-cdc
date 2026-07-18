"""Discovery and sequential execution of independent connector configurations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import AppConfig, load_config
from .errors import ConfigurationError


@dataclass(frozen=True)
class ConnectorDefinition:
    """A connector configuration together with its stable, file-derived identifier."""

    connector_id: str
    path: Path
    config: AppConfig


def discover_connectors(config_dir: str | Path) -> list[ConnectorDefinition]:
    """Load and validate all top-level JSON connector configurations in a directory."""
    directory = Path(config_dir)
    if not directory.is_dir():
        raise ConfigurationError(f"Connector config directory not found: {directory}")

    paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    if not paths:
        raise ConfigurationError(f"No JSON connector configurations found in: {directory}")

    connectors = [ConnectorDefinition(connector_id=path.stem, path=path, config=load_config(path)) for path in paths]
    _validate_connector_isolation(connectors)
    return connectors


def run_connectors(
    config_dir: str | Path,
    *,
    continue_on_error: bool = False,
    preflight_only: bool = False,
    commit_bookmarks_override: bool | None = None,
    output_mode_override: str | None = None,
    pipeline_runner: Callable[..., dict[str, object]] | None = None,
) -> dict[str, object]:
    """Run connector files in lexical order and return an aggregate result."""
    connectors = discover_connectors(config_dir)
    if pipeline_runner is None:
        # Keep configuration discovery usable in environments without the ODBC runtime.
        from .pipeline import run_pipeline

        pipeline_runner = run_pipeline
    results: list[dict[str, object]] = []
    failed = False

    for connector in connectors:
        try:
            result = pipeline_runner(
                str(connector.path),
                preflight_only=preflight_only,
                commit_bookmarks_override=commit_bookmarks_override,
                output_mode_override=output_mode_override,
            )
            results.append({"connector_id": connector.connector_id, "status": "success", "result": result})
        except Exception as exc:
            failed = True
            results.append(
                {
                    "connector_id": connector.connector_id,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            if not continue_on_error:
                break

    return {
        "status": "failed" if failed else "success",
        "connectors_discovered": len(connectors),
        "connectors_executed": len(results),
        "results": results,
    }


def _validate_connector_isolation(connectors: list[ConnectorDefinition]) -> None:
    _ensure_unique(connectors, "bookmark state store", _state_store_identity)
    _ensure_unique(connectors, "output destination", _output_identity)


def _ensure_unique(
    connectors: list[ConnectorDefinition],
    resource_label: str,
    identity_for: Callable[[AppConfig], str],
) -> None:
    owners: dict[str, str] = {}
    for connector in connectors:
        identity = identity_for(connector.config)
        existing = owners.get(identity)
        if existing is not None:
            raise ConfigurationError(
                f"Connectors {existing!r} and {connector.connector_id!r} share {resource_label}: {identity}"
            )
        owners[identity] = connector.connector_id


def _state_store_identity(config: AppConfig) -> str:
    if config.state_store.type == "s3":
        return f"s3://{config.state_store.bucket}/{config.state_store.key}"
    return f"file://{Path(config.state_store.path or '').resolve()}"


def _output_identity(config: AppConfig) -> str:
    if config.runtime.output_mode == "s3":
        return f"s3://{config.s3.bucket}/{config.s3.data_prefix}"
    return f"file://{Path(config.runtime.local_output_dir).resolve()}"
