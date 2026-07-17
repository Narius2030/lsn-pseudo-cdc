"""Command-line entrypoint for running all connector configs in a directory."""

from __future__ import annotations

import argparse
import json
import sys

from .connector_runner import run_connectors


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SQL Server CDC connector configurations from a directory.")
    parser.add_argument("--config-dir", required=True, help="Directory containing one JSON file per connector.")
    parser.add_argument("--continue-on-error", action="store_true", help="Run remaining connectors after a failure.")
    parser.add_argument("--preflight-only", action="store_true", help="Validate each connector without extracting data.")
    parser.add_argument("--output-mode", choices=["s3", "local"], help="Override runtime.output_mode for every connector.")
    parser.add_argument("--no-commit-bookmarks", action="store_true", help="Do not persist bookmarks for any connector.")
    args = parser.parse_args()

    try:
        result = run_connectors(
            args.config_dir,
            continue_on_error=args.continue_on_error,
            preflight_only=args.preflight_only,
            commit_bookmarks_override=False if args.no_commit_bookmarks else None,
            output_mode_override=args.output_mode,
        )
    except Exception as exc:
        print(f"Connector runner failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
