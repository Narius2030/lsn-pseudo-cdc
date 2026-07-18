"""CLI entrypoint."""

from __future__ import annotations

import argparse
import json
import sys

from .pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Export SQL Server CDC changes to S3 in Debezium-style JSON format.")
    parser.add_argument("--config", required=True, help="Path to the JSON configuration file.")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate SQL Server, CDC health, and destination access without extracting data.",
    )
    parser.add_argument(
        "--output-mode",
        choices=["s3", "local"],
        help="Override runtime.output_mode from the config file.",
    )
    parser.add_argument(
        "--no-commit-bookmarks",
        action="store_true",
        help="Do not save bookmark state after a successful run.",
    )
    args = parser.parse_args()

    try:
        result = run_pipeline(
            args.config,
            preflight_only=args.preflight_only,
            commit_bookmarks_override=False if args.no_commit_bookmarks else None,
            output_mode_override=args.output_mode,
        )
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
