import os
import sys
import json
from pathlib import Path

# Add src to path so we can import sqlserver_cdc_s3 without installing the package
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "src"))

from sqlserver_cdc_s3.pipeline import run_pipeline

def test_local_mode():
    """
    Test script for running the CDC pipeline in local mode.
    Reads from config/config.local.json and writes to local-output/.
    """
    config_path = current_dir / "config" / "config.local.json"
    example_path = current_dir / "config" / "config.local.example.json"

    # Ensure local config exists
    if not config_path.exists():
        if example_path.exists():
            print(f"Config file not found: {config_path}")
            print(f"Creating from example: {example_path}")
            config_path.write_text(example_path.read_text())
            print("Action Required: Please edit config/config.local.json with your SQL Server details.")
        else:
            print(f"Error: Neither {config_path} nor {example_path} exist.")
        return

    # Optional: Verify environment variables for the example config
    # These are only needed if your config uses the ${VAR} syntax
    required_env = ["SQLSERVER_HOST", "SQLSERVER_DATABASE", "SQLSERVER_USER", "SQLSERVER_PASSWORD"]
    missing = [env for env in required_env if env not in os.environ]
    
    if missing:
        print("--- Environment Variables Missing ---")
        print(f"The following variables are required by your config: {', '.join(missing)}")
        print("You can set them in your shell before running this script.")
        print("Example: export SQLSERVER_HOST=localhost")
        print("-------------------------------------")

    print(f"Starting CDC Extraction...")
    print(f"Config: {config_path}")
    print(f"Output Mode: local (writing to local-output/)")
    print(f"Bookmark Commit: disabled (dry-run mode)")
    print("-" * 40)

    try:
        # We override some settings here to ensure it's a safe local test
        result = run_pipeline(
            str(config_path),
            preflight_only=False,
            output_mode_override=None,
            commit_bookmarks_override=True
        )
        
        print("\n--- Pipeline Execution Summary ---")
        print(json.dumps(result, indent=2))
        print("-----------------------------------")
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed during execution:")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    test_local_mode()
