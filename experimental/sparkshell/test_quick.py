#!/usr/bin/env python3
"""
Quick test script for SparkShell with custom Delta builds.

This script auto-detects the Delta repository path (3 levels up from this script)
and provides a simple way to test SparkShell with your local Delta changes.

Usage:
    # Use local Delta (auto-detected)
    ./test_quick.py

    # Use Delta from GitHub
    ./test_quick.py --github https://github.com/tdas/delta --branch spark-uc

    # Specify local Delta path explicitly
    ./test_quick.py --local /path/to/delta

    # With Unity Catalog
    ./test_quick.py --uc-uri http://localhost:8081 --uc-token mytoken
"""

import sys
import os
import argparse
from pathlib import Path

# Add current directory to path
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))

from spark_shell import SparkShell, UCConfig, OpConfig


def auto_detect_delta_path():
    """
    Auto-detect Delta repository path.

    Assumes structure: delta_repo/experimental/sparkshell/test_quick.py
    So Delta root is 2 levels up.
    """
    delta_path = SCRIPT_DIR.parent.parent

    # Verify it looks like a Delta repo
    if (delta_path / "build.sbt").exists() and (delta_path / "version.sbt").exists():
        return str(delta_path)

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Quick test for SparkShell with custom Delta builds"
    )

    # Delta source options
    delta_group = parser.add_mutually_exclusive_group()
    delta_group.add_argument(
        "--local",
        help="Path to local Delta repository (default: auto-detect)"
    )
    delta_group.add_argument(
        "--github",
        help="GitHub URL to Delta repository (e.g., https://github.com/delta-io/delta)"
    )

    parser.add_argument(
        "--branch",
        default="master",
        help="Delta branch to checkout (only with --github, default: master)"
    )

    parser.add_argument(
        "--version",
        help="Delta version to build (default: auto-detect from version.sbt)"
    )

    # Unity Catalog options
    parser.add_argument("--uc-uri", help="Unity Catalog URI (e.g., http://localhost:8081)")
    parser.add_argument("--uc-token", help="Unity Catalog token")
    parser.add_argument("--uc-catalog", default="unity", help="Unity Catalog name (default: unity)")
    parser.add_argument("--uc-schema", help="Unity Catalog default schema")

    # Other options
    parser.add_argument("--port", type=int, default=8080, help="SparkShell server port (default: 8080)")
    parser.add_argument("--quiet", action="store_true", help="Reduce output verbosity")
    parser.add_argument("--sql", help="SQL query to execute (default: SELECT 1 as id)")

    args = parser.parse_args()

    # Determine Delta source
    if args.github:
        delta_repo_url = args.github
        delta_branch = args.branch
        use_local_delta = None
        source_desc = f"GitHub: {delta_repo_url} ({delta_branch})"
    elif args.local:
        delta_repo_url = None
        delta_branch = None
        use_local_delta = args.local
        source_desc = f"Local: {use_local_delta}"
    else:
        # Auto-detect local Delta
        auto_path = auto_detect_delta_path()
        if not auto_path:
            print("✗ Could not auto-detect Delta repository path")
            print("  Please use --local or --github to specify Delta source")
            return 1

        delta_repo_url = None
        delta_branch = None
        use_local_delta = auto_path
        source_desc = f"Local (auto-detected): {use_local_delta}"

    # Setup Unity Catalog if provided
    uc_config = None
    if args.uc_uri and args.uc_token:
        uc_config = UCConfig(
            uri=args.uc_uri,
            token=args.uc_token,
            catalog=args.uc_catalog,
            schema=args.uc_schema
        )
        print(f"Unity Catalog: {args.uc_uri} (catalog: {args.uc_catalog})")

    # Operational config
    op_config = OpConfig(
        verbose=not args.quiet,
        startup_timeout=120,
        build_timeout=900  # 15 minutes for Delta build
    )

    # Print configuration
    print("=" * 60)
    print("SparkShell Quick Test")
    print("=" * 60)
    print(f"Delta source: {source_desc}")
    print(f"Port: {args.port}")
    print(f"Unity Catalog: {'Enabled' if uc_config else 'Disabled'}")
    print("=" * 60)
    print()

    # Create SparkShell
    print("Creating SparkShell...")
    shell = SparkShell(
        source=str(SCRIPT_DIR),
        port=args.port,
        delta_repo_url=delta_repo_url,
        delta_branch=delta_branch,
        delta_version=args.version,
        use_local_delta=use_local_delta,
        uc_config=uc_config,
        op_config=op_config
    )

    try:
        # Start server
        print("\nStarting SparkShell server...")
        print("(First run: builds Delta ~5-10 min, then SparkShell ~2 min)")
        print("(Subsequent runs: uses cache ~30 sec)\n")

        shell.start()

        print("\n✓ SparkShell is ready!")
        print(f"  Server: {shell.base_url}")
        print(f"  Delta version: {shell.delta_version or '4.0.0 (default)'}")

        # Execute test SQL
        test_sql = args.sql or "SELECT 1 as id, 'Hello from custom Delta!' as message"
        print(f"\nExecuting SQL: {test_sql}")

        result = shell.execute_sql(test_sql)
        print(f"\nResult: {result}")

        print("\n✓ Success! SparkShell is working with your custom Delta build!")

        # Keep server running for interactive use
        print("\nServer is running. Press Ctrl+C to stop...")
        print(f"You can execute SQL by calling: shell.execute_sql('YOUR SQL HERE')")

        # Wait for user interrupt
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nShutting down...")

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Always shutdown
        if shell.process:
            shell.shutdown()


if __name__ == "__main__":
    sys.exit(main())
