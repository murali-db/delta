#!/usr/bin/env python3
"""
Integration tests for SparkShell with custom Delta builds.

These tests verify the Delta build integration without actually building Delta
(which would take too long). They test the integration logic and parameter passing.

Requirements:
- Python 3.7+

Run with:
    python3 test_delta_integration.py
"""

import sys
import os
import tempfile
from pathlib import Path

# Check Python version
if sys.version_info < (3, 7):
    print(f"✗ Python 3.7+ required (current: {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)

# Add sparkshell directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_shell import SparkShell, OpConfig


def test_constructor_with_delta_params():
    """Test that SparkShell constructor accepts Delta parameters."""
    try:
        # Create with Delta parameters (but don't start)
        shell = SparkShell(
            source=".",
            delta_repo_url="https://github.com/delta-io/delta",
            delta_branch="master",
            delta_version="3.4.0-SNAPSHOT",
            op_config=OpConfig(auto_start=False)
        )

        assert shell.delta_repo_url == "https://github.com/delta-io/delta"
        assert shell.delta_branch == "master"
        assert shell.delta_version == "3.4.0-SNAPSHOT"
        assert shell.delta_builder is not None, "Delta builder should be initialized"

        print("✓ Constructor with Delta parameters")
        return True
    except Exception as e:
        print(f"✗ Constructor test failed: {e}")
        return False


def test_constructor_with_local_delta():
    """Test constructor with local Delta path."""
    try:
        shell = SparkShell(
            source=".",
            use_local_delta="/tmp/delta",
            op_config=OpConfig(auto_start=False)
        )

        assert shell.use_local_delta == "/tmp/delta"
        assert shell.delta_builder is not None
        assert shell.delta_repo_url is None

        print("✓ Constructor with local Delta")
        return True
    except Exception as e:
        print(f"✗ Local Delta test failed: {e}")
        return False


def test_constructor_without_delta():
    """Test that constructor works without Delta parameters (default behavior)."""
    try:
        shell = SparkShell(
            source=".",
            op_config=OpConfig(auto_start=False)
        )

        assert shell.delta_builder is None, "Delta builder should not be initialized"
        assert shell.delta_repo_url is None
        assert shell.use_local_delta is None

        print("✓ Constructor without Delta (default)")
        return True
    except Exception as e:
        print(f"✗ Default constructor test failed: {e}")
        return False


def test_version_detection():
    """Test Delta version detection from version.sbt."""
    # Create a mock version.sbt file
    with tempfile.TemporaryDirectory() as temp_dir:
        version_file = Path(temp_dir) / "version.sbt"
        version_file.write_text('ThisBuild / version := "3.4.0-TEST-SNAPSHOT"\n')

        shell = SparkShell(
            source=".",
            op_config=OpConfig(auto_start=False, verbose=False)
        )

        detected_version = shell._detect_delta_version(Path(temp_dir))
        assert detected_version == "3.4.0-TEST-SNAPSHOT", \
            f"Expected '3.4.0-TEST-SNAPSHOT', got '{detected_version}'"

        print("✓ Version detection from version.sbt")
        return True


def test_delta_builder_initialization():
    """Test that Delta builder is only created when needed."""
    # With Delta repo URL - should create builder
    shell1 = SparkShell(
        source=".",
        delta_repo_url="https://github.com/delta-io/delta",
        op_config=OpConfig(auto_start=False)
    )
    assert shell1.delta_builder is not None

    # With local Delta path - should create builder
    shell2 = SparkShell(
        source=".",
        use_local_delta="/tmp/delta",
        op_config=OpConfig(auto_start=False)
    )
    assert shell2.delta_builder is not None

    # Without Delta params - should NOT create builder
    shell3 = SparkShell(
        source=".",
        op_config=OpConfig(auto_start=False)
    )
    assert shell3.delta_builder is None

    print("✓ Delta builder initialization logic")
    return True


if __name__ == "__main__":
    print("SparkShell Delta Integration Tests")
    print("=" * 50)

    tests = [
        ("Constructor with Delta parameters", test_constructor_with_delta_params),
        ("Constructor with local Delta", test_constructor_with_local_delta),
        ("Constructor without Delta", test_constructor_without_delta),
        ("Version detection", test_version_detection),
        ("Delta builder initialization", test_delta_builder_initialization),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            results.append(test_func())
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append(False)

    print("\n" + "=" * 50)
    passed = sum(results)
    total = len(results)

    if all(results):
        print(f"✓ All {total} tests passed!")
        sys.exit(0)
    else:
        print(f"✗ {total - passed}/{total} tests failed")
        sys.exit(1)
