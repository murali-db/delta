#!/usr/bin/env python3
"""
Comprehensive test runner for SparkShell.

Runs all unit and integration tests and provides a summary.

Requirements:
- Python 3.7+

Run with:
    python3 run_all_tests.py
"""

import sys
import os
import subprocess
from pathlib import Path

# Check Python version
if sys.version_info < (3, 7):
    print(f"✗ Python 3.7+ required (current: {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
VENV_PYTHON = SCRIPT_DIR / "venv" / "bin" / "python"

# Use venv python if available, otherwise system python
PYTHON = VENV_PYTHON if VENV_PYTHON.exists() else sys.executable

TEST_FILES = [
    ("Import Tests", "test_import.py"),
    ("Delta Builder Unit Tests", "test_delta_builder.py"),
    ("Delta Integration Tests", "test_delta_integration.py"),
]


def run_test(test_name: str, test_file: str) -> bool:
    """
    Run a test file and return success status.

    Args:
        test_name: Display name for the test
        test_file: Test file to run

    Returns:
        True if test passed, False otherwise
    """
    test_path = SCRIPT_DIR / test_file

    if not test_path.exists():
        print(f"✗ {test_name}: Test file not found: {test_file}")
        return False

    print(f"\n{'=' * 60}")
    print(f"Running: {test_name}")
    print(f"{'=' * 60}")

    try:
        result = subprocess.run(
            [str(PYTHON), str(test_path)],
            cwd=SCRIPT_DIR,
            capture_output=False,  # Show output directly
            timeout=30
        )

        if result.returncode == 0:
            print(f"✓ {test_name} passed")
            return True
        else:
            print(f"✗ {test_name} failed with exit code {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        print(f"✗ {test_name} timed out after 30 seconds")
        return False
    except Exception as e:
        print(f"✗ {test_name} failed with exception: {e}")
        return False


def main():
    """Run all tests and report results."""
    print("=" * 60)
    print("SparkShell Comprehensive Test Suite")
    print("=" * 60)
    print(f"Python: {PYTHON}")
    print(f"Working directory: {SCRIPT_DIR}")

    results = []
    for test_name, test_file in TEST_FILES:
        results.append((test_name, run_test(test_name, test_file)))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")

    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)

    print(f"\n{passed_count}/{total_count} test suites passed")

    if all(p for _, p in results):
        print("\n✓ All tests passed!")
        return 0
    else:
        print(f"\n✗ {total_count - passed_count} test suite(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
