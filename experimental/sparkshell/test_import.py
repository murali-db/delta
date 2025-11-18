#!/usr/bin/env python3
"""
Basic smoke test for SparkShell import.
This test verifies that the SparkShell module can be imported successfully.

Requirements:
- Python 3.7+ (uses dataclasses)
"""

import sys
import os

# Check Python version
if sys.version_info < (3, 7):
    print(f"✗ Python 3.7+ required (current: {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)

# Add sparkshell directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_import():
    """Test that spark_shell module can be imported."""
    try:
        import spark_shell
        print("✓ spark_shell module imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Failed to import spark_shell: {e}")
        return False

def test_classes_exist():
    """Test that expected classes exist in the module."""
    import spark_shell

    expected_classes = [
        'SparkShell',
        'UCConfig',
        'OpConfig',
        'SparkConfig'
    ]

    all_pass = True
    for class_name in expected_classes:
        if hasattr(spark_shell, class_name):
            print(f"✓ {class_name} class exists")
        else:
            print(f"✗ {class_name} class not found")
            all_pass = False

    return all_pass

def test_instantiation():
    """Test that SparkShell can be instantiated (without starting)."""
    import spark_shell

    try:
        # Just test that we can create the object
        # Don't actually start it as that would require building
        shell = spark_shell.SparkShell(source=".")
        print("✓ SparkShell can be instantiated")
        return True
    except Exception as e:
        print(f"✗ Failed to instantiate SparkShell: {e}")
        return False

if __name__ == "__main__":
    print("SparkShell Import Tests")
    print("=" * 50)

    tests = [
        ("Import module", test_import),
        ("Check classes", test_classes_exist),
        ("Instantiate SparkShell", test_instantiation),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        results.append(test_func())

    print("\n" + "=" * 50)
    if all(results):
        print("✓ All tests passed!")
        sys.exit(0)
    else:
        print("✗ Some tests failed")
        sys.exit(1)
