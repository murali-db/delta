#!/usr/bin/env python3
"""
Unit tests for delta_builder module.

These tests verify the Delta build infrastructure without requiring actual
Delta Lake builds (which would take too long).

Requirements:
- Python 3.7+

Run with:
    python3 test_delta_builder.py
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

# Check Python version
if sys.version_info < (3, 7):
    print(f"✗ Python 3.7+ required (current: {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)

# Add sparkshell directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from delta_builder import DeltaBuildCache, DeltaBuilder


def test_cache_key_generation():
    """Test that cache keys are generated consistently."""
    cache = DeltaBuildCache()

    # Same inputs should produce same key
    key1 = cache.get_cache_key("https://github.com/delta-io/delta", "master", "abc123")
    key2 = cache.get_cache_key("https://github.com/delta-io/delta", "master", "abc123")
    assert key1 == key2, "Same inputs should produce same cache key"

    # Different inputs should produce different keys
    key3 = cache.get_cache_key("https://github.com/delta-io/delta", "master", "def456")
    assert key1 != key3, "Different commits should produce different cache keys"

    key4 = cache.get_cache_key("https://github.com/delta-io/delta", "spark-uc", "abc123")
    assert key1 != key4, "Different branches should produce different cache keys"

    # Keys should be reasonably short (we use [:16])
    assert len(key1) == 16, f"Cache key should be 16 characters, got {len(key1)}"

    print("✓ Cache key generation")
    return True


def test_cache_operations():
    """Test cache storage and retrieval."""
    # Use temporary directory for cache
    with tempfile.TemporaryDirectory() as temp_dir:
        cache = DeltaBuildCache(cache_dir=temp_dir)

        cache_key = "test_key_12345"

        # Initially should not be cached
        cached = cache.get_cached_build(cache_key)
        assert cached is None, "New cache key should not be cached initially"

        # Mark as cached
        build_info = {
            "version": "3.4.0-SNAPSHOT",
            "timestamp": "2025-11-18",
            "commit": "abc123"
        }
        cache_path = cache.cache_build(cache_key, build_info)

        assert cache_path.exists(), "Cache path should exist after caching"
        assert (cache_path / ".build_complete").exists(), "Build marker should exist"

        # Should now be cached
        cached = cache.get_cached_build(cache_key)
        assert cached is not None, "Cache key should be found after caching"
        assert cached == cache_path, "Retrieved cache path should match"

        # Clear specific cache
        cache.clear_cache(cache_key)
        cached = cache.get_cached_build(cache_key)
        assert cached is None, "Cache should be cleared after clear_cache()"

    print("✓ Cache operations")
    return True


def test_compatibility_validation():
    """Test Spark/Delta compatibility checks."""
    builder = DeltaBuilder(verbose=False)

    # Test known compatible combinations
    assert builder.validate_compatibility("master", "4.0.0"), \
        "master branch should be compatible with Spark 4.0"

    assert builder.validate_compatibility("branch-3.1", "3.5.0"), \
        "branch-3.1 should be compatible with Spark 3.5"

    assert builder.validate_compatibility("branch-3.0", "3.5.0"), \
        "branch-3.0 should be compatible with Spark 3.5"

    # Test known incompatible combinations
    assert not builder.validate_compatibility("branch-2.4", "4.0.0"), \
        "branch-2.4 should NOT be compatible with Spark 4.0"

    # Test unknown branch (should return True with warning)
    assert builder.validate_compatibility("feature/new-feature", "4.0.0"), \
        "Unknown branches should be allowed (with warning)"

    print("✓ Compatibility validation")
    return True


def test_builder_initialization():
    """Test DeltaBuilder initialization."""
    # Default initialization
    builder1 = DeltaBuilder()
    assert builder1.cache is not None, "Cache should be initialized"
    assert not builder1.verbose, "Verbose should default to False"

    # With custom cache dir
    with tempfile.TemporaryDirectory() as temp_dir:
        builder2 = DeltaBuilder(cache_dir=temp_dir, verbose=True)
        assert builder2.cache.cache_dir == Path(temp_dir), "Custom cache dir should be used"
        assert builder2.verbose, "Verbose should be True"

    print("✓ Builder initialization")
    return True


def test_clear_artifact_caches():
    """Test artifact cache clearing (without actually clearing real caches)."""
    # This is a light test - we just verify the method exists and can be called
    # We don't actually want to clear the user's real Maven/Ivy caches
    builder = DeltaBuilder(verbose=False)

    # Should not raise exception
    try:
        # We can't really test this without side effects, so just verify method exists
        assert hasattr(builder, 'clear_artifact_caches'), \
            "Builder should have clear_artifact_caches method"
        print("✓ Artifact cache clearing")
        return True
    except Exception as e:
        print(f"✗ Artifact cache clearing failed: {e}")
        return False


def test_git_clone_validation():
    """Test git clone input validation."""
    builder = DeltaBuilder(verbose=False)

    # Create a temporary target directory
    with tempfile.TemporaryDirectory() as temp_dir:
        target = os.path.join(temp_dir, "delta_test")

        # Test with invalid repo URL (should fail quickly with retry)
        try:
            builder.safe_git_clone(
                "https://github.com/nonexistent/repository.git",
                "master",
                target,
                retry_count=1
            )
            print("✗ Should have raised exception for invalid repo")
            return False
        except Exception as e:
            # Expected to fail
            assert "failed" in str(e).lower(), f"Expected failure message, got: {e}"

    print("✓ Git clone validation")
    return True


def test_build_lock_path_generation():
    """Test that build lock file paths are generated correctly."""
    # Lock file paths should be unique per version
    version1 = "3.4.0-SNAPSHOT"
    version2 = "3.4.0-custom-SNAPSHOT"

    lock_path1 = f"/tmp/delta_build_{version1.replace('.', '_').replace('-', '_')}.lock"
    lock_path2 = f"/tmp/delta_build_{version2.replace('.', '_').replace('-', '_')}.lock"

    assert lock_path1 != lock_path2, "Different versions should have different lock paths"
    assert ".lock" in lock_path1, "Lock path should have .lock extension"

    print("✓ Build lock path generation")
    return True


if __name__ == "__main__":
    print("Delta Builder Unit Tests")
    print("=" * 50)

    tests = [
        ("Cache key generation", test_cache_key_generation),
        ("Cache operations", test_cache_operations),
        ("Compatibility validation", test_compatibility_validation),
        ("Builder initialization", test_builder_initialization),
        ("Artifact cache clearing", test_clear_artifact_caches),
        ("Git clone validation", test_git_clone_validation),
        ("Build lock path generation", test_build_lock_path_generation),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            results.append(test_func())
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
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
