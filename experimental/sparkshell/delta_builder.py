#!/usr/bin/env python3
"""
Delta Build Infrastructure for SparkShell

This module provides functionality to clone, build, and publish Delta Lake from
any GitHub repository and branch. It includes comprehensive safeguards for:
- Concurrent build protection via file locking
- Build caching for faster repeated builds
- Git operations with retry logic and error handling
- Compatibility validation between Delta, Spark, and Scala versions
- Artifact cache management

Usage:
    builder = DeltaBuilder()

    # Build Delta from GitHub
    commit_hash = builder.safe_git_clone(
        "https://github.com/delta-io/delta",
        "master",
        "/tmp/delta_build"
    )

    # Build and publish to ~/.m2
    builder.build_and_publish("/tmp/delta_build", "3.4.0-SNAPSHOT")
"""

import os
import sys
import time
import shutil
import hashlib
import subprocess
import fcntl
import tempfile
from typing import Optional, Tuple
from pathlib import Path


class DeltaBuildCache:
    """
    Manages caching of built Delta artifacts to avoid redundant builds.

    Cache keys are generated from (repo_url, branch, commit_hash) tuples to
    uniquely identify each Delta build. Cached builds are stored in a local
    directory and reused when the same version is requested again.
    """

    def __init__(self, cache_dir: str = None):
        """
        Initialize the build cache.

        Args:
            cache_dir: Directory to store cached builds. Defaults to
                      ~/.delta_build_cache
        """
        if cache_dir is None:
            cache_dir = os.path.expanduser("~/.delta_build_cache")

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_key(self, repo_url: str, branch: str, commit: str) -> str:
        """
        Generate a unique cache key for a Delta build.

        Args:
            repo_url: Git repository URL
            branch: Branch name
            commit: Commit hash

        Returns:
            SHA256 hash string as cache key
        """
        key_string = f"{repo_url}_{branch}_{commit}"
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def get_cached_build(self, cache_key: str) -> Optional[Path]:
        """
        Check if a build is cached and return its path.

        Args:
            cache_key: Cache key from get_cache_key()

        Returns:
            Path to cached build directory, or None if not cached
        """
        cache_path = self.cache_dir / cache_key

        if cache_path.exists() and (cache_path / ".build_complete").exists():
            return cache_path

        return None

    def cache_build(self, cache_key: str, build_info: dict) -> Path:
        """
        Mark a build as cached.

        Args:
            cache_key: Cache key for this build
            build_info: Dict with build information (version, timestamp, etc.)

        Returns:
            Path to cache directory
        """
        cache_path = self.cache_dir / cache_key
        cache_path.mkdir(parents=True, exist_ok=True)

        # Write build info
        info_file = cache_path / ".build_complete"
        with open(info_file, 'w') as f:
            for key, value in build_info.items():
                f.write(f"{key}={value}\n")

        return cache_path

    def clear_cache(self, cache_key: str = None):
        """
        Clear cached builds.

        Args:
            cache_key: Specific cache key to clear, or None to clear all
        """
        if cache_key:
            cache_path = self.cache_dir / cache_key
            if cache_path.exists():
                shutil.rmtree(cache_path)
        else:
            # Clear entire cache
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)


class DeltaBuilder:
    """
    Handles cloning, building, and publishing Delta Lake from source.

    This class provides comprehensive build infrastructure with safeguards for:
    - Concurrent build protection
    - Git operations with retry logic
    - Scala/Spark version compatibility validation
    - Artifact cache management
    """

    # Compatibility matrix: Delta branch -> compatible Spark versions
    COMPATIBILITY_MATRIX = {
        "branch-2.4": ["3.2", "3.3", "3.4"],
        "branch-3.0": ["3.4", "3.5"],
        "branch-3.1": ["3.5", "4.0"],
        "branch-3.2": ["3.5", "4.0"],
        "master": ["4.0"],
    }

    def __init__(self, cache_dir: str = None, verbose: bool = False):
        """
        Initialize Delta builder.

        Args:
            cache_dir: Directory for build cache
            verbose: Enable verbose output
        """
        self.cache = DeltaBuildCache(cache_dir)
        self.verbose = verbose
        self.build_locks = {}  # Track file locks

    def _log(self, message: str):
        """Log message if verbose mode enabled."""
        if self.verbose:
            print(f"[DeltaBuilder] {message}")

    def _run_cmd(self, cmd: list, cwd: str = None, timeout: int = 600) -> Tuple[int, str, str]:
        """
        Run shell command with timeout.

        Args:
            cmd: Command as list of strings
            cwd: Working directory
            timeout: Timeout in seconds

        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        self._log(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            raise Exception(f"Command timed out after {timeout}s: {' '.join(cmd)}")

    def validate_compatibility(self, delta_branch: str, spark_version: str,
                             scala_version: str = "2.13") -> bool:
        """
        Validate compatibility between Delta branch and Spark version.

        Args:
            delta_branch: Delta branch name (can be None for local repos)
            spark_version: Spark version (e.g., "4.0")
            scala_version: Scala version (e.g., "2.13")

        Returns:
            True if compatible, False otherwise
        """
        # If no branch specified (local repo), assume compatible with warning
        if not delta_branch:
            self._log("⚠ No branch specified (local Delta), skipping compatibility check")
            return True

        # Extract major.minor from spark_version
        spark_major_minor = ".".join(spark_version.split(".")[:2])

        # Check if branch is in compatibility matrix
        for pattern, compatible_versions in self.COMPATIBILITY_MATRIX.items():
            if pattern in delta_branch or delta_branch.startswith(pattern):
                if spark_major_minor in compatible_versions:
                    self._log(f"✓ Delta {delta_branch} compatible with Spark {spark_version}")
                    return True
                else:
                    self._log(f"✗ Delta {delta_branch} NOT compatible with Spark {spark_version}")
                    self._log(f"  Compatible Spark versions: {', '.join(compatible_versions)}")
                    return False

        # Unknown branch - allow but warn
        self._log(f"⚠ Unknown Delta branch '{delta_branch}', cannot verify compatibility")
        return True

    def safe_git_clone(self, repo_url: str, branch: str, target_dir: str,
                       retry_count: int = 3) -> str:
        """
        Safely clone Git repository with retry logic and cleanup.

        Args:
            repo_url: Git repository URL
            branch: Branch to checkout
            target_dir: Where to clone
            retry_count: Number of retry attempts

        Returns:
            Commit hash of checked out code

        Raises:
            Exception if clone/checkout fails after retries
        """
        target_path = Path(target_dir)

        # If directory exists, try to reuse it
        if target_path.exists():
            self._log(f"Directory {target_dir} exists, attempting to reuse...")

            try:
                # Verify it's a git repo
                returncode, _, _ = self._run_cmd(
                    ["git", "rev-parse", "--git-dir"],
                    cwd=target_dir,
                    timeout=10
                )

                if returncode == 0:
                    # Clean up any local changes
                    self._log("Cleaning existing repository...")
                    self._run_cmd(["git", "reset", "--hard"], cwd=target_dir, timeout=30)
                    self._run_cmd(["git", "clean", "-fdx"], cwd=target_dir, timeout=30)

                    # Fetch latest
                    self._log(f"Fetching latest from {repo_url}...")
                    self._run_cmd(["git", "fetch", "origin"], cwd=target_dir, timeout=300)

                    # Checkout branch
                    self._log(f"Checking out branch {branch}...")
                    returncode, _, stderr = self._run_cmd(
                        ["git", "checkout", branch],
                        cwd=target_dir,
                        timeout=30
                    )

                    if returncode == 0:
                        # Get commit hash
                        _, commit, _ = self._run_cmd(
                            ["git", "rev-parse", "HEAD"],
                            cwd=target_dir,
                            timeout=10
                        )
                        commit_hash = commit.strip()
                        self._log(f"✓ Reused existing repo at commit {commit_hash[:8]}")
                        return commit_hash
                    else:
                        self._log(f"⚠ Checkout failed: {stderr}")
                        # Fall through to fresh clone
                else:
                    self._log("Not a valid git repository, removing...")
                    shutil.rmtree(target_dir)
            except Exception as e:
                self._log(f"⚠ Failed to reuse existing directory: {e}")
                shutil.rmtree(target_dir, ignore_errors=True)

        # Fresh clone with retry
        for attempt in range(retry_count):
            try:
                self._log(f"Cloning {repo_url} (attempt {attempt + 1}/{retry_count})...")

                # Clone with depth=1 for speed
                returncode, _, stderr = self._run_cmd(
                    ["git", "clone", "--depth=1", "--branch", branch, repo_url, target_dir],
                    timeout=600
                )

                if returncode != 0:
                    raise Exception(f"Git clone failed: {stderr}")

                # Get commit hash
                _, commit, _ = self._run_cmd(
                    ["git", "rev-parse", "HEAD"],
                    cwd=target_dir,
                    timeout=10
                )
                commit_hash = commit.strip()

                self._log(f"✓ Successfully cloned at commit {commit_hash[:8]}")
                return commit_hash

            except Exception as e:
                self._log(f"✗ Clone attempt {attempt + 1} failed: {e}")

                # Clean up failed clone
                if target_path.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)

                if attempt < retry_count - 1:
                    time.sleep(5)  # Wait before retry
                else:
                    raise Exception(f"Git clone failed after {retry_count} attempts: {e}")

    def clear_artifact_caches(self):
        """Clear Maven and Ivy artifact caches for Delta."""
        self._log("Clearing Delta artifact caches...")

        caches_to_clear = [
            "~/.ivy2/cache/io.delta",
            "~/.ivy2/local/io.delta",
            "~/.m2/repository/io/delta"
        ]

        for cache_path in caches_to_clear:
            expanded_path = os.path.expanduser(cache_path)
            if os.path.exists(expanded_path):
                self._log(f"  Removing {expanded_path}")
                shutil.rmtree(expanded_path, ignore_errors=True)

    def build_and_publish(self, delta_dir: str, version: str,
                         clear_cache: bool = True) -> Path:
        """
        Build Delta and publish to local Maven repository.

        Args:
            delta_dir: Path to Delta source directory
            version: Delta version to build
            clear_cache: Whether to clear artifact caches before building

        Returns:
            Path to published artifacts in ~/.m2/repository

        Raises:
            Exception if build fails
        """
        delta_path = Path(delta_dir)

        if not delta_path.exists():
            raise Exception(f"Delta directory does not exist: {delta_dir}")

        # Acquire file lock to prevent concurrent builds of same version
        lock_file_path = f"/tmp/delta_build_{version.replace('.', '_').replace('-', '_')}.lock"
        lock_file = open(lock_file_path, 'w')

        try:
            self._log(f"Acquiring build lock for version {version}...")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._log("✓ Build lock acquired")

            # Clear caches if requested
            if clear_cache:
                self.clear_artifact_caches()

            # Set environment variables
            build_env = os.environ.copy()
            build_env["SBT_OPTS"] = "-J-Xmx8G -J-XX:+UseG1GC"
            build_env["DELTA_VERSION"] = version

            # Build command
            sbt_cmd = [str(delta_path / "build" / "sbt"), "publishM2"]

            self._log(f"Building Delta {version}...")
            self._log(f"Command: {' '.join(sbt_cmd)}")
            self._log("This may take 5-10 minutes...")

            # Run build
            proc = subprocess.Popen(
                sbt_cmd,
                cwd=delta_dir,
                env=build_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            # Stream output
            for line in proc.stdout:
                if self.verbose:
                    print(line, end='')

            proc.wait(timeout=900)  # 15 minute timeout

            if proc.returncode != 0:
                raise Exception(f"Delta build failed with exit code {proc.returncode}")

            self._log("✓ Delta build completed successfully")

            # Return path to published artifacts
            m2_path = Path.home() / ".m2" / "repository" / "io" / "delta"
            return m2_path

        except BlockingIOError:
            raise Exception(f"Another build of version {version} is already in progress")
        except subprocess.TimeoutExpired:
            proc.kill()
            raise Exception("Build timed out after 15 minutes")
        finally:
            # Release lock
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
                os.remove(lock_file_path)
            except:
                pass
