# SparkShell

Python-based framework for automatically building and testing Delta Lake with custom branches and Unity Catalog integration.

## Overview

SparkShell provides a simple Python API to:
- Build Delta Lake from any GitHub branch or local repository
- Automatically publish to local Maven repository (~/.m2)
- Start a Spark SQL server with Delta Lake and Unity Catalog support
- Execute SQL queries via REST API
- Cache builds for fast iteration

**Key Innovation:** Test Delta Lake changes before merging by building from your feature branch!

## Quick Start

### Installation

```bash
# Create Python 3.7+ virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```python
from spark_shell import SparkShell

# Use default Delta 4.0.0
with SparkShell(source=".") as shell:
    result = shell.execute_sql("SELECT 1 as id")
    print(result)
```

### Test Custom Delta Branch

```python
from spark_shell import SparkShell

# Test Delta from spark-uc branch
shell = SparkShell(
    source=".",
    delta_repo_url="https://github.com/tdas/delta",
    delta_branch="spark-uc",
    delta_version="3.4.0-SNAPSHOT"
)

with shell:
    # Your Delta from spark-uc branch is now running!
    result = shell.execute_sql("CREATE TABLE test (id INT)")
    print(result)
```

### Test Local Delta Changes

```python
from spark_shell import SparkShell

# Test your local Delta modifications
shell = SparkShell(
    source=".",
    use_local_delta="/home/user/delta"  # Your local Delta repo
)

with shell:
    # SparkShell builds and uses your local Delta
    result = shell.execute_sql("SELECT * FROM delta.`/path/to/table`")
    print(result)
```

## Features

### Delta Lake Custom Builds

- **Clone from GitHub**: Automatically clone Delta from any repository and branch
- **Local Development**: Use your local Delta repository with uncommitted changes
- **Version Detection**: Automatically detect Delta version from version.sbt
- **Build Caching**: First build takes 5-10 minutes, subsequent builds use cache
- **Maven Publishing**: Publishes to ~/.m2/repository for use by SparkShell
- **Compatibility Validation**: Warns if Delta branch incompatible with Spark version

### Unity Catalog Integration

```python
from spark_shell import SparkShell, UCConfig

uc_config = UCConfig(
    uri="http://localhost:8081",
    token="my-uc-token",
    catalog="unity",
    schema="default"
)

with SparkShell(source=".", uc_config=uc_config) as shell:
    # Three-level namespace automatically configured
    shell.execute_sql("CREATE TABLE unity.default.my_table (id INT)")
    shell.execute_sql("INSERT INTO unity.default.my_table VALUES (1)")
    result = shell.execute_sql("SELECT * FROM unity.default.my_table")
```

### Configuration

```python
from spark_shell import SparkShell, OpConfig, SparkConfig

# Operational configuration
op_config = OpConfig(
    verbose=True,              # Show detailed logs
    startup_timeout=120,       # Server startup timeout (seconds)
    cleanup_on_exit=True,      # Clean up temp files on exit
    build_timeout=600          # Build timeout (seconds)
)

# Spark configuration
spark_config = SparkConfig(configs={
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "10"
})

shell = SparkShell(
    source=".",
    op_config=op_config,
    spark_config=spark_config
)
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│              SparkShell (Python)                │
│  - Lifecycle management                         │
│  - REST API client                              │
│  - Configuration management                     │
└─────────────────┬───────────────────────────────┘
                  │
        ┌─────────┴──────────┐
        │                    │
┌───────▼────────┐  ┌────────▼─────────┐
│ DeltaBuilder   │  │  SparkShellServer│
│ (Python)       │  │  (Scala)         │
│ - Git clone    │  │ - Spark 4.0      │
│ - SBT build    │  │ - Delta Lake     │
│ - Maven publish│  │ - Unity Catalog  │
│ - Caching      │  │ - REST API       │
└────────────────┘  └──────────────────┘
        │
        │ Builds Delta and publishes to
        ▼
┌────────────────────┐
│ ~/.m2/repository   │
│ (Local Maven)      │
└────────────────────┘
```

### Build Flow

1. **Delta Preparation** (if custom Delta requested):
   - Clone Delta from GitHub or use local path
   - Detect version from version.sbt
   - Check build cache
   - Build with `build/sbt publishM2` (5-10 min first time)
   - Cache for future use
   - Set DELTA_VERSION environment variable

2. **SparkShell Build**:
   - Copy SparkShell source to temp directory
   - Build assembly JAR with SBT
   - Uses DELTA_VERSION from environment (if set)
   - Cache SparkShell build

3. **Server Startup**:
   - Start Scala server with Delta + Unity Catalog
   - Wait for health check
   - Ready to execute SQL

## API Reference

### SparkShell Constructor

```python
SparkShell(
    source: str,                        # SparkShell source directory
    port: int = 8080,                   # Server port
    delta_repo_url: Optional[str] = None,     # Delta GitHub URL
    delta_branch: str = "master",             # Delta branch
    delta_version: Optional[str] = None,      # Delta version (auto-detect if None)
    use_local_delta: Optional[str] = None,    # Local Delta path
    uc_config: Optional[UCConfig] = None,     # Unity Catalog config
    op_config: Optional[OpConfig] = None,     # Operational config
    spark_config: Optional[SparkConfig] = None # Spark config
)
```

### Methods

- `start(force_refresh=False)`: Start the server
- `shutdown()`: Stop the server
- `execute_sql(query: str) -> list`: Execute SQL and return results
- `is_healthy() -> bool`: Check if server is running

## Cache Management

### SparkShell Build Cache

Location: `~/.sparkshell_cache/<source_hash>/`

Clear with:
```python
import shutil
shutil.rmtree(os.path.expanduser("~/.sparkshell_cache"))
```

### Delta Build Cache

Location: `~/.delta_build_cache/<cache_key>/`

Clear with:
```python
from delta_builder import DeltaBuildCache
cache = DeltaBuildCache()
cache.clear_cache()  # Clear all
# or
cache.clear_cache("specific_key")  # Clear specific build
```

### Maven Artifacts

Location: `~/.m2/repository/io/delta/`

Clear with:
```bash
rm -rf ~/.m2/repository/io/delta
```

## Testing

Run all tests:
```bash
python3 run_all_tests.py
```

See [tests/README.md](tests/README.md) for detailed test documentation.

## Requirements

- Python 3.7+
- Java 11
- SBT (included in Delta repository as build/sbt)
- 8GB+ RAM for building Delta
- 5GB+ free disk space

## Troubleshooting

### Build Failures

**Problem:** Delta build times out or fails

**Solution:**
```python
# Increase timeouts
op_config = OpConfig(build_timeout=1200)  # 20 minutes

# Clear caches and rebuild
builder = DeltaBuilder()
builder.clear_artifact_caches()
```

### Port Already in Use

**Problem:** `Port 8080 is already in use`

**Solution:**
```python
# Use different port
shell = SparkShell(source=".", port=8081)
```

### Memory Issues

**Problem:** OutOfMemoryError during build

**Solution:** Delta build requires 8GB heap. Ensure sufficient system RAM or adjust:
```bash
export SBT_OPTS="-J-Xmx8G -J-XX:+UseG1GC"
```

## Examples

See usage examples in the Quick Start section above, or check:
- `test_delta_integration.py` - Integration test examples
- Unity Catalog test suites in `spark/unitycatalog/src/test/scala/`

## License

Apache License 2.0 (same as Delta Lake)
