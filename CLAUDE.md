# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta Lake is an open-source storage framework that enables building a Lakehouse architecture. This is the main Delta Lake repository containing the Spark connector, Kernel library, and various connectors for compute engines (Flink, standalone, etc.). The project is primarily written in Scala and Java.

## Build System

Delta Lake uses SBT (Scala Build Tool) for compilation and testing.

### Common Commands

**Build and compile:**
```bash
build/sbt compile
```

**Generate artifacts:**
```bash
build/sbt package
```

**Run all tests:**
```bash
build/sbt test
```

**Run a specific test suite:**
```bash
build/sbt spark/'testOnly org.apache.spark.sql.delta.DeltaLogSuite'
```

**Run a specific test within a suite:**
```bash
build/sbt spark/'testOnly *.DeltaLogSuite -- -z "test name substring"'
```

**Run tests for a specific project/module:**
```bash
build/sbt spark/test                    # Run all tests in spark module
build/sbt kernel-defaults/test          # Run all tests in kernel-defaults module
build/sbt unity/test                    # Run all tests in unity module
```

**Enable test coverage:**
```bash
build/sbt coverage test coverageAggregate coverageOff
```

### Python Tests

Python tests require a Conda environment setup:

```bash
# Create environment from environment.yml
conda env create --name delta_python_tests --file=/absolute/path/to/python/environment.yml

# Activate environment
conda activate delta_python_tests

# Run Python tests
python3 python/run-tests.py
```

**Requirements:** JDK 11 must be configured via `JAVA_HOME`.

## Architecture

### Module Structure

The repository contains several key modules organized in a layered architecture:

**Core Modules:**
- **`spark/`** - Primary Delta Lake implementation for Apache Spark
  - Contains transaction management (`OptimisticTransaction`, `DeltaLog`)
  - SQL command implementations (write, delete, update, merge)
  - Streaming sources/sinks
  - Main implementation code: `spark/src/main/scala/org/apache/spark/sql/delta/`
  - **`spark/unitycatalog/`** - NEW Unity Catalog integration test module (see "Current Branch Focus" section)

- **`kernel/`** - Engine-agnostic Java library for Delta protocol
  - `kernel-api` - Public APIs (Table, Snapshot, Scan)
  - `kernel-defaults` - Default implementations using Hadoop
  - `kernel-benchmarks` - Performance testing
  - Designed for building connectors to any compute engine

- **`storage/`** - Abstract LogStore interface for file system operations
  - Defines atomicity/durability requirements for Delta's ACID guarantees
  - Key class: `storage/src/main/java/io/delta/storage/LogStore.java`

- **`unity/`** - Unity Catalog integration (production code)
  - Coordinated commits with Unity Catalog
  - Catalog-managed table support
  - Note: `spark/unitycatalog/` contains integration tests for this module

**Connectors:**
- **`connectors/standalone`** - JVM library for non-Spark applications
- **`connectors/flink`** - Apache Flink sink/source
- **`kernel-spark`** - Spark DataSource V2 using Kernel

**Format Support:**
- **`iceberg/`** - UniForm integration with Iceberg
- **`hudi/`** - UniForm integration with Hudi

### Architectural Patterns

**Transaction Log Protocol:**
All table changes are recorded as JSON actions in `_delta_log/` directory:
- Actions defined in: `spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala`
- Action types: `Protocol`, `Metadata`, `AddFile`, `RemoveFile`, `CommitInfo`, `SetTransaction`
- Checkpoints consolidate logs (V1 classic and V2 UUID-named formats)

**Multi-Version Concurrency Control (MVCC):**
- Optimistic concurrency control via `OptimisticTransaction`
- Conflict detection and resolution during commit
- Core logic in: `spark/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala`

**Command Pattern:**
All operations extend `DeltaCommand`:
- Location: `spark/src/main/scala/org/apache/spark/sql/delta/commands/`
- Examples: `WriteIntoDelta`, `DeleteCommand`, `UpdateCommand`, `MergeIntoCommand`, `OptimizeTableCommand`, `VacuumCommand`

**State Management:**
- `DeltaLog` - Central coordinator for table state
- `Snapshot` - Point-in-time table state
- `SnapshotManagement` - Version tracking and cache management

### Key Entry Points

**For understanding reads:**
- Batch: `DeltaDataSource.scala` → `DeltaLog.createRelation()`
- Streaming: `DeltaSource.scala`
- Entry class: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaDataSource.scala`

**For understanding writes:**
- Main command: `WriteIntoDelta` in `spark/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala`
- Transaction logic: `OptimisticTransaction.scala` (see `commit()` at ~line 1332, `doCommit()` at ~line 2328)

**For understanding table operations:**
- DELETE: `spark/src/main/scala/org/apache/spark/sql/delta/commands/DeleteCommand.scala`
- UPDATE: `spark/src/main/scala/org/apache/spark/sql/delta/commands/UpdateCommand.scala`
- MERGE: `spark/src/main/scala/org/apache/spark/sql/delta/commands/MergeIntoCommand.scala`
- OPTIMIZE: `spark/src/main/scala/org/apache/spark/sql/delta/commands/optimize/OptimizeTableCommand.scala`

**For understanding the protocol:**
- Start with: `PROTOCOL.md` (complete protocol specification)
- Implementation: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala`
- Checkpoints: `spark/src/main/scala/org/apache/spark/sql/delta/Checkpoints.scala`

### Feature Implementation Areas

**Deletion Vectors:**
- Location: `spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/`
- Soft delete mechanism without file rewrites

**Coordinated Commits:**
- Location: `spark/src/main/scala/org/apache/spark/sql/delta/coordinatedcommits/`
- Pluggable commit coordination (e.g., Unity Catalog)

**Schema Evolution:**
- Location: `spark/src/main/scala/org/apache/spark/sql/delta/schema/`
- Handles merging, column mapping, type widening

**Clustering & Optimization:**
- Z-ordering: `spark/src/main/scala/org/apache/spark/sql/delta/zorder/`
- Clustering: `spark/src/main/scala/org/apache/spark/sql/delta/clustering/`

**Table Features:**
- Location: `spark/src/main/scala/org/apache/spark/sql/delta/TableFeature.scala`
- Protocol evolution mechanism via feature flags

## Testing

### Test Organization

Tests are organized by module, with the main test suite in:
- `spark/src/test/scala/org/apache/spark/sql/delta/` - Main Delta Lake tests
- Tests follow naming convention: `*Suite.scala` for test suites
- Base traits (e.g., `DeleteSuiteBase`, `CloneTableSuiteBase`) provide reusable test utilities
- Test utilities (e.g., `DeletionVectorsTestUtils`, `DeltaMetricsUtils`) in same directory

### Spark Version Compatibility

Tests include version-specific shims:
- `spark/src/test/scala-spark-3.5/shims/` - Spark 3.5 specific tests
- `spark/src/test/scala-spark-master/shims/` - Latest Spark version tests

### Running Integration Tests

```bash
python3 run-integration-tests.py
```

## Development Setup

### IntelliJ IDEA Setup

1. Clone repository
2. Import as SBT project with JDK 11
3. Run `build/sbt clean package` to generate necessary files
4. Verify setup by running `DeltaLogSuite` test

### Scala Version

- Current default: Scala 2.13.16
- All versions supported: Scala 2.13
- To build with specific version: `sbt '++ 2.13.16' compile`

## Important Files

- `PROTOCOL.md` - Complete Delta transaction protocol specification
- `build.sbt` - Main build configuration (module structure, dependencies)
- `README.md` - General project information
- `CONTRIBUTING.md` - Contribution guidelines (Apache Spark Scala Style Guide)

## Code Style

Follow the [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html). Use `scalastyle-config.xml` for automated style checking.

## Navigation Tips

1. **Start with `DeltaLog.scala`** to understand the central orchestrator
2. **Read `PROTOCOL.md`** for complete protocol understanding
3. **Explore `actions.scala`** for all state-changing primitives
4. **Study `OptimisticTransaction.scala`** for transaction lifecycle
5. **Check `commands/` directory** for operation implementations
6. **Review Kernel** (`kernel/`) for engine-agnostic protocol implementation
7. **Examine `build.sbt`** to understand module dependencies

## Experimental: SparkShell with Custom Delta Builds

**Location:** `experimental/sparkshell/`

SparkShell is a Python framework for testing Delta Lake with custom builds and Unity Catalog integration.

### Quick Start

```python
from spark_shell import SparkShell

# Test Delta from any GitHub branch
shell = SparkShell(
    source=".",
    delta_repo_url="https://github.com/tdas/delta",
    delta_branch="spark-uc",
    delta_version="3.4.0-SNAPSHOT"
)

with shell:
    result = shell.execute_sql("CREATE TABLE test (id INT)")
```

### Key Features

- **Custom Delta Builds**: Build Delta from any GitHub branch or local directory
- **Automatic Publishing**: Publishes Delta to ~/.m2/repository
- **Build Caching**: First build 5-10 min, subsequent builds use cache
- **Unity Catalog Integration**: Built-in UC support with three-level namespace
- **Version Detection**: Auto-detects Delta version from version.sbt

### Components

- **`spark_shell.py`**: Main Python API for lifecycle management
- **`delta_builder.py`**: Delta clone, build, and publish infrastructure
- **`build.sbt`**: SparkShell Scala server with Spark 4.0 + Delta + UC
- **Tests**: 17 unit and integration tests (run with `python3 run_all_tests.py`)

### Usage

See [experimental/sparkshell/README.md](experimental/sparkshell/README.md) for comprehensive documentation.

---

## Current Branch Focus: Unity Catalog Integration

**This branch contains recent work by TD on Unity Catalog integration between commits:**
- **Start:** `6cd09f41a` "spark-uc module"
- **End:** `2f6783d60` "Add Unity Catalog PR strategy and documentation"

### Unity Catalog Module (`spark/unitycatalog/`)

A new SBT module has been created specifically for Unity Catalog integration testing. This work establishes comprehensive test coverage for Delta Lake operations with Unity Catalog.

**Key Files and Location:**
- Module root: `spark/unitycatalog/`
- Test package: `com.sparkuctest`
- Test files location: `spark/unitycatalog/src/test/scala/com/sparkuctest/`

### Test Framework Architecture

**Foundation Infrastructure:**
- **`UnityCatalogSupport.scala`** - Core test infrastructure for Unity Catalog integration
  - Provides helper methods for catalog operations
  - Manages test lifecycle and cleanup
  - Located at: `spark/unitycatalog/src/test/scala/com/sparkuctest/UnityCatalogSupport.scala`

- **`UCDeltaTableIntegrationSuiteBase.scala`** - Base test suite class
  - Common setup/teardown for all Unity Catalog Delta table tests
  - Shared utilities and assertions
  - Located at: `spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableIntegrationSuiteBase.scala`

### Test Suites (43 Total Tests)

**DML Operations (13 tests) - `UCDeltaTableDMLSuite.scala`**
- INSERT, UPDATE, DELETE, MERGE operations
- Transactional behavior verification
- Data correctness validation

**DDL Operations (9 tests) - `UCDeltaTableDDLSuite.scala`**
- CREATE TABLE, ALTER TABLE, DROP TABLE
- Schema evolution and modifications
- Table property management

**Utility Operations (10 tests) - `UCDeltaTableUtilitySuite.scala`**
- OPTIMIZE, VACUUM operations
- Table maintenance commands
- Performance-related utilities

**Read Operations (7 tests) - `UCDeltaTableReadSuite.scala`**
- Various read patterns and queries
- Time travel queries
- Schema inference

**Foundation Tests (4 tests) - `UnityCatalogSupportSuite.scala`**
- Basic Unity Catalog connectivity
- Catalog/schema/table lifecycle
- Setup verification

### Testing Environment

- **Spark Version:** Spark 4.0
- **Unity Catalog Version:** 0.3.0
- **Status:** All 43 tests passing ✅
- **Total Test Code:** ~1,898 lines across test suites

### Documentation

**`spark/unitycatalog/PR_PLAN.md`**
- Detailed plan for breaking work into 5 PRs (~500 lines each)
- Individual PR branches and their scope
- Test coverage summary

**`spark/unitycatalog/MANAGE_GIT_STACK.md`**
- Git workflow for managing the PR stack
- Branch management conventions

### Running Unity Catalog Tests

```bash
# Run all Unity Catalog integration tests
build/sbt sparkUnityCatalog/test

# Run a specific test suite
build/sbt sparkUnityCatalog/'testOnly com.sparkuctest.UCDeltaTableDMLSuite'

# Run a specific test
build/sbt sparkUnityCatalog/'testOnly com.sparkuctest.UCDeltaTableDMLSuite -- -z "INSERT"'
```

### Build Configuration

The Unity Catalog module is defined in `build.sbt` (lines 776-814) as `sparkUnityCatalog` and includes:
- Dependencies on Spark and Unity Catalog libraries
- Test-only module (no production code yet)
- Integration with main Delta Lake Spark module
