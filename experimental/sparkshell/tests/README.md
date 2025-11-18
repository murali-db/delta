# SparkShell Tests

Comprehensive test suite for SparkShell with Delta Lake custom build support.

## Test Structure

```
experimental/sparkshell/
├── test_import.py              # Basic module import tests (5 tests)
├── test_delta_builder.py       # Delta builder unit tests (7 tests)
├── test_delta_integration.py   # Integration tests (5 tests)
├── test_build_sbt.sh          # Build.sbt env var tests
└── run_all_tests.py           # Comprehensive test runner
```

## Running Tests

### All Tests

Run all test suites:
```bash
python3 run_all_tests.py
```

Or with venv:
```bash
./venv/bin/python run_all_tests.py
```

### Individual Test Suites

```bash
# Import tests
python3 test_import.py

# Delta builder tests
python3 test_delta_builder.py

# Integration tests
python3 test_delta_integration.py

# Build.sbt tests (requires SBT)
./test_build_sbt.sh
```

## Test Categories

### Unit Tests
- **test_import.py**: Module import and class instantiation
- **test_delta_builder.py**: Delta builder functionality without actual builds
  - Cache operations
  - Compatibility validation
  - Path generation

### Integration Tests
- **test_delta_integration.py**: SparkShell + Delta builder integration
  - Constructor parameter validation
  - Version detection
  - Builder initialization logic

### Build Tests
- **test_build_sbt.sh**: Environment variable reading in build.sbt
  - Default version usage
  - Custom version from DELTA_VERSION env var

## Requirements

- Python 3.7+
- requests library (install via: `pip install -r requirements.txt`)
- For build.sbt tests: Java 11, SBT

## Test Coverage

Total: **17 tests** across 3 test suites

- ✅ Module imports and class existence
- ✅ Delta builder cache operations
- ✅ Compatibility matrix validation
- ✅ Git clone operations
- ✅ Build locking mechanisms
- ✅ SparkShell constructor with Delta parameters
- ✅ Version detection from version.sbt
- ✅ Environment variable propagation

## CI Integration

Tests are designed to run quickly without requiring:
- Actual Delta Lake builds (too slow for CI)
- Running Spark servers
- Network access (except git clone validation which uses timeouts)

All tests complete in < 5 seconds total.
