#!/usr/bin/env bash
#
# Test build.sbt DELTA_VERSION environment variable reading
#
# This script tests that build.sbt correctly reads DELTA_VERSION from
# the environment and uses the default when not set.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Testing build.sbt environment variable reading"
echo "=" ================================================

# Test 1: Default version (no env var)
echo -e "\nTest 1: No DELTA_VERSION set (should use default 4.0.0)"
unset DELTA_VERSION
OUTPUT=$(../../../build/sbt "show getDeltaVersion" 2>&1 | grep -A 2 "getDeltaVersion")
if echo "$OUTPUT" | grep -q "4.0.0"; then
    echo "✓ Default version 4.0.0 used correctly"
else
    echo "✗ Failed to use default version"
    echo "Output: $OUTPUT"
    exit 1
fi

# Test 2: Custom version from env var
echo -e "\nTest 2: DELTA_VERSION=3.4.0-SNAPSHOT"
export DELTA_VERSION="3.4.0-SNAPSHOT"
OUTPUT=$(../../../build/sbt "show getDeltaVersion" 2>&1 | grep -A 2 "getDeltaVersion")
if echo "$OUTPUT" | grep -q "3.4.0-SNAPSHOT"; then
    echo "✓ Custom version 3.4.0-SNAPSHOT used correctly"
else
    echo "✗ Failed to use custom version"
    echo "Output: $OUTPUT"
    exit 1
fi

# Test 3: Different custom version
echo -e "\nTest 3: DELTA_VERSION=2.3.0"
export DELTA_VERSION="2.3.0"
OUTPUT=$(../../../build/sbt "show getDeltaVersion" 2>&1 | grep -A 2 "getDeltaVersion")
if echo "$OUTPUT" | grep -q "2.3.0"; then
    echo "✓ Custom version 2.3.0 used correctly"
else
    echo "✗ Failed to use custom version"
    echo "Output: $OUTPUT"
    exit 1
fi

echo -e "\n================================================"
echo "✓ All build.sbt tests passed!"
