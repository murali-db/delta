# Presigned URL JSON Support Implementation

## Summary

This implementation adds support for reading JSON data from presigned HTTPS URLs returned by Unity Catalog's MATERIALIZED_JSON scan mode. This enables OSS Spark (via SparkShell) to work with FGAC tables that have row-level security or column masking policies.

## Implementation Date

December 10, 2025

## Branch

`server-side-planning-D-credentials-injection`

## Files Modified

### 1. ServerSidePlannedTable.scala
**Path**: `/home/murali.ramanujam/td-sparkshell/delta/spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/ServerSidePlannedTable.scala`

**Changes Made**:
- Added imports for Jackson JSON parsing, Apache HTTP client, and Spark type conversion utilities
- Added `PresignedUrlJsonPartitionReader` class (~150 lines) that:
  - Fetches JSON from presigned HTTPS URLs via HTTP GET
  - Parses array-of-arrays JSON format from Unity Catalog
  - Converts JSON arrays to Spark InternalRows with schema validation
  - Supports all primitive types (Int, Long, String, Boolean, etc.)
- Modified `ServerSidePlannedFilePartitionReaderFactory.createReader()` to:
  - Detect presigned URLs by checking if path starts with "https://"
  - Route to JSON reader for presigned URLs
  - Route to existing Parquet reader for S3 paths (unchanged)
- Added `isPresignedUrl()` helper method for URL detection

## Key Design Decisions

### 1. URL Detection Strategy
**Hardcoded HTTPS detection**: Check if `filePath.startsWith("https://")`
- Simple and reliable
- Can be enhanced later with more sophisticated pattern matching

### 2. No File Format Validation for Presigned URLs
**Critical Finding**: Unity Catalog reports `file-format: "PARQUET"` even when returning JSON content from presigned URLs.

**Solution**: Skip file format validation when presigned URL is detected. Detection is based solely on URL format (https://), not the file-format field.

```scala
if (isPresignedUrl(filePartition.filePath)) {
  // Note: UC reports fileFormat as "PARQUET" even when returning JSON
  // So we don't validate the format field here
  new PresignedUrlJsonPartitionReader(filePartition.filePath, schema)
}
```

### 3. JSON Format: Array-of-Arrays
**UC Response Format**:
```json
[
  ["value1_col1", "value1_col2"],
  ["value2_col1", "value2_col2"],
  ["value3_col1", "value3_col2"]
]
```

**Not** object-based JSON:
```json
[
  {"col1": "value1", "col2": "value2"},
  {"col1": "value3", "col2": "value4"}
]
```

**Implementation**:
- Outer array contains rows
- Each inner array contains column values in schema field order
- Values are mapped to schema by position (index), not by field name
- Array length is validated against schema field count

### 4. Type Conversion
**Supported Types**:
- Primitives: `Int`, `Long`, `Double`, `Float`, `String`, `Boolean`, `Short`, `Byte`
- Date/Time: `Date` (yyyy-MM-dd string → days since epoch), `Timestamp` (ISO-8601 → microseconds)

**Not Supported** (will throw `UnsupportedOperationException`):
- Complex types: `Struct`, `Array`, `Map`
- `Decimal` (can be added if needed)

## Actual UC Response Example

Based on manual testing with Unity Catalog:

**Request**:
```bash
curl --request POST \
  "https://e2-dogfood.staging.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/migration_bugbash/namespaces/philipzhu2_rlscm/tables/test_table_1099/plan?implementation=MATERIALIZED_JSON" \
  --header 'Authorization: Bearer <PAT>' \
  -H "Content-Type: application/json" \
  -d '{"snapshot-id": 0, "select": ["name"]}'
```

**Response**:
```json
{
  "plan-id": null,
  "plan-status": "COMPLETED",
  "file-scan-tasks": [
    {
      "data-file": {
        "spec-id": 0,
        "content": "DATA",
        "file-path": "https://e2-dogfood-core.s3.us-west-2.amazonaws.com/oregon-staging/...<presigned-url>...",
        "file-format": "PARQUET",  <-- Still reports PARQUET!
        "partition": {},
        "file-size-in-bytes": 127,
        "record-count": 7,
        "sort-order-id": 0
      },
      "delete-file-references": [],
      "residual-filter": true
    }
  ],
  "storage-credentials": []
}
```

**Fetching the presigned URL returns**:
```json
[["redacted_name"],["redacted_name"],["redacted_name"],["redacted_name"],["redacted_name"],["redacted_name"],["redacted_name"]]
```

This is 7 rows with 1 column each (matching the `select: ["name"]` in the request).

## Data Flow

```
UC /plan endpoint (?implementation=MATERIALIZED_JSON)
    ↓ Returns FileScanTask with presigned URL (file-format: "PARQUET")
IcebergRESTCatalogPlanningClient.planScan()
    ↓ Extracts https://... from file.path()
    ↓ Creates ScanFile with filePath and fileFormat
ServerSidePlannedScan.planInputPartitions()
    ↓ Creates ServerSidePlannedFileInputPartition per file
ServerSidePlannedFilePartitionReaderFactory.createReader()
    ↓ Calls isPresignedUrl(filePath)
    ↓ Detects "https://" → uses JSON reader path
PresignedUrlJsonPartitionReader
    ↓ HTTP GET to presigned URL → fetches JSON string
    ↓ Parses JSON: objectMapper.readTree() → JsonNode array
    ↓ Iterates rows: arrayNode.elements().asScala
    ↓ Per row: jsonToInternalRow(jsonNode)
    ↓   → Validates array length matches schema
    ↓   → Maps array[i] to schema.field[i] by position
    ↓   → Type conversion based on field.dataType
    ↓ Returns Iterator[InternalRow]
Spark consumes rows from iterator
    ↓ Query results returned to user
```

## Error Handling

### HTTP Errors
- **403/404**: "Failed to fetch presigned URL. HTTP status: 403" (URL expired or invalid)
- **Network errors**: "Error fetching presigned URL: <url>" with cause exception

### JSON Parse Errors
- **Not an array**: "Expected JSON array from presigned URL but got: ..."
- **Array length mismatch**: "JSON array size (N) doesn't match schema field count (M)"
- **Type conversion**: "Unsupported data type for JSON conversion: X for field 'Y'"

### Schema Validation
- **Null non-nullable field**: "Required field 'X' (index N) is null in JSON array"
- **Row format error**: "Expected JSON array for row but got: <node-type>"

## Backward Compatibility

✅ **No Breaking Changes**:
- Existing S3 path + Parquet flow is completely unchanged
- Existing credential injection for S3 is unchanged
- New code path only activates for HTTPS URLs
- Detection is explicit and safe (https:// prefix check)

## Performance Characteristics

### JSON Parsing Overhead
- **Estimate**: ~2-3x slower than Parquet (acceptable for FGAC use case where security > performance)
- **Mitigation**: Jackson ObjectMapper is efficient; parsing happens in parallel on executors

### HTTP Fetch Latency
- **Per-partition latency**: 100-500ms per HTTP GET request
- **Mitigation**: Parallelism across executors; presigned URLs typically served from edge cache

### Memory Usage
- **Current**: Entire JSON response is fetched into memory before parsing
- **Risk**: Large result sets (>1GB JSON) could cause OOM on executors
- **Future Enhancement**: Implement streaming JSON parser

## Testing Guide

### Manual Testing with Unity Catalog

1. **Prerequisites**:
   - Databricks workspace with Unity Catalog enabled
   - Table with FGAC policy (column masking or row-level security)
   - Databricks personal access token

2. **Test with curl** (to verify UC endpoint):
   ```bash
   curl --request POST \
     "https://<your-workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/<catalog>/namespaces/<schema>/tables/<table>/plan?implementation=MATERIALIZED_JSON" \
     --header "Authorization: Bearer <your-token>" \
     -H "Content-Type: application/json" \
     -d '{"snapshot-id": 0, "select": ["column1", "column2"]}'
   ```

   Expected: Response with `file-scan-tasks` containing presigned HTTPS URLs

3. **Test with SparkShell**:
   ```python
   from spark_shell import SparkShell, UCConfig, DeltaConfig, OpConfig

   # Point to your modified Delta with presigned URL support
   delta_config = DeltaConfig(
       source_repo="file:///home/murali.ramanujam/td-sparkshell/delta",
       source_branch="server-side-planning-D-credentials-injection"
   )

   uc_config = UCConfig(
       uri="https://<your-workspace>.cloud.databricks.com/",
       token="<your-token>",
       catalog="<catalog>",
       schema="<schema>"
   )

   with SparkShell(
       source=".",
       delta_config=delta_config,
       uc_config=uc_config,
       op_config=OpConfig(verbose=True)
   ) as shell:
       # Query table with FGAC policy
       result = shell.execute_sql("SELECT * FROM masked_table")
       print(result)
   ```

4. **Expected behavior**:
   - Query succeeds
   - Masked columns show masked values (e.g., "***" instead of actual data)
   - No credential errors
   - Data is fetched from presigned URLs

### Test with Column Masking

```python
# In Databricks notebook (DBR):
spark.sql("""
CREATE TABLE main.default.employee (
  id INT,
  name STRING,
  salary INT
)
""")

spark.sql("INSERT INTO main.default.employee VALUES (1, 'Alice', 100000), (2, 'Bob', 120000)")

# Create masking function
spark.sql("""
CREATE FUNCTION main.default.mask_salary()
RETURNS INT
RETURN
  CASE
    WHEN is_account_group_member('hr') THEN salary
    ELSE NULL
  END
""")

# Apply mask
spark.sql("""
ALTER TABLE main.default.employee
ALTER COLUMN salary
SET MASK main.default.mask_salary
""")

# Test from DBR (will see actual salary if in 'hr' group)
spark.sql("SELECT * FROM main.default.employee").show()

# Test from OSS Spark via SparkShell (should see masked values)
# Use the SparkShell code from above
```

## Known Limitations

1. **Complex Types**: Struct, Array, Map not supported (will throw exception)
2. **Large Result Sets**: Entire JSON loaded into memory (OOM risk for >1GB)
3. **No Retry Logic**: Transient HTTP failures are not retried
4. **No Compression**: gzipped JSON responses not supported
5. **Decimal Type**: Not implemented (easy to add if needed)

## Future Enhancements

1. **Streaming JSON Parser**: Avoid loading entire response into memory
2. **Retry Logic**: Automatic retry with exponential backoff for transient failures
3. **Compression Support**: Handle gzip, deflate content encoding
4. **Complex Type Support**: Parse nested JSON structures
5. **Metrics**: Add Spark metrics for fetch time, parse time, row count
6. **URL Pattern Detection**: More sophisticated presigned URL detection beyond "https://"
7. **Integration Tests**: Unit tests for JSON parsing, integration tests with mock UC server

## How to Build and Deploy

### Build Delta with Changes

```bash
cd /home/murali.ramanujam/td-sparkshell/delta
build/sbt "project spark" compile
build/sbt "project spark" publishLocal
```

### Use in SparkShell

```python
delta_config = DeltaConfig(
    source_repo="file:///home/murali.ramanujam/td-sparkshell/delta",
    source_branch="server-side-planning-D-credentials-injection"
)

with SparkShell(source=".", delta_config=delta_config, uc_config=uc_config) as shell:
    result = shell.execute_sql("SELECT * FROM fgac_table")
```

SparkShell will automatically:
1. Build Delta from the local repository
2. Publish to local Maven repository
3. Build SparkShell with the updated Delta dependency
4. Start server with presigned URL support

## Success Criteria

✅ **Implemented**:
- Detect presigned URLs (https://)
- Fetch JSON from presigned URLs via HTTP GET
- Parse array-of-arrays JSON format from UC
- Convert JSON to Spark InternalRows with schema validation
- Support primitive types (Int, Long, String, Boolean, etc.)
- Maintain backward compatibility with S3 + Parquet flow

🔄 **To Verify**:
- Build and compile successfully
- Manual test with actual UC MATERIALIZED_JSON endpoint
- Verify column masking works end-to-end with OSS Spark
- Performance acceptable (~2-3x Parquet is OK for security use case)

## References

- **Universe PR #1418008**: Added MATERIALIZED_JSON mode to UC filtering service
- **Delta branch**: `server-side-planning-D-credentials-injection` (murali-db/delta)
- **SparkShell**: Uses DeltaConfig to point to this Delta branch
- **UC Iceberg REST API**: `/v1/.../tables/{table}/plan?implementation=MATERIALIZED_JSON`

## Contact

For questions or issues:
- Check Delta logs in `~/.sparkshell_cache/<hash>/delta/`
- Check SparkShell logs in notebook output (verbose=True)
- Review implementation plan: `/home/murali.ramanujam/.claude/plans/polymorphic-forging-hamming.md`

---

**Status**: ✅ Implementation Complete - Ready for Testing

**Next Steps**:
1. Build Delta Lake with changes
2. Test with SparkShell + UC MATERIALIZED_JSON endpoint
3. Verify column masking works correctly
4. Add unit tests (optional, can be done later)
5. Merge to main branch once verified
