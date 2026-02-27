# PO Metrics Post-Commit Hook Implementation

## Overview

Phase 1 of PO (Predictive Optimization) integration for externally-accessed Unity Catalog Managed
Delta tables. Adds a post-commit hook that sends commit statistics synchronously to a UC endpoint
so the PO backend has visibility into external (non-DBR) writes.

**Design Doc**: https://docs.google.com/document/d/1pbMbCBIvU7X8cFdb14HrJWhIfS3vzHcvsXBVv0GXp_0/edit?tab=t.0#heading=h.ch26vislrfey
**Server-side PR**: https://github.com/databricks-eng/universe/pull/1387169 (ReportDeltaMetricsHandler)

---

## Problem Statement

PO has no visibility into UC Managed Delta tables accessed by external engines (non-DBR). External
writes don't send metrics to PO, causing inconsistent optimization behavior compared to Managed
Iceberg tables. Goal: achieve parity with Managed Iceberg table support.

---

## Architecture

```
Delta Commit
  → OptimisticTransaction.runPostCommitHooks()
      → UpdatePOMetricsHook.run()
          → Extract metrics from CommittedTransaction (AddFile/RemoveFile/CommitInfo)
          → Build JSON payload matching server contract
          → POMetricsClient.sendMetrics() [Synchronous HTTP POST]
              → POST /api/2.1/unity-catalog/delta/preview/metrics
                  → ReportDeltaMetricsHandler (server)
                      → Validates commit_version (staleness check)
                      → PredictiveOptimizationClient.pushExternalDeltaCommitMetrics
                          → Kafka → PO Backend
```

---

## Files Created / Modified

### New Files

| File | Purpose |
|------|---------|
| `spark/src/main/scala/.../hooks/UpdatePOMetricsHook.scala` | Post-commit hook implementation |
| `spark/src/main/scala/.../hooks/POMetricsClient.scala` | Data classes + HTTP client |
| `spark-unified/src/test/scala/.../hooks/UpdatePOMetricsHookSuite.scala` | Test suite |

### Modified Files

| File | Change |
|------|--------|
| `spark/src/main/scala/.../sources/DeltaSQLConf.scala` | 4 new config entries |
| `spark/src/main/scala/.../OptimisticTransaction.scala` | Hook registration |
| `spark/src/main/scala/.../hooks/UpdatePOMetricsHook.scala` | Follow-up fixes for UC table ID resolution |
| `spark-unified/src/test/scala/.../hooks/UpdatePOMetricsHookSuite.scala` | Follow-up tests for table ID resolution precedence |

---

## Server Contract

Discovered from universe PR #1387169. The server-side handler is `ReportDeltaMetricsHandler`.

### Endpoint

```
POST /api/2.1/unity-catalog/delta/preview/metrics
```

Note: it is `/api/2.1/`, not `/api/2.0/`.

### Server Validation

1. Table lookup via `getTableById` to fetch PO-enable status and latest version
2. `commit_version` (from `file_size_histogram.commit_version`) must be within
   `validCommitVersionWindow` (default: **10**) of the latest UC-tracked table version
3. All numeric fields must be non-negative
4. Feature flag: `databricks.uniformIcebergRestCatalog.enableReportDeltaMetricsEndpoint`
   (default: false) — must be enabled server-side before the endpoint accepts requests

### What the Server Does with the Payload

Calls `PredictiveOptimizationClient.pushExternalDeltaCommitMetrics` with
`is_external_commit = Some(true)` so PO can distinguish external commits from native DBR commits.

### JSON Schema (snake_case, nested)

The server uses Jackson `@JsonSubTypes(WRAPPER_OBJECT)` polymorphism, which requires the
`commit_report` wrapper key.

```json
{
  "table_id": "550e8400-e29b-41d4-a716-446655440000",
  "report": {
    "commit_report": {
      "num_files_added": 10,
      "num_files_removed": 2,
      "num_bytes_added": 1048576,
      "num_bytes_removed": 204800,
      "num_clustered_bytes_added": 524288,
      "num_rows_inserted": 1000,
      "num_rows_removed": 50,
      "num_rows_updated": 25,
      "file_size_histogram": {
        "sorted_bin_boundaries": [0, 8192, 65536, 524288, 1048576, 4194304,
                                  8388608, 16777216, 33554432, 67108864,
                                  134217728, 268435456, 536870912, 1073741824],
        "file_counts": [0, 0, 0, 3, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "total_bytes": [0, 0, 0, 1572864, 29360128, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "commit_version": 42
      }
    }
  }
}
```

**Important field notes**:
- `num_rows_inserted` — NOT `num_rows_added`. Used for WRITE/MERGE inserts.
- `num_rows_removed` — Used for MERGE deletes / DELETE operations.
- `num_rows_updated` — MERGE updates and UPDATE operations.
- `num_clustered_bytes_removed` — supported by the server as optional, but intentionally omitted by this client (see section below).
- `commit_version` lives inside `file_size_histogram`, not at the top level.
- All fields are `Option[Long]` — absent from JSON when `None`.

---

## Data Classes (POMetricsClient.scala)

```scala
case class ReportDeltaMetricsRequest(
    @JsonProperty("table_id") tableId: String,
    @JsonProperty("report") report: CommitReportEnvelope)

case class CommitReportEnvelope(
    @JsonProperty("commit_report") commitReport: CommitReport)

case class CommitReport(
    @JsonProperty("num_files_added")          numFilesAdded:          Option[Long] = None,
    @JsonProperty("num_files_removed")        numFilesRemoved:        Option[Long] = None,
    @JsonProperty("num_bytes_added")          numBytesAdded:          Option[Long] = None,
    @JsonProperty("num_bytes_removed")        numBytesRemoved:        Option[Long] = None,
    @JsonProperty("num_clustered_bytes_added") numClusteredBytesAdded: Option[Long] = None,
    // num_clustered_bytes_removed intentionally omitted
    @JsonProperty("num_rows_inserted")        numRowsInserted:        Option[Long] = None,
    @JsonProperty("num_rows_removed")         numRowsRemoved:         Option[Long] = None,
    @JsonProperty("num_rows_updated")         numRowsUpdated:         Option[Long] = None,
    @JsonProperty("file_size_histogram")      fileSizeHistogram:      Option[FileSizeHistogramPayload] = None)

case class FileSizeHistogramPayload(
    @JsonProperty("sorted_bin_boundaries") sortedBinBoundaries: Seq[Long],
    @JsonProperty("file_counts")           fileCounts:          Seq[Long],
    @JsonProperty("total_bytes")           totalBytes:          Seq[Long],
    @JsonProperty("commit_version")        commitVersion:       Option[Long] = None)
```

---

## Metrics Extraction

### File-Level Metrics

Computed directly from the committed actions — always available.

| Field | Source |
|-------|--------|
| `num_files_added` | `addFiles.size` |
| `num_bytes_added` | `addFiles.map(_.size).sum` |
| `num_files_removed` | `removeFiles.size` |
| `num_bytes_removed` | `removeFiles.flatMap(_.size).sum` |
| `num_clustered_bytes_added` | `addFiles.filter(_.clusteringProvider.isDefined).map(_.size).sum` |

### Row-Level Metrics

Prefer `CommitInfo.operationMetrics` (more accurate, set by Spark operations). Fall back to
file-level `numLogicalRecords` when `operationMetrics` are absent (e.g. external writers or
simple WRITE operations that don't populate all keys).

#### `num_rows_inserted`

| Operation | operationMetrics key |
|-----------|----------------------|
| MERGE | `numTargetRowsInserted` |
| WRITE / STREAMING_UPDATE | `numOutputRows` |
| fallback | `sum(AddFile.numLogicalRecords)` |

#### `num_rows_removed`

| Operation | operationMetrics key |
|-----------|----------------------|
| MERGE | `numTargetRowsDeleted` |
| DELETE | `numDeletedRows` |
| fallback | `sum(RemoveFile.numLogicalRecords)` |

#### `num_rows_updated`

| Operation | operationMetrics key |
|-----------|----------------------|
| MERGE | `numTargetRowsUpdated` |
| UPDATE | `numUpdatedRows` |
| fallback | **none** — updated rows are indistinguishable from inserts at the file level |

### File Size Histogram

Built using the existing `FileSizeHistogram` class in OSS Delta.

**Bin boundaries** (bytes): 0, 8 KB, 64 KB, 512 KB, 1 MB, 4 MB, 8 MB, 16 MB, 32 MB, 64 MB,
128 MB, 256 MB, 512 MB, 1 GB

`commit_version` is set to `txn.committedVersion`. The server uses this to reject stale payloads
(must be within `validCommitVersionWindow=10` of UC's tracked latest version).

---

## num_clustered_bytes_removed: Investigation & Decision

### The Question

Can we compute `num_clustered_bytes_removed`? It would require knowing which removed files were
previously clustered.

### OSS Delta File Fields

`AddFile` has `clusteringProvider: Option[String]` (set to `Some("liquid")` for liquid-clustered
files). `RemoveFile` does **not** have this field — it was never added.

Both `AddFile` and `RemoveFile` have `tags: Map[String, String]`, but there is no clustering-related
tag key in `AddFile.Tags`. The full tag key list is:

```
ZCUBE_ID, ZCUBE_ZORDER_BY, ZCUBE_ZORDER_CURVE, INSERTION_TIME,
PARTITION_ID, OPTIMIZE_TARGET_SIZE, ICEBERG_COMPAT_VERSION
```

No `CLUSTERING_PROVIDER` tag exists. Clustering state is tracked exclusively via
`AddFile.clusteringProvider`.

### Discussion

When this gap was raised with the team, someone responded "we don't have tags yet" — meaning they
thought the clustering info might come from tags. This was incorrect: OSS Delta already has the
`tags` field on both `AddFile` and `RemoveFile`, but clustering is not stored there.

### Conclusion

To compute `num_clustered_bytes_removed` you would need to:

```
removed_file.path  →  join against previous snapshot  →  find original AddFile  →  check .clusteringProvider
```

This is a full snapshot scan on every commit — too expensive for a post-commit hook.

**Decision: omit `num_clustered_bytes_removed` from the payload.** The server's `CommitReport`
defines it as `Option[Long]` so its absence is valid. PO can approximate clustered bytes removed
from the historical `num_clustered_bytes_added` series if needed.

---

## Configuration

Defined in `DeltaSQLConf.scala`:

| Spark Config Key | Type | Default | Description |
|-----------------|------|---------|-------------|
| `spark.databricks.delta.po.metrics.enabled` | Boolean | `false` | Enable/disable the hook |
| `spark.databricks.delta.po.metrics.endpoint` | String | (none) | UC endpoint URL |
| `spark.databricks.delta.po.metrics.authToken` | String | (none) | Bearer token; falls back to `DATABRICKS_TOKEN` env var |
| `spark.databricks.delta.po.metrics.timeoutMs` | Long | `5000` | HTTP timeout (ms) |

### Example

```scala
spark.conf.set("spark.databricks.delta.po.metrics.enabled", "true")
spark.conf.set("spark.databricks.delta.po.metrics.endpoint",
  "https://<workspace>.databricks.com/api/2.1/unity-catalog/delta/preview/metrics")
spark.conf.set("spark.databricks.delta.po.metrics.authToken", "dapi...")
// token can also come from DATABRICKS_TOKEN env var
```

---

## Hook Registration

Registered inside `catalogTable.foreach` in `OptimisticTransaction.scala` — the same block as
`UpdateCatalogHook`. This is intentional: the hook only makes sense for tables that have catalog
metadata (a prerequisite for being UC-managed).

```scala
catalogTable.foreach { ct =>
  registerPostCommitHook(UpdateCatalogFactory.getUpdateCatalogHook(ct, spark))
  if (spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_ENABLED)) {
    registerPostCommitHook(UpdatePOMetricsHook(Some(ct)))
  }
}
```

**Hook execution order**:
1. ChecksumHook
2. UpdateCatalogHook (inside catalogTable.foreach)
3. **UpdatePOMetricsHook** (inside catalogTable.foreach, if enabled)
4. CheckpointHook
5. IcebergConverterHook
6. HudiConverterHook

The hook has a secondary UC check inside `run()` via `isUCManagedTable()` as a defense-in-depth
guard even if somehow called without catalog metadata.

---

## UC Table Detection and Table ID Resolution

```scala
private def isUCManagedTable(deltaLog: DeltaLog, catalogTable: Option[CatalogTable]): Boolean = {
  if (deltaLog.tableId.isEmpty) return false
  catalogTable match {
    case Some(ct) =>
      ct.identifier.catalog.isDefined ||
      (ct.properties.get("provider").exists(_.toLowerCase(Locale.ROOT) == "delta") &&
        deltaLog.tableId.nonEmpty)
    case None => false
  }
}
```

### Table ID Resolution (post-implementation fix)

For PO reporting, the hook now resolves the outgoing `table_id` in this order:

1. `catalogTable.properties["io.unitycatalog.tableId"]` (Databricks-internal key)
2. `catalogTable.properties["ucTableId"]` (legacy Databricks-internal key)
3. `catalogTable.storage.properties["fs.unitycatalog.table.id"]`
   (open-source UCSingleCatalog key)
4. `deltaLog.tableId` fallback (Delta metadata ID)

This was added because the Delta metadata ID can diverge from the UC-registered table ID when the
first Delta commit is written by a non-DBR client. In that case, sending `deltaLog.tableId` can
lead to 404s from the PO metrics endpoint. Catalog metadata is the authoritative source when
present.

`DeltaLog.tableId` is a plain `String` (not `Option[String]`) and remains the fallback when no
catalog-derived UC ID is available.

---

## Error Handling

The hook is best-effort: no exception from `run()` ever propagates to the commit.

| Error type | Behavior |
|------------|----------|
| HTTP 4xx/5xx | Throw inside `sendMetrics`; caught by `run()`, logged as WARNING |
| Connection timeout | Same |
| Missing endpoint config | Early return with INFO log |
| Missing auth token | Exception; caught, logged as WARNING |
| JSON serialization failure | Caught, logged as WARNING |

---

## Implementation Notes (Lessons Learned)

### `spark.conf.get(key: String)` returns `String`, not `Option[String]`

For optional config entries, use `spark.conf.getOption(key: String)` to get `Option[String]`.
Using `spark.conf.get(key)` returns a raw `String` and throws if unset — do not do:

```scala
// WRONG
spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key) match {
  case Some(url) => ...  // compile error: String is not Option

// CORRECT
spark.conf.getOption(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key) match {
  case Some(url) if url.nonEmpty => ...
```

### `DeltaLog.tableId` is `String`, not `Option[String]`

```scala
// WRONG
deltaLog.tableId.getOrElse { throw ... }
deltaLog.tableId.isDefined

// CORRECT
if (deltaLog.tableId.isEmpty) throw ...
deltaLog.tableId.nonEmpty
```

### `table_id` for PO payloads should prefer catalog UC ID over Delta metadata ID

For UC tables, use catalog metadata keys first (`io.unitycatalog.tableId`, `ucTableId`, and
`storage.properties["fs.unitycatalog.table.id"]`) before falling back to `DeltaLog.tableId`.
This avoids misreporting for tables whose first Delta commit came from non-DBR writers.

### Non-ASCII characters fail scalastyle

Use `-` instead of `—` (em dash) and `->` instead of `→` in all Scala source comments.
The scalastyle `nonascii.message` rule rejects any non-ASCII characters in source files.

### Integration tests need UC-managed table context

Writing to a temp path (`spark.range(n).write.format("delta").save(path)`) does not produce a
`CatalogTable`, so the hook is never registered for such tables. Integration tests for the
end-to-end hook flow require a real catalog table. To test the HTTP client in isolation, call
`POMetricsClient.sendMetrics` directly with a `SimpleMockServer`.

---

## Test Suite

**Location**: `spark-unified/src/test/scala/.../hooks/UpdatePOMetricsHookSuite.scala`

| Test | What it covers |
|------|---------------|
| `extractRowsInserted: prefers operationMetrics, falls back to file stats` | MERGE/WRITE/fallback paths for inserted rows |
| `extractRowsRemoved: prefers operationMetrics, falls back to file stats` | MERGE/DELETE/fallback paths for removed rows |
| `extractRowsUpdated: uses operationMetrics only, no file-level fallback` | MERGE/UPDATE paths; confirms no file fallback |
| `resolveTableId: prefers UC table ID keys over deltaLog.tableId` | Verifies precedence across internal, legacy, OSS UCSingleCatalog, and fallback paths |
| `buildFileSizeHistogram: distributes files into correct bins` | Histogram bin placement and commit_version |
| `JSON payload validation - matches server contract (snake_case, nested)` | snake_case fields, nested structure, no camelCase leaks |
| `JSON payload: optional fields are omitted when None` | None fields absent from JSON; clustered_bytes_removed never appears |
| `hook disabled by config - skips execution` | No error when endpoint not configured and hook disabled |
| `POMetricsClient: throws RuntimeException on HTTP 5xx response` | Client throws on error, request count verified |
| `POMetricsClient: sends correct Authorization header and JSON body` | Bearer token header, snake_case JSON body |

Run with:
```bash
build/sbt 'spark/testOnly *UpdatePOMetricsHookSuite'
```

---

## Dependencies

No new dependencies. Uses:
- **Apache HttpClient** — already in `storage` module
- **Jackson** — already used via `JsonUtils`
- **`FileSizeHistogram`** — already in `delta/spark` (`stats` package)

---

## Code Quality

All files pass:
- `build/sbt scalastyle` — 0 errors
- `build/sbt scalafmtCheckAll` — 0 errors

---

## Deploying to Databricks

### Build the JAR

```bash
build/sbt "spark/package"
```

Output: `spark-unified/target/scala-2.13/delta-spark_2.13-4.1.0-SNAPSHOT.jar`

### Cluster Spark Config

Upload the JAR to DBFS or a cluster-accessible path, then set:

```
spark.driver.extraClassPath   /path/to/delta-spark_2.13-4.1.0-SNAPSHOT.jar
spark.executor.extraClassPath /path/to/delta-spark_2.13-4.1.0-SNAPSHOT.jar

spark.databricks.delta.po.metrics.enabled   true
spark.databricks.delta.po.metrics.endpoint  https://<workspace>.databricks.com/api/2.1/unity-catalog/delta/preview/metrics
spark.databricks.delta.po.metrics.authToken dapi...
```

The `authToken` can also be omitted if the `DATABRICKS_TOKEN` environment variable is set on the
cluster.

### Sanity Check Before Full Rollout

Verify the endpoint is reachable and accepts payloads before attaching the hook to live traffic:

```scala
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.hooks.{CommitReport, CommitReportEnvelope,
  POMetricsClient, ReportDeltaMetricsRequest}

// Get the real table UUID from DeltaLog
val tableId = DeltaLog.forTable(spark, "catalog.schema.table").tableId
println(s"table_id = $tableId")

// Send a minimal test payload directly - bypasses the hook guard logic
val req = ReportDeltaMetricsRequest(
  tableId = tableId,
  report  = CommitReportEnvelope(CommitReport(numFilesAdded = Some(0L))))
POMetricsClient.sendMetrics(spark, req)
// If this returns without throwing, the endpoint is reachable and auth is correct
```

---

## Future Work

- **Phase 1.5**: Fixed-schedule heuristics when metrics are missing from PO's view
- **Phase 2**: Capture scan metrics from external reads
- **num_clustered_bytes_removed**: Would require the Delta protocol to propagate
  `clusteringProvider` onto `RemoveFile` (currently missing by design)
