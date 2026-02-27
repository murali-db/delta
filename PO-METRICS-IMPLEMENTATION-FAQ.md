# PO Metrics: Implementation FAQ and Validation Notes

This document is the implementation companion to `PO-METRICS-PRODUCTIONIZATION-PLAN.md`.

It captures:

- what was validated manually,
- code walkthrough,
- key behavior Q&A,
- known environment caveats, and
- field-by-field metric mapping for the JSON payload.

---

## What I Have Done So Far

### 1) Manual POST via `curl`

- Sent `POST /api/2.1/unity-catalog/delta/preview/metrics` manually.
- Observed `200 OK` after fixing auth and payload shape.
- Verified DBConnect-side behavior reflected expected updates.

### 2) Built and tested with local jar

- Compiled Delta changes into a jar.
- Used a Spark client with that jar to insert rows into a UC-managed table.
- Observed end-to-end flow:
  - commit -> POST -> `200 OK`.
- Re-checked via DBConnect.

---

## Walk Through the Code

### Main pieces

- `spark/src/main/scala/org/apache/spark/sql/delta/hooks/UpdatePOMetricsHook.scala`
  - Post-commit hook entrypoint.
  - Feature-flag gate and UC-managed table gate.
  - Table ID resolution precedence.
  - Payload build (`buildRequest`) from committed actions.
  - Best-effort send through `POMetricsClient`.

- `spark/src/main/scala/org/apache/spark/sql/delta/hooks/POMetricsClient.scala`
  - JSON request model classes.
  - HTTP client for endpoint call.
  - Uses `Authorization: Bearer <token>`.
  - Reads endpoint/auth/timeout from Spark conf (token has env fallback).
  - Throws on non-2xx, which hook catches and logs.

- `spark/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala`
  - Registers `UpdatePOMetricsHook(Some(ct))` in post-commit hooks when enabled.

- `spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala`
  - auth token sourced from `spark.sql.catalog.<catalog>.token`
  - endpoint derived from `spark.sql.catalog.migration_bugbash.uri`
  - client timeout hardcoded to `5000` ms

---

## Questions and Answers (Validated)

### 1) Boolean flag on client side?

No in this prototype. The hook is always registered for catalog-backed transactions and attempts to
send for UC-managed tables.

### 2) Configuring endpoint URI?

Set only the base UC URI:

`spark.sql.catalog.migration_bugbash.uri=https://<workspace-host>`

The client appends:

`/api/2.1/unity-catalog/delta/preview/metrics`

### 3) Missing/unavailable fields?

- `num_clustered_bytes_removed` is not derivable from commit actions in OSS Delta because
  `RemoveFile` does not carry `clusteringProvider`.
- Current client intentionally omits it.
- Server supports it as optional.

### 4) If 200 OK do nothing. If not what?

- `2xx`: success path, no special action.
- non-2xx / timeout / exception:
  - `POMetricsClient` throws,
  - hook catches and logs warning,
  - commit still succeeds (best-effort behavior).

#### Client-side timeout

- Hardcoded to `5000` ms in this prototype.

### 5) Tests

- Unit tests: present.
- Smoke test with local mock server: present.
- Integration test: manual validation done via DBConnect/jar flow.
- Additional automated integration: future work if stable staging harness is available.

### 6) Without CCv2 (`delta.feature.catalogManaged = supported`) issue

Observed error:

```json
{"message":"Could not find commit version from UC table info","type":"BadRequestException","code":400}
```

Client-side surfaced:

```text
java.lang.RuntimeException: PO metrics endpoint returned error status 400: {"error":{"message":"Could not find commit version from UC table info","type":"BadRequestException","code":400}}
```

Interpretation:

- Server requires `latestTableVersion` from UC coordinated commits info for staleness validation.
- In environments where UC cannot provide this version, validation fails.
- Hook behavior remains correct (warn and continue); endpoint acceptance depends on server-side
  table metadata readiness.

---

## Metrics Sent in JSON: Source and Purpose

| JSON field | Sent by current client? | How value is derived | Purpose in PO/backend |
|---|---|---|---|
| `table_id` | Yes | `resolveTableId()` precedence: UC catalog keys first, then `deltaLog.tableId` fallback | Identifies target UC table for validation/routing |
| `report.commit_report.num_files_added` | Yes | `addFiles.size` | File churn added by commit |
| `report.commit_report.num_files_removed` | Yes | `removeFiles.size` | File churn removed by commit |
| `report.commit_report.num_bytes_added` | Yes | `sum(AddFile.size)` | Added write volume |
| `report.commit_report.num_bytes_removed` | Yes | `sum(RemoveFile.size if present)` | Removed/rewrite volume |
| `report.commit_report.num_clustered_bytes_added` | Yes | `sum(AddFile.size where clusteringProvider.isDefined)` | Clustered (liquid) write volume |
| `report.commit_report.num_clustered_bytes_removed` | No (intentional) | Not reliably derivable from `RemoveFile` in OSS Delta | Optional server field; intentionally omitted here |
| `report.commit_report.num_rows_inserted` | Yes (when available) | Prefer operation metrics (`numTargetRowsInserted`, `numOutputRows`), else `sum(AddFile.numLogicalRecords)` | Insert row workload |
| `report.commit_report.num_rows_removed` | Yes (when available) | Prefer operation metrics (`numTargetRowsDeleted`, `numDeletedRows`), else `sum(RemoveFile.numLogicalRecords)` | Delete row workload |
| `report.commit_report.num_rows_updated` | Yes (when available) | Operation metrics only (`numTargetRowsUpdated`, `numUpdatedRows`) | Update row workload |
| `report.commit_report.file_size_histogram.sorted_bin_boundaries` | Yes | Fixed file-size boundaries (0, 8KB, 64KB, ..., 1GB) | Histogram bucket definitions |
| `report.commit_report.file_size_histogram.file_counts` | Yes | Bucketized counts from `AddFile.size` | Added file distribution by size |
| `report.commit_report.file_size_histogram.total_bytes` | Yes | Bucketized bytes from `AddFile.size` | Added byte distribution by size |
| `report.commit_report.file_size_histogram.commit_version` | Yes | `txn.committedVersion` | Server staleness guard against UC latest version |

Notes:

- Fields are optional in payload classes and omitted when `None`.
- Server validates numeric fields are non-negative when present.
