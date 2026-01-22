# Delta Lake FGAC Changes

This document tracks all changes made to Delta Lake for FGAC (Fine-Grained Access Control) support with Unity Catalog server-side planning.

## Summary

Modified Delta Lake to support querying Unity Catalog tables with row filters and column masks via server-side planning. When credentials fail for FGAC tables, Delta automatically triggers server-side planning where UC applies filters/masks and returns pre-filtered Parquet files.

## Files Modified

### 1. build.sbt
**Location:** `/home/murali.ramanujam/fgac_bugbash_prep/delta/build.sbt`

**Change:** Added server-side planning package to iceberg assembly include list

**Lines 1105-1117:**
```scala
val deltaIcebergSparkIncludePrefixes = Seq(
  // We want everything from this package
  "org/apache/spark/sql/delta/icebergShaded",

  // Server-side planning client for FGAC support
  "org/apache/spark/sql/delta/serverSidePlanning",

  // We only want the files in this project from this package...
  "org/apache/spark/sql/delta/commands/convert/IcebergFileManifest",
  "org/apache/spark/sql/delta/commands/convert/IcebergSchemaUtils",
  "org/apache/spark/sql/delta/commands/convert/IcebergTable"
)
```

**Why:** The delta-iceberg assembly was excluding server-side planning classes. This ensures `IcebergRESTCatalogPlanningClient` and related classes are included in the JAR.

---

### 2. spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/ServerSidePlanningClient.scala
**Location:** `/home/murali.ramanujam/fgac_bugbash_prep/delta/spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/ServerSidePlanningClient.scala`

**Change:** Added ServiceLoader auto-registration block

**Lines added to `ServerSidePlanningClientFactory` object:**
```scala
private[serverSidePlanning] object ServerSidePlanningClientFactory {
  @volatile private var registeredFactory: Option[ServerSidePlanningClientFactory] = None

  // ServiceLoader auto-registration block
  {
    import java.util.ServiceLoader
    import scala.jdk.CollectionConverters._

    try {
      val loader = ServiceLoader.load(
        classOf[ServerSidePlanningClientFactory],
        Thread.currentThread().getContextClassLoader)

      val factories = loader.iterator().asScala.toList

      if (factories.nonEmpty) {
        registeredFactory = Some(factories.head)

        if (factories.size > 1) {
          // scalastyle:off println
          System.err.println(
            s"[Delta] Warning: Multiple ServerSidePlanningClientFactory implementations found. " +
            s"Using ${factories.head.getClass.getName}. " +
            s"Others: ${factories.tail.map(_.getClass.getName).mkString(", ")}")
          // scalastyle:on println
        }
      }
    } catch {
      case e: Exception =>
        // scalastyle:off println
        System.err.println(
          s"[Delta] Warning: Failed to auto-discover server-side planning factory: ${e.getMessage}")
        // scalastyle:on println
    }
  }

  def isFactoryRegistered(): Boolean = registeredFactory.isDefined
  def getFactoryInfo(): Option[String] = registeredFactory.map(_.getClass.getName)
}
```

**Why:** Enables automatic factory discovery via Java ServiceLoader, eliminating need for manual registration in spark-sql CLI.

---

### 3. iceberg/src/main/resources/META-INF/services/org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlanningClientFactory
**Location:** `/home/murali.ramanujam/fgac_bugbash_prep/delta/iceberg/src/main/resources/META-INF/services/org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlanningClientFactory`

**Change:** Created ServiceLoader declaration file

**Content:**
```
org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClientFactory
```

**Why:** ServiceLoader uses this file to discover the factory implementation at runtime.

---

### 4. iceberg/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/IcebergRESTCatalogPlanningClient.scala
**Location:** `/home/murali.ramanujam/fgac_bugbash_prep/delta/iceberg/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/IcebergRESTCatalogPlanningClient.scala`

**Change:** Added `?implementation=MATERIALIZED_PARQUET` query parameter to plan endpoint

**Lines 195-196:**
```scala
val planTableScanUri = s"$icebergRestCatalogUriRoot/v1/namespaces/$database/tables/$table" +
  "/plan?implementation=MATERIALIZED_PARQUET"
```

**Why:** Unity Catalog requires this query parameter to return pre-filtered Parquet files instead of JSON. Without it, the server returns 404.

---

### 5. spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/UnityCatalogMetadata.scala
**Location:** `/home/murali.ramanujam/fgac_bugbash_prep/delta/spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/UnityCatalogMetadata.scala`

**Change 1:** Fixed catalog name extraction to use current session catalog

**Lines 166-173:**
```scala
// Get the catalog name from the identifier if it's a 3-part name (catalog.schema.table),
// otherwise use the current catalog from the session
val catalogName = if (ident.namespace().length > 1) {
  ident.namespace().head
} else {
  // Use current catalog from session instead of defaulting to "spark_catalog"
  spark.sessionState.catalogManager.currentCatalog.name()
}
```

**Why:** Original code defaulted to "spark_catalog" for 2-part names, causing empty URI lookups. Now correctly uses the active catalog from the session.

**Change 2:** Added dual URI path handling for Unity Catalog

**Lines 73-78 (in fetchCatalogConfig):**
```scala
// Check if base already contains /api/2.1/unity-catalog
val icebergRestBase = if (baseUri.contains("/api/2.1/unity-catalog")) {
  s"$baseUri/iceberg-rest"
} else {
  s"$baseUri/api/2.1/unity-catalog/iceberg-rest"
}
```

**Lines 132-137 (in constructPlanEndpoint):**
```scala
// Check if base already contains /api/2.1/unity-catalog
val icebergRestBase = if (base.contains("/api/2.1/unity-catalog")) {
  s"$base/iceberg-rest"
} else {
  s"$base/api/2.1/unity-catalog/iceberg-rest"
}
```

**Why:** Handles both URI formats:
- Hostname only: `https://host` → adds `/api/2.1/unity-catalog/iceberg-rest`
- Full path: `https://host/api/2.1/unity-catalog` → adds `/iceberg-rest`

Prevents path duplication like `/unity-catalog/api/2.1/unity-catalog`.

**Change 3:** Added catalog name fallback for endpoint construction

**Lines 142-147:**
```scala
val endpoint = prefix match {
  case Some(p) => s"$icebergRestBase/v1/$p"
  case None =>
    // Fallback: construct prefix from catalog name
    s"$icebergRestBase/v1/catalogs/$catalogName"
}
```

**Why:** When `/v1/config` call fails, uses catalog name to construct endpoint path.

**Change 4:** Added debug logging

Added `System.err.println` statements throughout to trace:
- Config endpoint calls and responses
- Constructed endpoint URIs
- Catalog name resolution

**Why:** Helps debug endpoint construction and server communication issues.

---

## Build Commands

All modules rebuilt and published to local Ivy cache:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Unity Catalog (already built earlier in session)
cd /home/murali.ramanujam/td-sparkshell/unitycatalog
./build/sbt spark/publishLocal
./build/sbt client/publishLocal

# Delta Storage
cd /home/murali.ramanujam/fgac_bugbash_prep/delta
./build/sbt storage/publishLocal

# Delta Spark
./build/sbt spark/publishLocal

# Delta Iceberg (rebuilt multiple times with fixes)
./build/sbt clean icebergShaded/compile iceberg/compile iceberg/publishLocal
```

---

## Published Artifacts

All JARs published to `~/.ivy2/local/`:

- `io.unitycatalog/unitycatalog-spark_2.13/0.3.0-SNAPSHOT`
- `io.unitycatalog/unitycatalog-client/0.3.0-SNAPSHOT`
- `io.delta/delta-storage/4.1.0-SNAPSHOT`
- `io.delta/delta-spark_2.13/4.1.0-SNAPSHOT`
- `io.delta/delta-iceberg_2.13/4.1.0-SNAPSHOT`

---

## Spark Configuration

Required spark-sql launch command:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export UC_TOKEN="your_databricks_token"
export UC_URI="https://e2-dogfood.staging.cloud.databricks.com"
export UC_CATALOG="migration_bugbash"

./bin/spark-sql \
  --jars ~/.ivy2/local/io.delta/delta-spark_2.13/4.1.0-SNAPSHOT/jars/delta-spark_2.13.jar,\
~/.ivy2/local/io.delta/delta-iceberg_2.13/4.1.0-SNAPSHOT/jars/delta-iceberg_2.13.jar,\
~/.ivy2/local/io.delta/delta-storage/4.1.0-SNAPSHOT/jars/delta-storage.jar,\
~/.ivy2/local/io.unitycatalog/unitycatalog-spark_2.13/0.3.0-SNAPSHOT/jars/unitycatalog-spark_2.13.jar,\
~/.ivy2/local/io.unitycatalog/unitycatalog-client/0.3.0-SNAPSHOT/jars/unitycatalog-client.jar,\
jars/hadoop-aws-3.4.2.jar,\
jars/aws-java-sdk-bundle-1.12.770.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.$UC_CATALOG=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.$UC_CATALOG.uri=$UC_URI \
  --conf spark.sql.catalog.$UC_CATALOG.token=$UC_TOKEN \
  --conf spark.sql.defaultCatalog=$UC_CATALOG \
  --conf spark.databricks.delta.catalog.enableServerSidePlanning=true
```

**Key configuration:**
- `spark.databricks.delta.catalog.enableServerSidePlanning=true` - **REQUIRED** to enable server-side planning
- `UC_URI` - Use hostname only (no path), UC client adds `/api/2.1/unity-catalog` automatically
- AWS JARs (`hadoop-aws`, `aws-java-sdk-bundle`) - Required for S3 access to materialized Parquet files

---

## Current Status

### Working:
✅ Unity Catalog FGAC credential handling (returns None instead of throwing)
✅ Server-side planning detection and triggering
✅ ServiceLoader auto-discovery of factory
✅ Catalog name resolution from session
✅ Dual URI path handling (hostname vs full path)
✅ MATERIALIZED_PARQUET query parameter

### Issues:
❌ 404 error when calling plan endpoint:
```
https://e2-dogfood.staging.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/migration_bugbash/namespaces/philipzhu2_rlscm/tables/rls_cm_1211/plan?implementation=MATERIALIZED_PARQUET
```

The endpoint construction appears correct based on Iceberg REST spec, but UC returns 404. This suggests:
1. UC may not implement the Iceberg REST catalog spec's path structure
2. The catalog name prefix might be wrong
3. UC may use a different endpoint structure for FGAC/server-side planning

---

## Debug Output Example

When working correctly, you should see:
```
[FGAC-UC] loadTable called for tableId: xxx (FGAC-ENABLED UC VERSION)
[FGAC-UC] Attempting READ_WRITE credentials...
[FGAC-UC] READ_WRITE credentials failed: ...ROW_COLUMN_ACCESS_POLICIES...
[FGAC-UC] Returning None - will use server-side planning
[UC-SSP] Calling config endpoint: https://...iceberg-rest/v1/config
[UC-SSP] Config endpoint status: 400
[UC-SSP] Constructed endpoint: https://...iceberg-rest/v1/catalogs/migration_bugbash
```

---

## Next Steps

Need to determine correct Unity Catalog plan endpoint structure. Possibilities:
1. Check if UC uses a different path (not `/v1/catalogs/{catalog}`)
2. Verify if catalog name should be in query param instead of path
3. Check if UC requires different authentication for plan endpoint
4. Consider if UC's FGAC implementation uses a custom endpoint (not standard Iceberg REST)
