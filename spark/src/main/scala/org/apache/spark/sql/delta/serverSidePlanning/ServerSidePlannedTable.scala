/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.serverSidePlanning

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Companion object for ServerSidePlannedTable with factory methods.
 */
object ServerSidePlannedTable extends DeltaLogging {
  /**
   * Property keys that indicate table credentials are available.
   * Unity Catalog tables may expose temporary credentials via these properties.
   */
  private val CREDENTIAL_PROPERTY_KEYS = Seq(
    "storage.credential",
    "aws.temporary.credentials",
    "azure.temporary.credentials",
    "gcs.temporary.credentials",
    "credential"
  )

  /**
   * Determine if server-side planning should be used based on catalog type,
   * credential availability, and configuration.
   *
   * Decision logic:
   * - Use server-side planning if forceServerSidePlanning is true (config override)
   * - Use server-side planning if Unity Catalog table lacks credentials
   * - Otherwise use normal table loading path
   *
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @param hasCredentials Whether the table has credentials available
   * @param forceServerSidePlanning Whether to force server-side planning (config flag)
   * @return true if server-side planning should be used
   */
  private[serverSidePlanning] def shouldUseServerSidePlanning(
      isUnityCatalog: Boolean,
      hasCredentials: Boolean,
      forceServerSidePlanning: Boolean): Boolean = {
    (isUnityCatalog && !hasCredentials) || forceServerSidePlanning
  }

  /**
   * Try to create a ServerSidePlannedTable if server-side planning is needed.
   * Returns None if not needed or if the planning client factory is not available.
   *
   * This method encapsulates all the logic to decide whether to use server-side planning:
   * - Checks if Unity Catalog table lacks credentials
   * - Checks if server-side planning is forced via config (for testing)
   * - Extracts catalog name and table identifiers
   * - Attempts to create the planning client
   *
   * Test coverage: ServerSidePlanningSuite tests verify the decision logic through
   * shouldUseServerSidePlanning() method with different input combinations.
   *
   * @param spark The SparkSession
   * @param ident The table identifier
   * @param table The loaded table from the delegate catalog
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @return Some(ServerSidePlannedTable) if server-side planning should be used, None otherwise
   */
  def tryCreate(
      spark: SparkSession,
      ident: Identifier,
      table: Table,
      isUnityCatalog: Boolean): Option[ServerSidePlannedTable] = {
    // Check if we should force server-side planning (for testing)
    val forceServerSidePlanning =
      spark.conf.get(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false").toBoolean
    val hasTableCredentials = hasCredentials(table)

    // Check if we should use server-side planning
    if (shouldUseServerSidePlanning(isUnityCatalog, hasTableCredentials, forceServerSidePlanning)) {
      val namespace = ident.namespace().mkString(".")
      val tableName = ident.name()

      // Create metadata from table
      val metadata = ServerSidePlanningMetadata.fromTable(table, spark, ident, isUnityCatalog)

      // Try to create ServerSidePlannedTable with server-side planning
      create(spark, namespace, tableName, table.schema(), metadata) match {
        case Some(plannedTable) =>
          Some(plannedTable)
        case None =>
          // Factory not registered - fall through to normal path
          logWarning(
            s"Server-side planning not available for catalog ${metadata.catalogName}. " +
              "Falling back to normal table loading.")
          None
      }
    } else {
      None
    }
  }

  /**
   * Try to create a ServerSidePlannedTable with server-side planning.
   * Returns None if the planning client factory is not available.
   *
   * @param spark The SparkSession
   * @param database The database name (may include catalog prefix)
   * @param tableName The table name
   * @param tableSchema The table schema
   * @param metadata Metadata extracted from loadTable response
   * @return Some(ServerSidePlannedTable) if successful, None if factory not registered
   */
  private def create(
      spark: SparkSession,
      database: String,
      tableName: String,
      tableSchema: StructType,
      metadata: ServerSidePlanningMetadata): Option[ServerSidePlannedTable] = {
    try {
      val client = ServerSidePlanningClientFactory.buildClient(spark, metadata)
      Some(new ServerSidePlannedTable(spark, database, tableName, tableSchema, client, metadata))
    } catch {
      case _: IllegalStateException =>
        // Factory not registered - this shouldn't happen in production but could during testing
        None
    }
  }

  /**
   * Create a ServerSidePlannedTable with an explicit client for testing.
   *
   * This bypasses the normal production creation path (tryCreate -> create) which goes through
   * factory registration and metadata extraction. This lets tests validate the full credential
   * flow (IRC server -> client -> Hadoop config -> filesystem) without needing Unity Catalog or
   * going through DeltaCatalog.loadTable().
   *
   * @param spark The SparkSession
   * @param database The database name (may include catalog prefix)
   * @param tableName The table name
   * @param tableSchema The StructType
   * @param client The planning client to use
   * @param catalogName Catalog name for catalog-specific configuration keys
   *                    (default: spark_catalog)
   * @return ServerSidePlannedTable instance
   */
  def forTesting(
      spark: SparkSession,
      database: String,
      tableName: String,
      tableSchema: StructType,
      client: ServerSidePlanningClient,
      testCatalogName: String = "spark_catalog"): ServerSidePlannedTable = {
    // Create test metadata inline (can't import TestMetadata from test code)
    val metadata = new ServerSidePlanningMetadata {
      override def planningEndpointUri: String = ""
      override def authToken: Option[String] = None
      override def catalogName: String = testCatalogName
      override def tableProperties: Map[String, String] = Map.empty
    }
    new ServerSidePlannedTable(
      spark, database, tableName, tableSchema, client, metadata)
  }

  /**
   * Check if a table has credentials available.
   * Unity Catalog tables may lack credentials when accessed without proper permissions.
   * UC injects credentials as table properties, see:
   * https://github.com/unitycatalog/unitycatalog/blob/main/connectors/spark/src/main/scala/
   *   io/unitycatalog/spark/UCSingleCatalog.scala#L260
   */
  private def hasCredentials(table: Table): Boolean = {
    // Check table properties for credential information
    val properties = table.properties()
    CREDENTIAL_PROPERTY_KEYS.exists(key => properties.containsKey(key))
  }
}

/**
 * A Spark Table implementation that uses server-side scan planning
 * to get the list of files to read. Used as a fallback when Unity Catalog
 * doesn't provide credentials.
 *
 * Similar to DeltaTableV2, we accept SparkSession as a constructor parameter
 * since Tables are created on the driver and are not serialized to executors.
 */
class ServerSidePlannedTable(
    spark: SparkSession,
    database: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    metadata: ServerSidePlanningMetadata)
    extends Table with SupportsRead with DeltaLogging {

  // Returns fully qualified name (e.g., "catalog.database.table").
  // The database parameter receives ident.namespace().mkString(".") from DeltaCatalog,
  // which includes the catalog name when present, similar to DeltaTableV2's name() method.
  override def name(): String = s"$database.$tableName"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ServerSidePlannedScanBuilder(
      spark, database, tableName, tableSchema, planningClient, metadata)
  }
}

/**
 * ScanBuilder that uses ServerSidePlanningClient to plan the scan.
 * Implements SupportsPushDownFilters to enable WHERE clause pushdown to the server.
 * Implements SupportsPushDownRequiredColumns to enable column pruning (projection pushdown).
 */
class ServerSidePlannedScanBuilder(
    spark: SparkSession,
    database: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    metadata: ServerSidePlanningMetadata)
  extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  // Filters that have been pushed down and will be sent to the server
  private var _pushedFilters: Array[Filter] = Array.empty

  // Required schema (columns to read). Defaults to full table schema.
  private var _requiredSchema: StructType = tableSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Accept all filters for pushdown. The SparkToIcebergExpressionConverter will handle
    // filtering out unsupported filters when converting to Iceberg expressions.
    // Return empty array to indicate all filters were accepted.
    _pushedFilters = filters
    Array.empty
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    _requiredSchema = requiredSchema
  }

  override def build(): Scan = {
    new ServerSidePlannedScan(
      spark, database, tableName, tableSchema, planningClient, metadata,
      _pushedFilters, _requiredSchema)
  }
}

/**
 * Scan implementation that calls the server-side planning API to get file list.
 */
class ServerSidePlannedScan(
    spark: SparkSession,
    database: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    metadata: ServerSidePlanningMetadata,
    pushedFilters: Array[Filter],
    requiredSchema: StructType) extends Scan with Batch {

  override def readSchema(): StructType = requiredSchema

  override def toBatch: Batch = this

  // Call the server-side planning API once and store the result.
  // Convert pushed filters to a single Spark Filter for the API call.
  // If no filters, pass None. If filters exist, combine them into a single filter.
  private val combinedFilter: Option[Filter] = {
    if (pushedFilters.isEmpty) {
      None
    } else if (pushedFilters.length == 1) {
      Some(pushedFilters.head)
    } else {
      // Combine multiple filters with And
      Some(pushedFilters.reduce((left, right) => And(left, right)))
    }
  }

  // Pass projection to server if it differs from full table schema.
  // This enables the catalog to optimize file listing for large tables.
  private val projection: Option[StructType] = {
    if (requiredSchema == tableSchema) {
      None  // Full table scan, no projection needed
    } else {
      Some(requiredSchema)
    }
  }

  private val scanPlan = planningClient.planScan(database, tableName, combinedFilter, projection)

  override def planInputPartitions(): Array[InputPartition] = {
    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      ServerSidePlannedFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ServerSidePlannedFilePartitionReaderFactory(
      spark, tableSchema, requiredSchema, scanPlan.credentials, metadata)
  }
}

/**
 * InputPartition representing a single file from the server-side scan plan.
 */
case class ServerSidePlannedFileInputPartition(
    filePath: String,
    fileSizeInBytes: Long,
    fileFormat: String) extends InputPartition

/**
 * Factory for creating PartitionReaders that read server-side planned files.
 * Builds reader functions on the driver for Parquet files.
 *
 * @param dataSchema The full table schema (all columns in the file)
 * @param requiredSchema The required schema (columns to read after projection pushdown)
 * @param credentials Optional storage credentials from server-side planning response.
 *                    When present, these credentials are injected into the Hadoop configuration
 *                    for S3A file access.
 * @param metadata Metadata for credential refresh context (catalog name, UC URI/token)
 */
class ServerSidePlannedFilePartitionReaderFactory(
    spark: SparkSession,
    dataSchema: StructType,
    requiredSchema: StructType,
    credentials: Option[StorageCredentials],
    metadata: ServerSidePlanningMetadata)
    extends PartitionReaderFactory {

  import org.apache.spark.util.SerializableConfiguration

  // scalastyle:off deltahadoopconfiguration
  // We use sessionState.newHadoopConf() here instead of deltaLog.newDeltaHadoopConf().
  // This means DataFrame options (like custom S3 credentials) passed by users will NOT be
  // included in the Hadoop configuration. This would fail if users specify credentials in
  // DataFrame read options expecting them to be used when accessing the underlying files.
  // However, for now we accept this limitation to avoid requiring a DeltaLog parameter.
  private val hadoopConf = {
    val conf = spark.sessionState.newHadoopConf()

    // Inject temporary credentials from server-side planning response if present
    credentials.foreach { creds =>
      conf.set("fs.s3a.access.key", creds.accessKeyId)
      conf.set("fs.s3a.secret.key", creds.secretAccessKey)
      conf.set("fs.s3a.session.token", creds.sessionToken)
    }

    // Inject credential refresh context (catalog URI/token for UC, no-op for non-UC)
    // For UC: Sets spark.sql.catalog.$catalogName.uri and .token
    //
    // TODO: Implement credential refresh in cloud-specific filesystem implementations
    // (S3CredentialFileSystem, AzureCredentialFileSystem, GCSCredentialFileSystem).
    // These classes should:
    // 1. Detect when credentials are expiring (check token expiration time)
    // 2. Call table service endpoint (not UC) to refresh credentials
    // 3. Use catalogName/ucUri/ucToken from Hadoop config to authenticate refresh request
    // 4. Update Hadoop config with new credentials
    // This will enable long-running queries to continue after initial credentials expire.
    metadata.injectCredentialRefreshContext(conf)

    // After serialization, executors will have access to these credentials for S3 file reads

    new SerializableConfiguration(conf)
  }
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader function for Parquet on the driver
  // This function will be serialized and sent to executors
  // dataSchema: All columns in the file (full table schema)
  // requiredSchema: Columns to actually read (after projection pushdown)
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = dataSchema,
    partitionSchema = StructType(Nil),
    requiredSchema = requiredSchema,
    filters = Seq.empty,
    options = Map(
      FileFormat.OPTION_RETURNING_BATCH -> "false"
    ),
    hadoopConf = hadoopConf.value
  )

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val filePartition = partition.asInstanceOf[ServerSidePlannedFileInputPartition]

    // Verify file format is Parquet
    // Scalastyle suppression needed: the caselocale regex incorrectly flags even correct usage
    // of toLowerCase(Locale.ROOT). Similar to PartitionUtils.scala and SchemaUtils.scala.
    // scalastyle:off caselocale
    if (filePartition.fileFormat.toLowerCase(Locale.ROOT) != "parquet") {
    // scalastyle:on caselocale
      throw new UnsupportedOperationException(
        s"File format '${filePartition.fileFormat}' is not supported. Only Parquet is supported.")
    }

    new ServerSidePlannedFilePartitionReader(filePartition, parquetReaderBuilder)
  }
}

/**
 * PartitionReader that reads a single file using a pre-built reader function.
 * The reader function was created on the driver and is executed on the executor.
 */
class ServerSidePlannedFilePartitionReader(
    partition: ServerSidePlannedFileInputPartition,
    readerBuilder: PartitionedFile => Iterator[InternalRow])
    extends PartitionReader[InternalRow] {

  // Create PartitionedFile for this file
  private val partitionedFile = PartitionedFile(
    partitionValues = InternalRow.empty,
    filePath = SparkPath.fromPathString(partition.filePath),
    start = 0,
    length = partition.fileSizeInBytes
  )

  // Call the pre-built reader function with our PartitionedFile
  // This happens on the executor and doesn't need SparkSession
  private lazy val readerIterator: Iterator[InternalRow] = {
    readerBuilder(partitionedFile)
  }

  override def next(): Boolean = {
    readerIterator.hasNext
  }

  override def get(): InternalRow = {
    readerIterator.next()
  }

  override def close(): Unit = {
    // Reader cleanup is handled by Spark
  }
}
