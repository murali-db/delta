/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
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
   * - Requires enableServerSidePlanning flag to be enabled (prevents accidental enablement)
   * - In production: Also requires Unity Catalog table that lacks credentials
   * - In test mode: Only requires the enable flag (allows testing without UC setup)
   * - Otherwise use normal table loading path
   *
   * The logic is: ((isUnityCatalog && !hasCredentials) || skipUCRequirementForTests) && enableFlag
   *
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @param hasCredentials Whether the table has credentials available
   * @param enableServerSidePlanning Whether to enable server-side planning (config flag)
   * @param skipUCRequirementForTests Whether to skip Unity Catalog requirement for testing
   *                                   with non-UC tables
   * @return true if server-side planning should be used
   */
  private[serverSidePlanning] def shouldUseServerSidePlanning(
      isUnityCatalog: Boolean,
      hasCredentials: Boolean,
      enableServerSidePlanning: Boolean,
      skipUCRequirementForTests: Boolean): Boolean = {
    ((isUnityCatalog && !hasCredentials) || skipUCRequirementForTests) && enableServerSidePlanning
  }

  /**
   * Try to create a ServerSidePlannedTable if server-side planning is needed.
   * Returns None if not needed or if the planning client factory is not available.
   *
   * This method encapsulates all the logic to decide whether to use server-side planning:
   * - Checks if Unity Catalog table lacks credentials
   * - Checks if server-side planning is enabled via config (required for all cases)
   * - In test mode, Unity Catalog check is bypassed to allow testing
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
    // Check if we should enable server-side planning (for testing)
    val enableServerSidePlanning =
      spark.conf.get(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false").toBoolean
    val hasTableCredentials = hasCredentials(table)

    // Check if we should use server-side planning
    if (shouldUseServerSidePlanning(
        isUnityCatalog, hasTableCredentials, enableServerSidePlanning,
        skipUCRequirementForTests = DeltaUtils.isTesting)) {
      val namespace = ident.namespace().mkString(".")
      val tableName = ident.name()

      // Create metadata from table
      val metadata = ServerSidePlanningMetadata.fromTable(table, spark, ident, isUnityCatalog)

      // Try to create ServerSidePlannedTable with server-side planning
      val plannedTable = tryCreate(spark, namespace, tableName, table.schema(), metadata)
      if (plannedTable.isEmpty) {
        logWarning(
          s"Server-side planning not available for catalog ${metadata.catalogName}. " +
            "Falling back to normal table loading.")
      }
      plannedTable
    } else {
      None
    }
  }

  /**
   * Try to create a ServerSidePlannedTable with server-side planning.
   * Returns None if the planning client factory is not available.
   *
   * @param spark The SparkSession
   * @param databaseName The database name (may include catalog prefix)
   * @param tableName The table name
   * @param tableSchema The table schema
   * @param metadata Metadata extracted from loadTable response
   * @return Some(ServerSidePlannedTable) if successful, None if factory not registered
   */
  private def tryCreate(
      spark: SparkSession,
      databaseName: String,
      tableName: String,
      tableSchema: StructType,
      metadata: ServerSidePlanningMetadata): Option[ServerSidePlannedTable] = {
    try {
      val client = ServerSidePlanningClientFactory.buildClient(spark, metadata)
      Some(new ServerSidePlannedTable(spark, databaseName, tableName, tableSchema, client))
    } catch {
      case _: IllegalStateException =>
        // Factory not registered - this shouldn't happen in production but could during testing
        None
    }
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
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient)
    extends Table with SupportsRead with DeltaLogging {

  // Returns fully qualified name (e.g., "catalog.database.table").
  // The databaseName parameter receives ident.namespace().mkString(".") from DeltaCatalog,
  // which includes the catalog name when present, similar to DeltaTableV2's name() method.
  override def name(): String = s"$databaseName.$tableName"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ServerSidePlannedScanBuilder(spark, databaseName, tableName, tableSchema, planningClient)
  }
}

/**
 * ScanBuilder that uses ServerSidePlanningClient to plan the scan.
 * Implements SupportsPushDownFilters to enable WHERE clause pushdown to the server.
 * Implements SupportsPushDownRequiredColumns to enable column pruning pushdown to the server.
 * Implements SupportsPushDownLimit to enable LIMIT pushdown to the server.
 */
class ServerSidePlannedScanBuilder(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with DeltaLogging {

  // Filters that have been pushed down and will be sent to the server
  private var _pushedFilters: Array[Filter] = Array.empty

  // Required schema (columns to read). Defaults to full table schema.
  private var _requiredSchema: StructType = tableSchema

  // Limit that has been pushed down. None means no limit.
  private var _limit: Option[Int] = None

  /**
   * Push filters to the server-side planning client.
   *
   * Strategy:
   * - If ALL filters convert to server's native format: Returns empty array (no residuals)
   *   This enables Spark to push down LIMIT in addition to filters
   * - If ANY filter fails conversion: Returns all filters as residuals
   *   This falls back to safety mode where Spark re-applies all filters locally
   *
   * The server receives converted filters in both cases, but residuals provide a safety net
   * for correctness if the server silently ignores unsupported filters.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Store filters to send to IRC server
    _pushedFilters = filters

    // Strategy: Check if all filters can be converted upfront
    // Case 1: ALL convert -> return empty residuals -> enables filter+limit pushdown
    // Case 2: ANY fails -> return all residuals -> only filter pushdown (safety mode)

    if (filters.isEmpty) {
      // No filters to push
      return Array.empty
    }

    // Check if all filters are convertible
    val allConvertible = planningClient.canConvertFilters(filters)

    if (allConvertible) {
      // All filters successfully converted to server's native format
      // Trust that the server can handle them - return no residuals
      // This enables Spark to call pushLimit() for combined filter+limit pushdown
      logInfo(s"All ${filters.length} filters convertible, " +
              "returning empty residuals to enable limit pushdown")
      Array.empty
    } else {
      // At least one filter failed to convert
      // Return all filters as residuals for safety (Spark will re-apply)
      // Note: Server will still receive converted filters, but Spark provides safety net
      logWarning(s"Some filters failed to convert, " +
                 "returning all as residuals (limit pushdown disabled)")
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    _requiredSchema = requiredSchema
  }

  override def pushLimit(limit: Int): Boolean = {
    _limit = Some(limit)
    true
  }

  override def isPartiallyPushed(): Boolean = {
    // Return true if we have a limit - indicates partial pushdown so Spark applies it too
    _limit.isDefined
  }

  override def build(): Scan = {
    new ServerSidePlannedScan(
      spark, databaseName, tableName, tableSchema, planningClient, _pushedFilters, _requiredSchema,
      _limit)
  }
}

/**
 * Scan implementation that calls the server-side planning API to get file list.
 */
class ServerSidePlannedScan(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    pushedFilters: Array[Filter],
    requiredSchema: StructType,
    limit: Option[Int]) extends Scan with Batch {

  override def readSchema(): StructType = requiredSchema

  override def toBatch: Batch = this

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

  // Only pass projection if columns are actually pruned (not SELECT *)
  // Extract field names for planning client (server only needs names, not types)
  private val projectionColumnNames: Option[Seq[String]] = {
    if (requiredSchema.fieldNames.toSet == tableSchema.fieldNames.toSet) {
      None
    } else {
      Some(requiredSchema.fieldNames.toSeq)
    }
  }

  // Call the server-side planning API to get the scan plan with files AND credentials
  private val scanPlan: ScanPlan = planningClient.planScan(
    databaseName,
    tableName,
    combinedFilter,
    projectionColumnNames,
    limit)

  override def planInputPartitions(): Array[InputPartition] = {
    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      ServerSidePlannedFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ServerSidePlannedFilePartitionReaderFactory(
      spark, tableSchema, requiredSchema, scanPlan.credentials)
  }
}

/**
 * InputPartition representing a single file from the server-side scan plan.
 */
case class ServerSidePlannedFileInputPartition(
    filePath: String,
    fileSizeInBytes: Long,
    fileFormat: String,
    credentialId: Option[String] = None) extends InputPartition

/**
 * Factory for creating PartitionReaders that read server-side planned files.
 * Builds reader functions on the driver for Parquet files.
 *
 * @param tableSchema The full table schema (all columns in the file)
 * @param requiredSchema The required schema (columns to read after projection pushdown)
 * @param credentials Optional storage credentials from server-side planning response
 */
class ServerSidePlannedFilePartitionReaderFactory(
    spark: SparkSession,
    tableSchema: StructType,
    requiredSchema: StructType,
    credentials: Option[ScanPlanStorageCredentials])
    extends PartitionReaderFactory {

  import org.apache.spark.util.SerializableConfiguration

  // Store credential ID for thread-local setting in readers
  // NOT transient - needs to be serialized and sent to executors
  private var s3CredentialId: Option[String] = None

  // scalastyle:off deltahadoopconfiguration
  // We use sessionState.newHadoopConf() here instead of deltaLog.newDeltaHadoopConf().
  // This means DataFrame options (like custom S3 credentials) passed by users will NOT be
  // included in the Hadoop configuration. This is intentional:
  // - Server-side planning uses server-provided credentials, not user-specified credentials
  // - ServerSidePlannedTable is NOT a Delta table, so we don't want Delta-specific options
  //   from deltaLog.newDeltaHadoopConf()
  // - General Spark options from spark.hadoop.* are included and work for all tables
  private val hadoopConf = {
    // scalastyle:off println
    System.err.println(s"[UC-SSP] Creating hadoopConf in ReaderFactory " +
      s"(Thread: ${Thread.currentThread().getName})")
    // scalastyle:on println

    val conf = spark.sessionState.newHadoopConf()

    // Disable S3A FileSystem caching to prevent credential conflicts in joins
    // and successive queries. Each scan will have a unique credential ID in the
    // configuration, which will force Hadoop to create a new FS instance.
    // Combined with DynamicAwsCredentialsProvider that never caches credentials,
    // this ensures fresh credentials are always used from the registry.
    conf.set("fs.s3a.impl.disable.cache", "true")

    // scalastyle:off println
    System.err.println(s"[UC-SSP] Disabled S3A FileSystem caching (fs.s3a.impl.disable.cache=true)")
    // scalastyle:on println

    // Inject temporary credentials from IRC server response
    credentials.foreach { creds =>
      creds match {
        case S3Credentials(accessKeyId, secretAccessKey, sessionToken) =>
          // Register credentials in global registry and get unique UUID
          // This UUID will be different for each scan, forcing Hadoop to create
          // new FileSystem instances even when fs.s3a.impl.disable.cache is true
          val credentialId = CredentialRegistry.register(
            accessKeyId = accessKeyId,
            secretAccessKey = secretAccessKey,
            sessionToken = sessionToken,
            ttlMs = 60 * 60 * 1000L  // 1 hour TTL
          )

          // Store credential ID for thread-local setting in partition readers
          s3CredentialId = Some(credentialId)

          // Configure S3A to use our dynamic credentials provider
          // This provider reads from the registry on EVERY S3 API call,
          // ensuring credentials are never cached within the AWS SDK
          conf.set("fs.s3a.aws.credentials.provider",
            "org.apache.spark.sql.delta.serverSidePlanning.DynamicAwsCredentialsProvider")

          // Set credential ID so the provider can look it up in the registry
          // This UUID becomes part of the Hadoop Configuration content,
          // which means different credentials get different cache keys
          conf.set("fs.s3a.credential.id", credentialId)

          // scalastyle:off println
          System.err.println(s"[UC-SSP] Registered credentials in registry:")
          System.err.println(s"[UC-SSP]   Credential ID = $credentialId")
          System.err.println(s"[UC-SSP]   Access Key = ${accessKeyId.take(8)}...")
          System.err.println(s"[UC-SSP]   Session Token = ${sessionToken.take(20)}...")
          System.err.println(s"[UC-SSP]   Credential Provider = DynamicAwsCredentialsProvider")
          // scalastyle:on println

        case AzureCredentials(accountName, sasToken, containerName) =>
          // Format: fs.azure.sas.<container>.<account>.dfs.core.windows.net
          val sasKey = s"fs.azure.sas.$containerName.$accountName.dfs.core.windows.net"
          // scalastyle:off println
          System.err.println(s"[UC-SSP] Injecting Azure credentials for container: $containerName")
          // scalastyle:on println
          conf.set(sasKey, sasToken)

        case GcsCredentials(oauth2Token) =>
          // scalastyle:off println
          System.err.println(s"[UC-SSP] Injecting GCS credentials")
          // scalastyle:on println
          conf.set("fs.gs.auth.access.token", oauth2Token)
      }
    }

    new SerializableConfiguration(conf)
  }
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader function for Parquet on the driver
  // This function will be serialized and sent to executors
  // tableSchema: All columns in the file (full table schema)
  // requiredSchema: Columns to actually read (after projection pushdown)
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = tableSchema,
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

    new ServerSidePlannedFilePartitionReader(filePartition, parquetReaderBuilder, s3CredentialId)
  }
}

/**
 * PartitionReader that reads a single file using a pre-built reader function.
 * The reader function was created on the driver and is executed on the executor.
 */
class ServerSidePlannedFilePartitionReader(
    partition: ServerSidePlannedFileInputPartition,
    readerBuilder: PartitionedFile => Iterator[InternalRow],
    credentialId: Option[String])
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
  // Set thread-local credential ID before calling reader (for Dynamic...Provider)
  private lazy val readerIterator: Iterator[InternalRow] = {
    credentialId.foreach { credId =>
      CredentialRegistry.setThreadLocalCredentialId(credId)
    }
    try {
      readerBuilder(partitionedFile)
    } finally {
      // Note: We don't clear thread-local here because the iterator might be lazy
      // and needs the credential ID when elements are actually read
    }
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
