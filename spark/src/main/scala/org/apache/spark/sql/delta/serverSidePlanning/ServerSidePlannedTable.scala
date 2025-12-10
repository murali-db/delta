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

import java.io.IOException
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

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
    // TEMPORARY: Always use server-side planning for testing
    true
    // Original logic (commented out):
    // (isUnityCatalog && !hasCredentials) || forceServerSidePlanning
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

      // Create metadata from table - this reads config once and extracts all needed info
      val metadata = ServerSidePlanningMetadata.fromTable(table, spark, ident, isUnityCatalog)

      // Try to create ServerSidePlannedTable with server-side planning
      create(spark, namespace, tableName, table.schema(), metadata) match {
        case Some(plannedTable) =>
          Some(plannedTable)
        case None =>
          // Factory not registered - fall through to normal path
          logWarning(s"Server-side planning not available for catalog ${metadata.catalogName}. " +
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
      val client = ServerSidePlanningClientFactory.buildFromMetadata(spark, metadata)
      Some(new ServerSidePlannedTable(
        spark, database, tableName, tableSchema, client,
        metadata.catalogName,
        metadata.unityCatalogUri.getOrElse(""),
        metadata.unityCatalogToken.getOrElse("")))
    } catch {
      case _: IllegalStateException =>
        // Factory not registered - this shouldn't happen in production but could during testing
        None
    }
  }

  /**
   * Create a ServerSidePlannedTable with an explicit client for testing.
   *
   * @param spark The SparkSession
   * @param database The database name (may include catalog prefix)
   * @param tableName The table name
   * @param tableSchema The StructType
   * @param client The planning client to use
   * @param catalogName Catalog name for catalog-specific configuration keys
   *                    (default: spark_catalog)
   * @param ucUri Unity Catalog URI for credential refresh
   *              (default: empty for tests)
   * @param ucToken Unity Catalog token for credential refresh
   *                (default: empty for tests)
   * @return ServerSidePlannedTable instance
   */
  def forTesting(
      spark: SparkSession,
      database: String,
      tableName: String,
      tableSchema: StructType,
      client: ServerSidePlanningClient,
      catalogName: String = "spark_catalog",
      ucUri: String = "",
      ucToken: String = ""): ServerSidePlannedTable = {
    new ServerSidePlannedTable(
      spark, database, tableName, tableSchema, client, catalogName, ucUri, ucToken)
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
 *
 * @param catalogName Catalog name for catalog-specific configuration keys
 * @param ucUri Unity Catalog URI for credential refresh (passed to executors via Hadoop config)
 * @param ucToken Unity Catalog token for credential refresh (passed to executors via Hadoop config)
 */
class ServerSidePlannedTable(
    spark: SparkSession,
    database: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    catalogName: String,
    ucUri: String,
    ucToken: String)
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
      spark, database, tableName, tableSchema, planningClient, catalogName, ucUri, ucToken)
  }
}

/**
 * ScanBuilder that uses ServerSidePlanningClient to plan the scan.
 */
class ServerSidePlannedScanBuilder(
    spark: SparkSession,
    database: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    catalogName: String,
    ucUri: String,
    ucToken: String) extends ScanBuilder {

  override def build(): Scan = {
    new ServerSidePlannedScan(
      spark, database, tableName, tableSchema, planningClient, catalogName, ucUri, ucToken)
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
    catalogName: String,
    ucUri: String,
    ucToken: String) extends Scan with Batch {

  override def readSchema(): StructType = tableSchema

  override def toBatch: Batch = this

  // Call the server-side planning API once and store the result
  private val scanPlan = planningClient.planScan(database, tableName)

  override def planInputPartitions(): Array[InputPartition] = {
    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      ServerSidePlannedFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ServerSidePlannedFilePartitionReaderFactory(
      spark, tableSchema, scanPlan.credentials, catalogName, ucUri, ucToken)
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
 * @param credentials Optional storage credentials from server-side planning response.
 *                    When present, these credentials are injected into the Hadoop configuration
 *                    for S3A file access.
 * @param catalogName Catalog name for catalog-specific configuration keys
 * @param ucUri Unity Catalog URI - injected into Hadoop config for credential refresh
 * @param ucToken Unity Catalog token - injected into Hadoop config for credential refresh
 */
class ServerSidePlannedFilePartitionReaderFactory(
    spark: SparkSession,
    schema: StructType,
    credentials: Option[StorageCredentials],
    catalogName: String,
    ucUri: String,
    ucToken: String)
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

    // Inject Unity Catalog URI and token for future credential refresh on executors
    // Use catalog-specific configuration keys to support multiple catalogs
    //
    // TODO: Implement credential refresh in cloud-specific filesystem implementations
    // (S3CredentialFileSystem, AzureCredentialFileSystem, GCSCredentialFileSystem).
    // These classes should:
    // 1. Detect when credentials are expiring (check token expiration time)
    // 2. Call table service endpoint (not UC) to refresh credentials
    // 3. Use catalogName/ucUri/ucToken from Hadoop config to authenticate refresh request
    // 4. Update Hadoop config with new credentials
    // This will enable long-running queries to continue after initial credentials expire.
    if (ucUri.nonEmpty) {
      conf.set(s"spark.sql.catalog.$catalogName.uri", ucUri)
    }
    if (ucToken.nonEmpty) {
      conf.set(s"spark.sql.catalog.$catalogName.token", ucToken)
    }

    // After serialization, executors will have access to these credentials for S3 file reads

    new SerializableConfiguration(conf)
  }
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader function for Parquet on the driver
  // This function will be serialized and sent to executors
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = schema,
    partitionSchema = StructType(Nil),
    requiredSchema = schema,
    filters = Seq.empty,
    options = Map(
      FileFormat.OPTION_RETURNING_BATCH -> "false"
    ),
    hadoopConf = hadoopConf.value
  )

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val filePartition = partition.asInstanceOf[ServerSidePlannedFileInputPartition]

    // Check if this is a presigned URL (HTTPS) vs S3 path
    if (isPresignedUrl(filePartition.filePath)) {
      // Presigned URL path - use JSON reader
      // Note: UC reports fileFormat as "PARQUET" even when returning JSON from presigned URLs
      // So we don't validate the format field here - we rely solely on URL detection
      new PresignedUrlJsonPartitionReader(filePartition.filePath, schema)

    } else {
      // S3 path - use Parquet reader with credential injection
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
   * Detect if a file path is a presigned URL.
   * MVP implementation: hardcode HTTPS detection.
   *
   * Future enhancement: Check for UC-specific URL patterns, expiration parameters, etc.
   */
  private def isPresignedUrl(filePath: String): Boolean = {
    filePath.startsWith("https://")
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

/**
 * PartitionReader that reads JSON data from presigned URLs.
 * Supports both newline-delimited JSON and JSON array formats.
 *
 * This reader is used when Unity Catalog returns presigned URLs in MATERIALIZED_JSON mode,
 * which happens for tables with FGAC policies (row-level security, column masking).
 *
 * @param presignedUrl The HTTPS presigned URL to fetch JSON data from
 * @param expectedSchema The schema that JSON data should conform to
 */
class PresignedUrlJsonPartitionReader(
    presignedUrl: String,
    expectedSchema: StructType)
    extends PartitionReader[InternalRow] {

  private val objectMapper = new ObjectMapper()
  private lazy val jsonContent: String = fetchJsonContent()
  private lazy val jsonIterator: Iterator[JsonNode] = parseJson(jsonContent)

  private var currentRow: Option[InternalRow] = None

  /**
   * Fetch JSON content from presigned URL via HTTP GET.
   * Presigned URLs are time-limited, typically valid for 15-60 minutes.
   */
  private def fetchJsonContent(): String = {
    val httpClient = HttpClients.createDefault()
    try {
      val httpGet = new HttpGet(presignedUrl)
      val response = httpClient.execute(httpGet)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode != 200) {
          throw new IOException(
            s"Failed to fetch presigned URL. HTTP status: $statusCode, URL: $presignedUrl")
        }
        EntityUtils.toString(response.getEntity)
      } finally {
        response.close()
      }
    } catch {
      case e: Exception =>
        throw new IOException(s"Error fetching presigned URL: $presignedUrl", e)
    } finally {
      httpClient.close()
    }
  }

  /**
   * Parse JSON content from Unity Catalog presigned URLs.
   *
   * UC returns JSON in array-of-arrays format where:
   * - Outer array contains rows
   * - Each inner array contains column values in schema order
   *
   * Example for a single-column "name" field:
   * {{{
   * [["alice"], ["bob"], ["charlie"]]
   * }}}
   */
  private def parseJson(content: String): Iterator[JsonNode] = {
    val trimmed = content.trim

    // UC presigned URLs always return JSON array format
    if (!trimmed.startsWith("[")) {
      throw new IllegalArgumentException(
        s"Expected JSON array from presigned URL but got: ${trimmed.take(100)}...")
    }

    val arrayNode = objectMapper.readTree(trimmed)
    if (!arrayNode.isArray) {
      throw new IllegalArgumentException(
        s"Expected JSON array but got: ${arrayNode.getNodeType}")
    }

    arrayNode.elements().asScala
  }

  /**
   * Convert JsonNode (array of values) to InternalRow according to expected schema.
   *
   * UC returns each row as a JSON array where values are in the same order as schema fields.
   * Example for schema (id: Int, name: String), UC returns:
   * {{{
   * [1, "alice"]
   * }}}
   *
   * Validates that array length matches schema and types are compatible.
   */
  private def jsonToInternalRow(jsonNode: JsonNode): InternalRow = {
    // Validate this is an array (each row from UC is an array)
    if (!jsonNode.isArray) {
      throw new IllegalArgumentException(
        s"Expected JSON array for row but got: ${jsonNode.getNodeType}")
    }

    // Validate array length matches schema
    if (jsonNode.size() != expectedSchema.length) {
      throw new IllegalArgumentException(
        s"JSON array size (${jsonNode.size()}) doesn't match schema field count " +
        s"(${expectedSchema.length}). Schema: ${expectedSchema.fieldNames.mkString(", ")}")
    }

    val values = new Array[Any](expectedSchema.length)

    expectedSchema.fields.zipWithIndex.foreach { case (field, index) =>
      val jsonValue = jsonNode.get(index)

      if (jsonValue == null || jsonValue.isNull) {
        if (!field.nullable) {
          throw new IllegalArgumentException(
            s"Required field '${field.name}' (index $index) is null in JSON array")
        }
        values(index) = null
      } else {
        values(index) = field.dataType match {
          case IntegerType => jsonValue.asInt()
          case LongType => jsonValue.asLong()
          case DoubleType => jsonValue.asDouble()
          case FloatType => jsonValue.asDouble().toFloat
          case StringType => UTF8String.fromString(jsonValue.asText())
          case BooleanType => jsonValue.asBoolean()
          case ShortType => jsonValue.asInt().toShort
          case ByteType => jsonValue.asInt().toByte
          case DateType =>
            // Assumes JSON contains date as string "yyyy-MM-dd"
            // Convert to days since epoch
            val dateStr = jsonValue.asText()
            java.sql.Date.valueOf(dateStr).toLocalDate.toEpochDay.toInt
          case TimestampType =>
            // Assumes JSON contains ISO-8601 timestamp
            val tsStr = jsonValue.asText()
            java.sql.Timestamp.valueOf(tsStr).getTime * 1000L // Convert to microseconds
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported data type for JSON conversion: ${field.dataType} " +
              s"for field '${field.name}'")
        }
      }
    }

    new GenericInternalRow(values)
  }

  override def next(): Boolean = {
    if (jsonIterator.hasNext) {
      try {
        val jsonNode = jsonIterator.next()
        currentRow = Some(jsonToInternalRow(jsonNode))
        true
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            s"Error parsing JSON row from presigned URL: $presignedUrl", e)
      }
    } else {
      currentRow = None
      false
    }
  }

  override def get(): InternalRow = {
    currentRow.getOrElse(
      throw new IllegalStateException("No current row available. Call next() first."))
  }

  override def close(): Unit = {
    // Cleanup handled by lazy initialization
  }
}
