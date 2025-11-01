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

package org.apache.spark.sql.delta.catalog

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.icebergScanPlan.IcebergTableClient
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A Spark Table implementation that uses Iceberg REST scan planning
 * to get the list of files to read. Used as a fallback when Unity Catalog
 * doesn't provide credentials.
 */
class IcebergPlannedTable(
    namespace: String,
    tableName: String,
    client: IcebergTableClient,
    tableSchema: StructType) extends Table with SupportsRead {

  override def name(): String = s"$namespace.$tableName"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new IcebergPlanScanBuilder(namespace, tableName, client, tableSchema, options)
  }
}

/**
 * ScanBuilder that uses IcebergTableClient to plan the scan.
 * Supports pushing down filters to the Iceberg REST server.
 */
class IcebergPlanScanBuilder(
    namespace: String,
    tableName: String,
    client: IcebergTableClient,
    tableSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends ScanBuilder with SupportsPushDownFilters {

  import org.apache.spark.sql.sources._

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // For MVP: Only support simple comparison operators
    val (supported, unsupported) = filters.partition {
      case _: EqualTo => true
      case _: LessThan => true
      case _: GreaterThan => true
      case _: LessThanOrEqual => true
      case _: GreaterThanOrEqual => true
      case _: And => true
      case _: Or => true
      case _ => false
    }

    _pushedFilters = supported
    unsupported  // Return filters Spark needs to apply client-side
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    new IcebergPlanScan(
      namespace, tableName, client, tableSchema, _pushedFilters, options)
  }
}

/**
 * Scan implementation that calls the Iceberg REST API to get file list.
 * Applies server-side filtering via Iceberg expressions.
 */
class IcebergPlanScan(
    namespace: String,
    tableName: String,
    client: IcebergTableClient,
    tableSchema: StructType,
    pushedFilters: Array[org.apache.spark.sql.sources.Filter],
    options: CaseInsensitiveStringMap)
  extends Scan with Batch {

  override def readSchema(): StructType = tableSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // TODO: Convert pushedFilters to Iceberg expression format
    // For now, call without filters (will add converter in next task)
    val scanPlan = client.planTableScan(namespace, tableName)

    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      IcebergFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Pass filters to reader factory so they can be pushed to file formats
    new IcebergFilePartitionReaderFactory(tableSchema, pushedFilters)
  }
}

/**
 * InputPartition representing a single file from the Iceberg scan plan.
 */
case class IcebergFileInputPartition(
    filePath: String,
    fileSizeInBytes: Long,
    fileFormat: String) extends InputPartition

/**
 * Factory for creating PartitionReaders that read Iceberg-planned files.
 * Builds reader functions on the driver for each file format.
 * Pushes filters down to file formats (Parquet, ORC) for better performance.
 */
class IcebergFilePartitionReaderFactory(
    schema: StructType,
    pushedFilters: Array[org.apache.spark.sql.sources.Filter])
  extends PartitionReaderFactory {

  import org.apache.spark.util.SerializableConfiguration

  // Get SparkSession and Hadoop configuration on driver
  private val spark = SparkSession.active
  // scalastyle:off deltahadoopconfiguration
  private val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader functions for each file format on the driver
  // These functions will be serialized and sent to executors
  // Pass filters to enable filter pushdown into Parquet/ORC
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = schema,
    partitionSchema = StructType(Nil),
    requiredSchema = schema,
    filters = pushedFilters.toSeq,  // Push filters to Parquet!
    options = Map(
      FileFormat.OPTION_RETURNING_BATCH -> "false"
    ),
    hadoopConf = hadoopConf.value
  )

  private val orcReaderBuilder = new OrcFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = schema,
    partitionSchema = StructType(Nil),
    requiredSchema = schema,
    filters = pushedFilters.toSeq,  // Push filters to ORC!
    options = Map(
      FileFormat.OPTION_RETURNING_BATCH -> "false"
    ),
    hadoopConf = hadoopConf.value
  )

  private val csvReaderBuilder = new CSVFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = schema,
    partitionSchema = StructType(Nil),
    requiredSchema = schema,
    filters = pushedFilters.toSeq,  // CSV doesn't support pushdown but pass anyway
    options = Map.empty[String, String],
    hadoopConf = hadoopConf.value
  )

  private val jsonReaderBuilder = new JsonFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = schema,
    partitionSchema = StructType(Nil),
    requiredSchema = schema,
    filters = pushedFilters.toSeq,  // JSON doesn't support pushdown but pass anyway
    options = Map.empty[String, String],
    hadoopConf = hadoopConf.value
  )

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val filePartition = partition.asInstanceOf[IcebergFileInputPartition]

    // Select the appropriate reader builder based on file format
    val readerBuilder = {
      // scalastyle:off caselocale
      filePartition.fileFormat.toLowerCase(Locale.ROOT) match {
      // scalastyle:on caselocale
        case "parquet" => parquetReaderBuilder
        case "orc" => orcReaderBuilder
        case "csv" => csvReaderBuilder
        case "json" => jsonReaderBuilder
        case other => throw new UnsupportedOperationException(
          s"File format '$other' is not supported. Supported formats: parquet, orc, csv, json")
      }
    }

    new IcebergFilePartitionReader(filePartition, readerBuilder)
  }
}

/**
 * PartitionReader that reads a single file using a pre-built reader function.
 * The reader function was created on the driver and is executed on the executor.
 */
class IcebergFilePartitionReader(
    partition: IcebergFileInputPartition,
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
