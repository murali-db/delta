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

import scala.jdk.CollectionConverters._

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.iceberg.{Files => IcebergFiles}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.Parquet
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.delta.serverSidePlanning.{IcebergRESTCatalogPlanningClient, IcebergRESTCatalogPlanningClientFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import shadedForDelta.org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec, Schema, Table}
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer
import shadedForDelta.org.apache.iceberg.types.Types

class IcebergRESTCatalogPlanningClientSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get),
    Types.NestedField.required(2, "name", Types.StringType.get))
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces].createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException("Catalog does not support namespaces")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  // Tests that the REST /plan endpoint returns 0 files for an empty table.
  test("basic plan table scan via IcebergRESTCatalogPlanningClient") {
    withTempTable("testTable") { table =>
      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        val scanPlan = client.planScan(defaultNamespace.toString, "testTable")
        assert(scanPlan != null, "Scan plan should not be null")
        assert(scanPlan.files != null, "Scan plan files should not be null")
        assert(scanPlan.files.isEmpty, s"Empty table should have 0 files, got ${scanPlan.files.length}")
      } finally {
        client.close()
      }
    }
  }

  // Tests that the REST /plan endpoint returns the correct number of files for a non-empty table.
  // Creates a table, writes actual parquet files with data, then verifies the response includes them.
  test("plan scan on non-empty table with data files") {
    withTempTable("tableWithData") { table =>
      // Add two data files with actual parquet data
      val file1Path = s"${table.location()}/data/file1.parquet"
      val file2Path = s"${table.location()}/data/file2.parquet"
      addDataFileToTable(table, file1Path, recordCount = 100)
      addDataFileToTable(table, file2Path, recordCount = 150)

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        val scanPlan = client.planScan(defaultNamespace.toString, "tableWithData")
        assert(scanPlan != null, "Scan plan should not be null")
        assert(scanPlan.files != null, "Scan plan files should not be null")
        assert(scanPlan.files.length == 2, s"Expected 2 files but got ${scanPlan.files.length}")

        // Verify the actual file information matches what was generated.
        // Check the paths match by comparing the ends.
        val filePaths = scanPlan.files.map(_.filePath).toSet
        assert(filePaths.exists(_.endsWith("/data/file1.parquet")),
          s"Scan plan should contain file ending with /data/file1.parquet. Got: $filePaths")
        assert(filePaths.exists(_.endsWith("/data/file2.parquet")),
          s"Scan plan should contain file ending with /data/file2.parquet. Got: $filePaths")

        // Verify all files have valid metadata
        scanPlan.files.foreach { file =>
          assert(file.fileSizeInBytes > 0,
            s"File ${file.filePath} should have size > 0, got ${file.fileSizeInBytes}")
          assert(file.fileFormat == "parquet",
            s"File ${file.filePath} should be parquet format, got ${file.fileFormat}")
        }

        // Verify file contents match what was written
        val file1 = scanPlan.files.find(_.filePath.endsWith("/data/file1.parquet")).get
        val file2 = scanPlan.files.find(_.filePath.endsWith("/data/file2.parquet")).get
        verifyParquetFileContent(file1.filePath, expectedRecordCount = 100)
        verifyParquetFileContent(file2.filePath, expectedRecordCount = 150)
      } finally {
        client.close()
      }
    }
  }

  // TODO: Add test for partitioned table rejection
  // Once the test server (IcebergRESTCatalogAdapterWithPlanSupport) properly retains and serves
  // partition data through the commit/serialize/deserialize cycle, add a test that verifies:
  // 1. Creates a partitioned table with data files containing partition info
  // 2. Calls client.planScan() and expects UnsupportedOperationException
  // 3. Verifies exception message contains "partition data"
  // This will test the client's partition validation logic at IcebergRESTCatalogPlanningClient:160-164

  private def startServer(): IcebergRESTServer = {
    val config = Map(IcebergRESTServer.REST_PORT -> "0").asJava
    val newServer = new IcebergRESTServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start IcebergRESTServer")
    }
    newServer
  }

  private def isServerReachable(server: IcebergRESTServer): Boolean = {
    val httpHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

    val httpClient = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .build()

    try {
      val httpGet = new HttpGet(s"http://localhost:${server.getPort}/v1/config")
      val httpResponse = httpClient.execute(httpGet)
      try {
        val statusCode = httpResponse.getStatusLine.getStatusCode
        statusCode == 200
      } finally {
        httpResponse.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    val tableId = TableIdentifier.of(defaultNamespace, tableName)
    val table = catalog.createTable(tableId, defaultSchema, defaultSpec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
    }
  }


  /**
   * Add a data file to an Iceberg table by writing actual parquet data.
   * Uses unshaded parquet utilities to write to a local file, then shaded utilities to add it.
   * Based on the pattern from Iceberg's TestSparkParquetWriter.
   *
   * @param table Iceberg table to add the file to (shaded type)
   * @param filePath Path where the parquet file will be written
   * @param recordCount Number of records to write
   * @param partitionPath Optional partition path (e.g., "name=test") for partitioned tables
   */
  private def addDataFileToTable(
      table: Table,
      filePath: String,
      recordCount: Int = 100,
      partitionPath: Option[String] = None): Unit = {

    // Create unshaded schema matching the table schema for writing
    val unshadedSchema = new org.apache.iceberg.Schema(
      org.apache.iceberg.types.Types.NestedField.required(
        1, "id", org.apache.iceberg.types.Types.LongType.get),
      org.apache.iceberg.types.Types.NestedField.required(
        2, "name", org.apache.iceberg.types.Types.StringType.get)
    )

    // Create test records using unshaded types
    val records = (1 to recordCount).map { i =>
      val record = GenericRecord.create(unshadedSchema)
      record.setField("id", i.toLong)
      record.setField("name", s"test_$i")
      record
    }

    // Create a temporary file for writing
    // Use Files.localOutput to avoid shaded/unshaded FileIO issues
    // Handle both file:// URIs and plain paths
    val tempFile = if (filePath.startsWith("file:")) {
      new File(new java.net.URI(filePath))
    } else {
      new File(filePath)
    }
    tempFile.getParentFile.mkdirs()

    // Write the parquet file using unshaded parquet utilities
    val fileAppender: FileAppender[GenericRecord] = Parquet
      .write(IcebergFiles.localOutput(tempFile))
      .schema(unshadedSchema)
      .createWriterFunc(GenericParquetWriter.buildWriter)
      .overwrite()
      .build()

    try {
      fileAppender.addAll(records.asJava)
    } finally {
      fileAppender.close()
    }

    // Extract split offsets from the actual parquet file by reading block metadata
    // This is the correct way - split offsets are the starting positions of parquet row groups
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val parquetInputFile = HadoopInputFile.fromPath(new Path(tempFile.getAbsolutePath), hadoopConf)
    val splitOffsets = {
      val reader = ParquetFileReader.open(parquetInputFile)
      try {
        val blocks = reader.getFooter.getBlocks.asScala
        val offsets = blocks.map(block => java.lang.Long.valueOf(block.getStartingPos)).sorted.toSeq
        java.util.Arrays.asList(offsets: _*)
      } finally {
        reader.close()
      }
    }

    // Now add the file to the table using shaded DataFiles.builder
    val inputFile = table.io().newInputFile(filePath)
    val metrics = fileAppender.metrics()

    val dataFileBuilder = DataFiles.builder(table.spec())
      .withInputFile(inputFile)
      .withFormat(FileFormat.PARQUET)
      .withRecordCount(metrics.recordCount())
      .withSplitOffsets(splitOffsets)

    // Add partition path if provided (for partitioned tables)
    val dataFile = partitionPath match {
      case Some(path) => dataFileBuilder.withPartitionPath(path).build()
      case None => dataFileBuilder.build()
    }

    table.newAppend()
      .appendFile(dataFile)
      .commit()
  }

  /**
   * Verify that a parquet file contains the expected number of records with correct data.
   */
  private def verifyParquetFileContent(filePath: String, expectedRecordCount: Int): Unit = {
    val file = if (filePath.startsWith("file:")) {
      new File(new java.net.URI(filePath))
    } else {
      new File(filePath)
    }

    require(file.exists(), s"File should exist: $filePath")

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val parquetInputFile = HadoopInputFile.fromPath(new Path(file.getAbsolutePath), hadoopConf)
    val reader = ParquetFileReader.open(parquetInputFile)

    try {
      // Verify the file has the expected number of records
      val totalRecordCount = reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
      assert(totalRecordCount == expectedRecordCount,
        s"File $filePath should have $expectedRecordCount records, got $totalRecordCount")
    } finally {
      reader.close()
    }
  }
}
