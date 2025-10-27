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

package org.apache.spark.sql.delta.icebergScanPlan

import java.util.Collections

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.catalog.IcebergPlannedTable
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class IcebergPlannedTableSuite extends QueryTest with SharedSparkSession {

  // ========== Unit Tests with Mock Client ==========

  test("IcebergPlannedTable basic properties") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))

    val mockClient = MockIcebergTableClient.withSingleFile(
      namespace = "testdb",
      table = "testtable",
      filePath = "/path/to/data/file1.parquet",
      fileSize = 1000
    )

    val table = new IcebergPlannedTable(
      namespace = "testdb",
      tableName = "testtable",
      client = mockClient,
      tableSchema = schema
    )

    // Verify table properties
    assert(table.name() == "testdb.testtable")
    assert(table.schema() == schema)
    assert(table.capabilities().contains(
      org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ))
  }

  test("IcebergPlannedTable scan with single file") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))

    val mockClient = MockIcebergTableClient.withSingleFile(
      namespace = "testdb",
      table = "testtable",
      filePath = "/path/to/data/file1.parquet",
      fileSize = 1000
    )

    val table = new IcebergPlannedTable(
      namespace = "testdb",
      tableName = "testtable",
      client = mockClient,
      tableSchema = schema
    )

    // Create scan and verify file list
    val scan = table.newScanBuilder(
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    ).build()

    assert(scan.readSchema() == schema)

    val batch = scan.toBatch
    val partitions = batch.planInputPartitions()

    assert(partitions.length == 1)

    val partition = partitions(0).asInstanceOf[
      org.apache.spark.sql.delta.catalog.IcebergFileInputPartition]
    assert(partition.filePath == "/path/to/data/file1.parquet")
    assert(partition.fileSizeInBytes == 1000)
    assert(partition.fileFormat == "parquet")
  }

  test("IcebergPlannedTable scan with multiple files") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true)
    ))

    val files = Seq(
      ScanFile("/path/file1.parquet", 1000, "parquet"),
      ScanFile("/path/file2.parquet", 2000, "parquet"),
      ScanFile("/path/file3.parquet", 1500, "parquet")
    )

    val mockClient = MockIcebergTableClient.withFiles(
      namespace = "testdb",
      table = "multitable",
      files = files
    )

    val table = new IcebergPlannedTable(
      namespace = "testdb",
      tableName = "multitable",
      client = mockClient,
      tableSchema = schema
    )

    val scan = table.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap())).build()
    val batch = scan.toBatch
    val partitions = batch.planInputPartitions()

    assert(partitions.length == 3)

    // Verify all files are represented
    val filePartitions = partitions.map(_.asInstanceOf[
      org.apache.spark.sql.delta.catalog.IcebergFileInputPartition])
    assert(filePartitions.map(_.filePath).toSet == files.map(_.filePath).toSet)
    assert(filePartitions.map(_.fileSizeInBytes).toSet == files.map(_.fileSizeInBytes).toSet)
  }

  test("IcebergPlannedTable handles errors from client") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true)
    ))

    // Create mock client without configuring the table - will throw error
    val mockClient = new MockIcebergTableClient()

    val table = new IcebergPlannedTable(
      namespace = "testdb",
      tableName = "nonexistent",
      client = mockClient,
      tableSchema = schema
    )

    val scan = table.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap())).build()

    // Should throw when trying to plan
    intercept[RuntimeException] {
      scan.toBatch.planInputPartitions()
    }
  }

  // ========== End-to-End Tests with Spark-Based Client ==========

  test("end-to-end: discover Parquet files using Spark-based client with input_file_name()") {
    withTable("testdb.test_table") {
      // Create a Parquet table with data
      sql("CREATE DATABASE IF NOT EXISTS testdb")
      sql("""
        CREATE TABLE testdb.test_table (
          id INT,
          name STRING,
          category STRING
        ) USING parquet
      """)

      sql("""
        INSERT INTO testdb.test_table (id, name, category) VALUES
        (1, 'Alice', 'A'),
        (2, 'Bob', 'B'),
        (3, 'Charlie', 'A'),
        (4, 'David', 'B')
      """)

      // Configure factory to use Spark-based client
      val sparkFactory = new SparkBasedIcebergTableClientFactory()
      IcebergTableClientFactory.setFactory(sparkFactory)

      try {
        // Create client and get scan plan
        val client = IcebergTableClientFactory.createClient(spark)
        val scanPlan = client.planTableScan("testdb", "test_table")

        // Verify we discovered files using input_file_name()
        assert(scanPlan.files.nonEmpty, "Should discover data files")
        assert(scanPlan.files.forall(_.fileFormat == "parquet"),
          "Parquet tables use parquet format")

        // Verify we can parse the schema
        assert(scanPlan.schema.contains("id"))
        assert(scanPlan.schema.contains("name"))

        // Create IcebergPlannedTable using schema from scan plan
        val tableSchema = org.apache.spark.sql.types.DataType
          .fromJson(scanPlan.schema).asInstanceOf[StructType]
        val table = new IcebergPlannedTable(
          namespace = "testdb",
          tableName = "test_table",
          client = client,
          tableSchema = tableSchema
        )

        // Verify scan produces correct number of partitions
        val scan = table.newScanBuilder(
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(
            java.util.Collections.emptyMap()
          )
        ).build()

        val partitions = scan.toBatch.planInputPartitions()
        assert(partitions.length == scanPlan.files.length,
          s"Should have ${scanPlan.files.length} partitions")

      } finally {
        // Clean up factory
        IcebergTableClientFactory.clearFactory()
      }
    }
  }

  test("end-to-end: SELECT query returns correct results through IcebergPlannedTable") {
    // Save original catalog config
    val originalCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog")

    try {
      // Configure DeltaCatalog as the Spark catalog
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      withTable("testdb.parquet_table") {
        // Create test table with data
        sql("CREATE DATABASE IF NOT EXISTS testdb")
        sql("""
          CREATE TABLE testdb.parquet_table (
            id INT,
            name STRING,
            value DOUBLE
          ) USING parquet
        """)

        sql("""
          INSERT INTO testdb.parquet_table VALUES
          (1, 'one', 1.0),
          (2, 'two', 2.0),
          (3, 'three', 3.0),
          (4, 'four', 4.0)
        """)

        // Configure test client and enable force Iceberg scan planning
        IcebergTableClientFactory.setFactory(new SparkBasedIcebergTableClientFactory())
        spark.conf.set("spark.databricks.delta.catalog.forceIcebergScanPlanning", "true")

        try {
          // Execute SELECT query through IcebergPlannedTable
          val results = sql("SELECT id, name, value FROM testdb.parquet_table ORDER BY id")
            .collect()

          // Verify results: Should return all 4 rows through Iceberg scan planning
          assert(results.length == 4, "Expected 4 rows from IcebergPlannedTable")
          assert(results(0).getInt(0) == 1)
          assert(results(0).getString(1) == "one")
          assert(results(0).getDouble(2) == 1.0)
          assert(results(1).getInt(0) == 2)
          assert(results(1).getString(1) == "two")
          assert(results(2).getInt(0) == 3)
          assert(results(3).getInt(0) == 4)

          // Verify scan planning discovered the files
          val client = IcebergTableClientFactory.createClient(spark)
          val scanPlan = client.planTableScan("testdb", "parquet_table")
          assert(scanPlan.files.length == 2, "Should have discovered 2 parquet files")

        } finally {
          spark.conf.unset("spark.databricks.delta.catalog.forceIcebergScanPlanning")
          IcebergTableClientFactory.clearFactory()
        }
      }
    } finally {
      // Restore original catalog
      originalCatalog.foreach(spark.conf.set("spark.sql.catalog.spark_catalog", _))
    }
  }

  test("IcebergTableClientFactory registry works correctly") {
    // Verify default factory
    assert(IcebergTableClientFactory.getFactory()
      .isInstanceOf[RESTIcebergTableClientFactory])

    // Set custom factory
    val mockClient = MockIcebergTableClient.withSingleFile(
      "db", "table", "/file.parquet", 100)
    val mockFactory = new MockIcebergTableClientFactory(mockClient)

    IcebergTableClientFactory.setFactory(mockFactory)
    assert(IcebergTableClientFactory.getFactory() == mockFactory)

    // Verify client creation uses custom factory
    val client = IcebergTableClientFactory.createClient(spark)
    assert(client == mockClient)

    // Clear and verify back to default
    IcebergTableClientFactory.clearFactory()
    assert(IcebergTableClientFactory.getFactory()
      .isInstanceOf[RESTIcebergTableClientFactory])
  }
}
