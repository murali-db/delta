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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Tests ServerSidePlannedTable in isolation using a mock client.
 * ServerSidePlanningSuite tests the full integration through DeltaCatalog.
 */
class ServerSidePlannedTableSuite extends QueryTest with DeltaSQLCommandTest {

  test("end-to-end: ServerSidePlannedTable with test client") {
    withTable("test_table") {
      // Create a Parquet table with data
      sql("""
        CREATE TABLE test_table (
          id INT,
          name STRING,
          category STRING
        ) USING parquet
      """)

      sql("""
        INSERT INTO test_table (id, name, category) VALUES
        (1, 'Alice', 'A'),
        (2, 'Bob', 'B'),
        (3, 'Charlie', 'A'),
        (4, 'David', 'B')
      """)

      // Configure factory to use test client
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      assert(ServerSidePlanningClientFactory.getFactory() == testFactory,
        "Factory should be set to test factory")

      try {
        // Create client and verify it's the test client
        val client = ServerSidePlanningClientFactory.buildForCatalog(spark, "spark_catalog")
        assert(client.isInstanceOf[TestServerSidePlanningClient],
          "Client should be TestServerSidePlanningClient")

        // Get scan plan and verify file discovery
        val scanPlan = client.planScan("default", "test_table")
        assert(scanPlan.files.nonEmpty, "Should discover data files")
        assert(scanPlan.files.forall(_.fileFormat == "parquet"),
          "Parquet tables should have parquet file format")
        assert(scanPlan.files.forall(_.fileSizeInBytes > 0),
          "All files should have positive size")

        // Get the table schema from the actual table
        val tableSchema = spark.table("test_table").schema

        // Create ServerSidePlannedTable using schema from the table
        val table = ServerSidePlannedTable.forTesting(
          spark = spark,
          database = "default",
          tableName = "test_table",
          tableSchema = tableSchema,
          client = client
        )

        // Verify table metadata
        assert(table.name() == "default.test_table",
          "Table name should be fully qualified")
        assert(table.schema() == tableSchema,
          "Table schema should match")

        // Verify scan produces correct number of partitions
        val scan = table.newScanBuilder(
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(
            java.util.Collections.emptyMap()
          )
        ).build()

        val partitions = scan.toBatch.planInputPartitions()
        assert(partitions.length == scanPlan.files.length,
          s"Should have ${scanPlan.files.length} partitions, one per file")

        // Verify reader factory can be created
        val readerFactory = scan.toBatch.createReaderFactory()
        assert(readerFactory != null, "Reader factory should be created")

        // Verify we can create a reader for the first partition
        val reader = readerFactory.createReader(partitions(0))
        assert(reader != null, "Reader should be created for partition")

      } finally {
        // Clean up factory
        ServerSidePlanningClientFactory.clearFactory()
      }
    }
  }

  test("ServerSidePlannedTable is read-only and does not support writes") {
    withTable("readonly_test") {
      // Create a Parquet table with data
      sql("""
        CREATE TABLE readonly_test (
          id INT,
          data STRING
        ) USING parquet
      """)

      sql("INSERT INTO readonly_test VALUES (1, 'test')")

      // Create ServerSidePlannedTable
      val tableSchema = spark.table("readonly_test").schema
      val client = new TestServerSidePlanningClient(spark)
      val table = ServerSidePlannedTable.forTesting(
        spark = spark,
        database = "default",
        tableName = "readonly_test",
        tableSchema = tableSchema,
        client = client
      )

      // Verify table only supports BATCH_READ capability
      val capabilities = table.capabilities()
      assert(capabilities.size() == 1,
        "ServerSidePlannedTable should have exactly one capability")
      assert(capabilities.contains(
        org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ),
        "ServerSidePlannedTable should support BATCH_READ")
      assert(!capabilities.contains(
        org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE),
        "ServerSidePlannedTable should NOT support BATCH_WRITE")

      // Verify table does not implement SupportsWrite
      assert(!table.isInstanceOf[org.apache.spark.sql.connector.catalog.SupportsWrite],
        "ServerSidePlannedTable should not implement SupportsWrite")
    }
  }
}
