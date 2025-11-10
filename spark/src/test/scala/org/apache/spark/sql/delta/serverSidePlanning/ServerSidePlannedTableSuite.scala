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
import org.apache.spark.sql.delta.catalog.ServerSidePlannedTable
import org.apache.spark.sql.test.SharedSparkSession

class ServerSidePlannedTableSuite extends QueryTest with SharedSparkSession {

  test("end-to-end: discover Parquet files using test client with input_file_name()") {
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

      // Configure factory to use test client
      val testFactory = new ServerSidePlanningTestClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      try {
        // Create client and get scan plan
        val client = ServerSidePlanningClientFactory.createClient(spark)
        val scanPlan = client.planScan("testdb", "test_table")

        // Verify we discovered files using input_file_name()
        assert(scanPlan.files.nonEmpty, "Should discover data files")
        assert(scanPlan.files.forall(_.fileFormat == "parquet"),
          "Parquet tables use parquet format")

        // Get the table schema from the actual table
        val tableSchema = spark.table("testdb.test_table").schema

        // Create ServerSidePlannedTable using schema from the table
        val table = new ServerSidePlannedTable(
          database = "testdb",
          tableName = "test_table",
          tableSchema = tableSchema,
          planningClient = client
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
        ServerSidePlanningClientFactory.clearFactory()
      }
    }
  }

  test("end-to-end: SELECT query returns correct results through ServerSidePlannedTable") {
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

        // Configure test client and enable force server-side planning
        ServerSidePlanningClientFactory.setFactory(new ServerSidePlanningTestClientFactory())
        spark.conf.set("spark.databricks.delta.catalog.forceServerSidePlanning", "true")

        try {
          // Execute SELECT query through ServerSidePlannedTable
          val results = sql("SELECT id, name, value FROM testdb.parquet_table ORDER BY id")
            .collect()

          // Verify results: Should return all 4 rows through server-side scan planning
          assert(results.length == 4, "Expected 4 rows from ServerSidePlannedTable")
          assert(results(0).getInt(0) == 1)
          assert(results(0).getString(1) == "one")
          assert(results(0).getDouble(2) == 1.0)
          assert(results(1).getInt(0) == 2)
          assert(results(1).getString(1) == "two")
          assert(results(2).getInt(0) == 3)
          assert(results(3).getInt(0) == 4)

          // Verify scan planning discovered the files
          val client = ServerSidePlanningClientFactory.createClient(spark)
          val scanPlan = client.planScan("testdb", "parquet_table")
          assert(scanPlan.files.length == 2, "Should have discovered 2 parquet files")

        } finally {
          spark.conf.unset("spark.databricks.delta.catalog.forceServerSidePlanning")
          ServerSidePlanningClientFactory.clearFactory()
        }
      }
    } finally {
      // Restore original catalog
      originalCatalog.foreach(spark.conf.set("spark.sql.catalog.spark_catalog", _))
    }
  }

  test("ServerSidePlanningClientFactory registry works correctly") {
    // Verify default factory
    assert(ServerSidePlanningClientFactory.getFactory()
      .isInstanceOf[IcebergRESTCatalogPlanningClientFactory])

    // Set custom factory (test client)
    val testFactory = new ServerSidePlanningTestClientFactory()

    ServerSidePlanningClientFactory.setFactory(testFactory)
    assert(ServerSidePlanningClientFactory.getFactory() == testFactory)

    // Verify client creation uses custom factory
    val client = ServerSidePlanningClientFactory.createClient(spark)
    assert(client.isInstanceOf[ServerSidePlanningTestClient])

    // Clear and verify back to default
    ServerSidePlanningClientFactory.clearFactory()
    assert(ServerSidePlanningClientFactory.getFactory()
      .isInstanceOf[IcebergRESTCatalogPlanningClientFactory])
  }
}
