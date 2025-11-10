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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.catalog.ServerSidePlannedTable
import org.apache.spark.sql.test.SharedSparkSession

class ServerSidePlannedTableSuite extends QueryTest with SharedSparkSession {

  test("end-to-end: SELECT query returns correct results through ServerSidePlannedTable") {
    // Save original catalog config
    val originalCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog")

    try {
      // Configure DeltaCatalog as the Spark catalog
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      withTable("testdb.delta_table") {
        // Create test Delta table with data
        sql("CREATE DATABASE IF NOT EXISTS testdb")
        sql("""
          CREATE TABLE testdb.delta_table (
            id INT,
            name STRING,
            value DOUBLE
          ) USING delta
        """)

        sql("""
          INSERT INTO testdb.delta_table VALUES
          (1, 'one', 1.0),
          (2, 'two', 2.0),
          (3, 'three', 3.0),
          (4, 'four', 4.0)
        """)

        // Configure test client
        ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())

        try {
          // Execute SELECT query through ServerSidePlannedTable and verify results
          checkAnswer(
            sql("SELECT id, name, value FROM testdb.delta_table ORDER BY id"),
            Seq(
              Row(1, "one", 1.0),
              Row(2, "two", 2.0),
              Row(3, "three", 3.0),
              Row(4, "four", 4.0)
            )
          )

          // Verify scan planning discovered the files with correct format
          val client = ServerSidePlanningClientFactory.buildForCatalog(spark, "spark_catalog")
          val scanPlan = client.planScan("testdb", "delta_table")
          assert(scanPlan.files.nonEmpty, "Should have discovered Delta data files")
          assert(scanPlan.files.forall(_.fileFormat == "parquet"),
            "Delta tables store data in parquet format")

          // Verify partition count matches discovered files
          val catalogTable = spark.sessionState.catalog.getTableMetadata(
            org.apache.spark.sql.catalyst.TableIdentifier("delta_table", Some("testdb")))
          val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, catalogTable)
          val tableSchema = spark.table("testdb.delta_table").schema

          val table = new ServerSidePlannedTable(
            spark = spark,
            database = "testdb",
            tableName = "delta_table",
            tableSchema = tableSchema,
            planningClient = client,
            deltaLog = deltaLog
          )

          val partitions = table.newScanBuilder(
            new org.apache.spark.sql.util.CaseInsensitiveStringMap(
              java.util.Collections.emptyMap()
            )
          ).build().toBatch.planInputPartitions()

          assert(partitions.length == scanPlan.files.length,
            s"Should have ${scanPlan.files.length} partitions matching discovered files")

        } finally {
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
    val testFactory = new TestServerSidePlanningClientFactory()

    ServerSidePlanningClientFactory.setFactory(testFactory)
    assert(ServerSidePlanningClientFactory.getFactory() == testFactory)

    // Verify client creation uses custom factory
    val client = ServerSidePlanningClientFactory.buildForCatalog(spark, "spark_catalog")
    assert(client.isInstanceOf[TestServerSidePlanningClient])

    // Clear and verify back to default
    ServerSidePlanningClientFactory.clearFactory()
    assert(ServerSidePlanningClientFactory.getFactory()
      .isInstanceOf[IcebergRESTCatalogPlanningClientFactory])
  }
}
