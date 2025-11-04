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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.serverSidePlanning.{ServerSidePlanningClientFactory, ServerSidePlanningTestClientFactory}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end integration tests for server-side planning functionality.
 * Tests the full stack: DeltaCatalog -> ServerSidePlannedTable -> ServerSidePlanningClient -> Data
 */
class ServerSidePlanningIntegrationSuite extends QueryTest with SharedSparkSession {

  test("E2E: Full stack integration with DeltaCatalog") {
    // Save original catalog config
    val originalCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog")

    try {
      // Configure DeltaCatalog
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      withTable("integration_db.e2e_test") {
        // Create test database and table
        sql("CREATE DATABASE IF NOT EXISTS integration_db")
        sql("""
          CREATE TABLE integration_db.e2e_test (
            id INT,
            name STRING,
            value DOUBLE
          ) USING parquet
        """)

        // Insert test data
        sql("""
          INSERT INTO integration_db.e2e_test VALUES
          (1, 'alpha', 1.1),
          (2, 'beta', 2.2),
          (3, 'gamma', 3.3)
        """)

        // Enable server-side planning via test client
        ServerSidePlanningClientFactory.setFactory(new ServerSidePlanningTestClientFactory())
        spark.conf.set("spark.databricks.delta.catalog.forceServerSidePlanning", "true")

        try {
          // Execute query - should go through full server-side planning stack
          val results = sql(
            "SELECT id, name, value FROM integration_db.e2e_test ORDER BY id"
          ).collect()

          // Verify results
          assert(results.length == 3, "Expected 3 rows")
          assert(results(0).getInt(0) == 1)
          assert(results(0).getString(1) == "alpha")
          assert(results(0).getDouble(2) == 1.1)
          assert(results(1).getInt(0) == 2)
          assert(results(1).getString(1) == "beta")
          assert(results(2).getInt(0) == 3)
          assert(results(2).getString(1) == "gamma")

          // Verify the plan used server-side planning
          val client = ServerSidePlanningClientFactory.createClient(spark)
          val scanPlan = client.planScan("integration_db", "e2e_test")
          assert(scanPlan.files.nonEmpty, "Should have discovered files via server-side planning")

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

  test("E2E: Aggregation query through server-side planning") {
    val originalCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog")

    try {
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      withTable("integration_db.agg_test") {
        sql("CREATE DATABASE IF NOT EXISTS integration_db")
        sql("""
          CREATE TABLE integration_db.agg_test (
            category STRING,
            amount DOUBLE
          ) USING parquet
        """)

        sql("""
          INSERT INTO integration_db.agg_test VALUES
          ('A', 10.0),
          ('B', 20.0),
          ('A', 15.0),
          ('B', 25.0),
          ('A', 5.0)
        """)

        ServerSidePlanningClientFactory.setFactory(new ServerSidePlanningTestClientFactory())
        spark.conf.set("spark.databricks.delta.catalog.forceServerSidePlanning", "true")

        try {
          // Execute aggregation query
          val results = sql("""
            SELECT category, SUM(amount) as total, COUNT(*) as count
            FROM integration_db.agg_test
            GROUP BY category
            ORDER BY category
          """).collect()

          assert(results.length == 2)
          assert(results(0).getString(0) == "A")
          assert(results(0).getDouble(1) == 30.0)
          assert(results(0).getLong(2) == 3)
          assert(results(1).getString(0) == "B")
          assert(results(1).getDouble(1) == 45.0)
          assert(results(1).getLong(2) == 2)

        } finally {
          spark.conf.unset("spark.databricks.delta.catalog.forceServerSidePlanning")
          ServerSidePlanningClientFactory.clearFactory()
        }
      }
    } finally {
      originalCatalog.foreach(spark.conf.set("spark.sql.catalog.spark_catalog", _))
    }
  }

  test("E2E: Verify normal path unchanged when feature disabled") {
    val originalCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog")

    try {
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      withTable("integration_db.normal_test") {
        sql("CREATE DATABASE IF NOT EXISTS integration_db")
        sql("""
          CREATE TABLE integration_db.normal_test (
            id INT,
            data STRING
          ) USING parquet
        """)

        sql("INSERT INTO integration_db.normal_test VALUES (1, 'test')")

        // Do NOT enable server-side planning
        // Verify normal table loading still works
        val results = sql("SELECT * FROM integration_db.normal_test").collect()
        assert(results.length == 1)
        assert(results(0).getInt(0) == 1)
        assert(results(0).getString(1) == "test")
      }
    } finally {
      originalCatalog.foreach(spark.conf.set("spark.sql.catalog.spark_catalog", _))
    }
  }
}
