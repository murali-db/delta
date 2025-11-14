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
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Tests the server-side planning flow through DeltaCatalog with a mock client.
 * ServerSidePlannedTableSuite tests the Table implementation in isolation.
 */
class ServerSidePlanningSuite extends QueryTest with DeltaSQLCommandTest {

  override def afterEach(): Unit = {
    try {
      // Clean up factory and config after each test to prevent interference
      spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  test("E2E: Full stack integration with DeltaCatalog") {
    withTable("integration_db.e2e_test") {
        // Create test database and table
        sql("CREATE DATABASE IF NOT EXISTS integration_db")
        sql("""
          CREATE TABLE integration_db.e2e_test (
            id INT,
            name STRING,
            value INT
          ) USING parquet
        """)

        // Insert test data
        sql("""
          INSERT INTO integration_db.e2e_test (id, name, value) VALUES
          (1, 'alpha', 10),
          (2, 'beta', 20),
          (3, 'gamma', 30)
        """)

        // Enable server-side planning via test client
        ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

        // Verify that DeltaCatalog actually returns ServerSidePlannedTable
        val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
          .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
        val loadedTable = catalog.loadTable(
          org.apache.spark.sql.connector.catalog.Identifier.of(
            Array("integration_db"), "e2e_test"))
        assert(loadedTable.isInstanceOf[ServerSidePlannedTable],
          s"Expected ServerSidePlannedTable but got ${loadedTable.getClass.getName}")

        // Execute query - should go through full server-side planning stack
        checkAnswer(
          sql("SELECT id, name, value FROM integration_db.e2e_test ORDER BY id"),
          Seq(
            Row(1, "alpha", 10),
            Row(2, "beta", 20),
            Row(3, "gamma", 30)
          )
        )

        // Verify the plan used server-side planning
        val client = ServerSidePlanningClientFactory.buildForCatalog(spark, "spark_catalog")
        val scanPlan = client.planScan("integration_db", "e2e_test")
        assert(scanPlan.files.nonEmpty, "Should have discovered files via server-side planning")
        assert(scanPlan.files.forall(_.fileFormat == "parquet"),
          "Delta tables use Parquet format for data files")
      }
  }

  test("E2E: Verify normal path unchanged when feature disabled") {
    withTable("integration_db.normal_test") {
        sql("CREATE DATABASE IF NOT EXISTS integration_db")
        sql("""
          CREATE TABLE integration_db.normal_test (
            id INT,
            data STRING
          ) USING parquet
        """)

        sql("INSERT INTO integration_db.normal_test (id, data) VALUES (1, 'test')")

        // Do NOT enable server-side planning
        // Verify that DeltaCatalog returns normal table, not ServerSidePlannedTable
        val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
          .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
        val loadedTable = catalog.loadTable(
          org.apache.spark.sql.connector.catalog.Identifier.of(
            Array("integration_db"), "normal_test"))
        assert(!loadedTable.isInstanceOf[ServerSidePlannedTable],
          s"Expected normal table but got ServerSidePlannedTable when config is disabled")
      }
  }

  test("loadTable() decision logic with ENABLE_SERVER_SIDE_PLANNING config") {
    withTable("test_db.decision_test") {
        sql("CREATE DATABASE IF NOT EXISTS test_db")
        sql("CREATE TABLE test_db.decision_test (id INT) USING parquet")
        sql("INSERT INTO test_db.decision_test VALUES (1)")

        ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())

        val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
          .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
          .asInstanceOf[DeltaCatalog]

        // Case 1: Config enabled (force server-side planning)
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
        val table1 = catalog.loadTable(
          org.apache.spark.sql.connector.catalog.Identifier.of(
            Array("test_db"), "decision_test"))
        assert(table1.isInstanceOf[ServerSidePlannedTable],
          "Expected ServerSidePlannedTable when config is enabled")

        // Case 2: Config disabled, normal path
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false")
        val table2 = catalog.loadTable(
          org.apache.spark.sql.connector.catalog.Identifier.of(
            Array("test_db"), "decision_test"))
        assert(!table2.isInstanceOf[ServerSidePlannedTable],
          "Expected normal table when config is disabled")
      }
  }

  test("shouldUseServerSidePlanning() decision logic") {
    // Case 1: Force flag enabled -> should always use server-side planning
    assert(ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = true,
      forceServerSidePlanning = true),
      "Should use server-side planning when force flag is true")

    // Case 2: Unity Catalog without credentials -> should use server-side planning
    assert(ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = true,
      hasCredentials = false,
      forceServerSidePlanning = false),
      "Should use server-side planning for UC table without credentials")

    // Case 3: Unity Catalog with credentials -> should NOT use server-side planning
    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = true,
      hasCredentials = true,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for UC table with credentials")

    // Case 4: Non-UC catalog -> should NOT use server-side planning
    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = true,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for non-UC catalog")

    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = false,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for non-UC catalog (even without credentials)")
  }
}
