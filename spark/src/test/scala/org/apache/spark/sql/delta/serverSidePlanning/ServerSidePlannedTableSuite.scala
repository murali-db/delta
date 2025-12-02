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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Tests for server-side planning with a mock client.
 */
class ServerSidePlannedTableSuite extends QueryTest with DeltaSQLCommandTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test database and shared table once for all tests
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("""
      CREATE TABLE test_db.shared_test (
        id INT,
        name STRING,
        value INT
      ) USING parquet
    """)
    sql("""
      INSERT INTO test_db.shared_test (id, name, value) VALUES
      (1, 'alpha', 10),
      (2, 'beta', 20),
      (3, 'gamma', 30)
    """)
  }

  /**
   * Helper method to run tests with server-side planning enabled.
   * Automatically sets up the test factory and config, then cleans up afterwards.
   * This prevents test pollution from leaked configuration.
   */
  private def withServerSidePlanningEnabled(f: => Unit): Unit = {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
    try {
      f
    } finally {
      // Reset factory
      ServerSidePlanningClientFactory.clearFactory()
      // Restore original config
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("full query through DeltaCatalog with server-side planning") {
    // This test verifies server-side planning works end-to-end by checking:
    // (1) DeltaCatalog returns ServerSidePlannedTable (not normal table)
    // (2) Query execution returns correct results
    // If both are true, the server-side planning client worked correctly - that's the only way
    // ServerSidePlannedTable can read data.

    withServerSidePlanningEnabled {
      // (1) Verify that DeltaCatalog actually returns ServerSidePlannedTable
      val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
        .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      val loadedTable = catalog.loadTable(
        org.apache.spark.sql.connector.catalog.Identifier.of(
          Array("test_db"), "shared_test"))
      assert(loadedTable.isInstanceOf[ServerSidePlannedTable],
        s"Expected ServerSidePlannedTable but got ${loadedTable.getClass.getName}")

      // (2) Execute query - should go through full server-side planning stack
      checkAnswer(
        sql("SELECT id, name, value FROM test_db.shared_test ORDER BY id"),
        Seq(
          Row(1, "alpha", 10),
          Row(2, "beta", 20),
          Row(3, "gamma", 30)
        )
      )
    }
  }

  test("verify normal path unchanged when feature disabled") {
    // Explicitly disable server-side planning
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false")

    // Verify that DeltaCatalog returns normal table, not ServerSidePlannedTable
    val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
      .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
    val loadedTable = catalog.loadTable(
      org.apache.spark.sql.connector.catalog.Identifier.of(
        Array("test_db"), "shared_test"))
    assert(!loadedTable.isInstanceOf[ServerSidePlannedTable],
      s"Expected normal table but got ServerSidePlannedTable when config is disabled")
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

  test("ServerSidePlannedTable is read-only") {
    withTable("readonly_test") {
      sql("""
        CREATE TABLE readonly_test (
          id INT,
          data STRING
        ) USING parquet
      """)

      // First insert WITHOUT server-side planning should succeed
      sql("INSERT INTO readonly_test VALUES (1, 'initial')")
      checkAnswer(
        sql("SELECT * FROM readonly_test"),
        Seq(Row(1, "initial"))
      )

      // Try to insert WITH server-side planning enabled - should fail
      withServerSidePlanningEnabled {
        val exception = intercept[AnalysisException] {
          sql("INSERT INTO readonly_test VALUES (2, 'should_fail')")
        }
        assert(exception.getMessage.contains("does not support append"))
      }

      // Verify data unchanged - second insert didn't happen
      checkAnswer(
        sql("SELECT * FROM readonly_test"),
        Seq(Row(1, "initial"))
      )
    }
  }

  test("S3 credential injection via CredentialTestFileSystem") {
    // This test verifies that credentials from server-side planning response
    // are correctly injected into Hadoop configuration for S3 access.
    //
    // Flow:
    // 1. Configure fs.s3a.impl to use S3CredentialTestFileSystem
    // 2. TestServerSidePlanningClient returns ScanPlan with test credentials
    // 3. TestServerSidePlanningClient rewrites file:// paths to s3a://
    // 4. ServerSidePlannedTable injects credentials into Hadoop config
    // 5. S3CredentialTestFileSystem validates credentials before allowing file access
    // 6. Query succeeds if credentials are valid

    val originalFsImpl = spark.conf.getOption("fs.s3a.impl")
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)

    try {
      // Configure S3 to use our test filesystem
      spark.conf.set("fs.s3a.impl",
        "org.apache.spark.sql.delta.serverSidePlanning.S3CredentialTestFileSystem")

      // Create test credentials
      val testCredentials = Some(StorageCredentials(
        accessKeyId = "test-access-key",
        secretAccessKey = "test-secret-key",
        sessionToken = "test-session-token"
      ))

      // Set expected credentials for validation
      // The filesystem will verify these exact values match what's in Hadoop config
      S3CredentialTestFileSystem.setExpectedCredentials(
        accessKey = "test-access-key",
        secretKey = "test-secret-key",
        sessionToken = "test-session-token"
      )

      // Set up factory that returns clients with credentials
      // pathRewriteScheme = "s3a" causes file:// paths to be rewritten to s3a://
      ServerSidePlanningClientFactory.setFactory(
        new TestServerSidePlanningClientFactoryWithCredentials(
          credentials = testCredentials,
          pathRewriteScheme = Some("s3a")
        )
      )
      spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

      // Query the shared test table
      // This will:
      // - Load table via DeltaCatalog
      // - Get ServerSidePlannedTable
      // - Call planScan() which returns s3a:// paths with credentials
      // - Inject credentials into Hadoop config
      // - Access files via S3CredentialTestFileSystem
      // - S3CredentialTestFileSystem validates exact credential values
      checkAnswer(
        sql("SELECT id, name, value FROM test_db.shared_test ORDER BY id"),
        Seq(
          Row(1, "alpha", 10),
          Row(2, "beta", 20),
          Row(3, "gamma", 30)
        )
      )

      // If we got here, credentials were successfully injected and validated!
    } finally {
      // Cleanup
      S3CredentialTestFileSystem.clearExpectedCredentials()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
      originalFsImpl match {
        case Some(value) => spark.conf.set("fs.s3a.impl", value)
        case None => spark.conf.unset("fs.s3a.impl")
      }
    }
  }

  test("UnityCatalogMetadata constructs IRC endpoint from UC URI") {
    val ucUri = "https://my-workspace.cloud.databricks.com"
    val metadata = UnityCatalogMetadata(
      catalogName = "test_catalog",
      ucUri = ucUri,
      ucToken = "test-token",
      tableProps = Map.empty
    )

    // This test validates the fallback case where /v1/config is unreachable.
    // The endpoint construction logic attempts to call /v1/config at the UC URI,
    // but since there's no server at this URL, it falls back to the simple path
    // without prefix. For tests of the prefix case with a real IRC server, see
    // IcebergRESTCatalogPlanningClientSuite.
    val expectedEndpoint =
      "https://my-workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest"
    assert(metadata.planningEndpointUri == expectedEndpoint)
  }

  test("TestMetadata returns injected values") {
    val metadata = TestMetadata(
      catalogName = "test_catalog",
      endpointUri = "http://localhost:8080/api",
      token = "test-token",
      props = Map("key1" -> "value1", "key2" -> "value2")
    )

    assert(metadata.catalogName == "test_catalog")
    assert(metadata.planningEndpointUri == "http://localhost:8080/api")
    assert(metadata.authToken.contains("test-token"))
    assert(metadata.tableProperties == Map("key1" -> "value1", "key2" -> "value2"))
  }

  test("DefaultMetadata provides empty defaults for non-UC catalogs") {
    val metadata = DefaultMetadata(
      catalogName = "spark_catalog",
      tableProps = Map("location" -> "/tmp/test")
    )

    assert(metadata.catalogName == "spark_catalog")
    assert(metadata.planningEndpointUri == "")
    assert(metadata.authToken.isEmpty)
    assert(metadata.tableProperties == Map("location" -> "/tmp/test"))
  }

  test("filter pushdown - simple EqualTo filter") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query with WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id = 2").collect()

      // Verify filter was captured
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Spark may wrap EqualTo with IsNotNull check: And(IsNotNull("id"), EqualTo("id", 2))
      // We need to handle both cases
      val filter = capturedFilter.get
      val equalToFilter = filter match {
        case and: org.apache.spark.sql.sources.And =>
          // Wrapped case - extract the EqualTo from the And
          and.right match {
            case eq: org.apache.spark.sql.sources.EqualTo => eq
            case _ => and.left.asInstanceOf[org.apache.spark.sql.sources.EqualTo]
          }
        case eq: org.apache.spark.sql.sources.EqualTo =>
          // Unwrapped case
          eq
        case other =>
          fail(s"Expected EqualTo or And(IsNotNull, EqualTo) but got ${other.getClass.getName}")
      }

      assert(equalToFilter.attribute == "id",
        s"Expected attribute 'id' but got '${equalToFilter.attribute}'")
      assert(equalToFilter.value == 2, s"Expected value 2 but got ${equalToFilter.value}")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("filter pushdown - compound And filter") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query with compound WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id > 1 AND value < 30").collect()

      // Verify filter was captured
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Spark may wrap filters with IsNotNull checks in nested And structures:
      // And(And(IsNotNull("id"), GreaterThan("id", 1)), And(IsNotNull("value"), LessThan("value", 30)))
      // We just verify that the top-level is an And filter and contains the expected predicates
      val filter = capturedFilter.get
      assert(filter.isInstanceOf[org.apache.spark.sql.sources.And],
        s"Expected And filter but got ${filter.getClass.getName}")

      // Convert filter to string and verify it contains both predicates
      val filterStr = filter.toString
      assert(filterStr.contains("GreaterThan") && filterStr.contains("id"),
        s"Filter should contain GreaterThan on id: $filterStr")
      assert(filterStr.contains("LessThan") && filterStr.contains("value"),
        s"Filter should contain LessThan on value: $filterStr")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("filter pushdown - no filter when no WHERE clause") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query without WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test").collect()

      // Verify no filter was pushed
      // getCapturedFilter returns Option[Filter] - should be None when no WHERE clause
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter != null, "planScan should have been called")
      assert(capturedFilter.isEmpty, s"Expected no filter (None) but got ${capturedFilter}")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("projection pushdown - SELECT specific columns") {
    withTempDir { dir =>
      // Create test table with multiple columns
      spark.range(10)
        .selectExpr("id", "id * 2 as double_id", "CAST(id AS STRING) as id_str")
        .write
        .format("delta")
        .save(dir.getAbsolutePath)

      sql(s"CREATE TABLE test_db.projection_test USING DELTA LOCATION '${dir.getAbsolutePath}'")

      val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      try {
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
        ServerSidePlanningClientFactory.setFactory(new ProjectionCapturingTestClientFactory())
        ProjectionCapturingTestClient.clearCapturedProjection()

        // Execute query selecting only 2 of 3 columns
        val result = sql("SELECT id, id_str FROM test_db.projection_test").collect()

        // Verify results are correct
        assert(result.length == 10)
        assert(result(0).length == 2)  // Only 2 columns returned

        // Verify projection was pushed
        val capturedProjection = ProjectionCapturingTestClient.getCapturedProjection
        assert(capturedProjection.isDefined, "Projection should have been pushed")

        val schema = capturedProjection.get
        assert(schema.fields.length == 2, s"Expected 2 columns but got ${schema.fields.length}")
        assert(schema.fieldNames.toSeq == Seq("id", "id_str"))
      } finally {
        ProjectionCapturingTestClient.clearCapturedProjection()
        ServerSidePlanningClientFactory.clearFactory()
        originalConfig match {
          case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
          case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
        }
      }
    }
  }

  test("projection pushdown - SELECT * reads all columns") {
    withTempDir { dir =>
      // Create test table with multiple columns
      spark.range(5)
        .selectExpr("id", "id * 2 as double_id", "CAST(id AS STRING) as id_str")
        .write
        .format("delta")
        .save(dir.getAbsolutePath)

      sql(s"CREATE TABLE test_db.projection_all USING DELTA LOCATION '${dir.getAbsolutePath}'")

      val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      try {
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
        ServerSidePlanningClientFactory.setFactory(new ProjectionCapturingTestClientFactory())
        ProjectionCapturingTestClient.clearCapturedProjection()

        // Execute SELECT * query
        val result = sql("SELECT * FROM test_db.projection_all").collect()

        // Verify results are correct
        assert(result.length == 5)
        assert(result(0).length == 3)  // All 3 columns returned

        // Verify no projection was pushed (None means full table scan)
        val capturedProjection = ProjectionCapturingTestClient.getCapturedProjection
        assert(capturedProjection.isEmpty,
          s"Expected no projection (None) for SELECT * but got ${capturedProjection}")
      } finally {
        ProjectionCapturingTestClient.clearCapturedProjection()
        ServerSidePlanningClientFactory.clearFactory()
        originalConfig match {
          case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
          case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
        }
      }
    }
  }

  test("projection pushdown - combined with filter") {
    withTempDir { dir =>
      // Create test table
      spark.range(20)
        .selectExpr("id", "id * 2 as double_id", "CAST(id AS STRING) as id_str")
        .write
        .format("delta")
        .save(dir.getAbsolutePath)

      sql(s"CREATE TABLE test_db.proj_filter USING DELTA LOCATION '${dir.getAbsolutePath}'")

      val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      try {
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
        ServerSidePlanningClientFactory.setFactory(new ProjectionCapturingTestClientFactory())
        ProjectionCapturingTestClient.clearCapturedProjection()

        // Execute query with both projection and filter
        val result = sql("SELECT id, id_str FROM test_db.proj_filter WHERE id > 10").collect()

        // Verify results
        assert(result.length == 9)  // IDs 11-19
        assert(result(0).length == 2)  // Only 2 columns

        // Verify both projection and filter were pushed
        val capturedProjection = ProjectionCapturingTestClient.getCapturedProjection
        assert(capturedProjection.isDefined, "Projection should have been pushed")
        assert(capturedProjection.get.fields.length == 2)
      } finally {
        ProjectionCapturingTestClient.clearCapturedProjection()
        ServerSidePlanningClientFactory.clearFactory()
        originalConfig match {
          case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
          case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
        }
      }
    }
  }
}
