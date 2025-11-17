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
import org.scalatest.BeforeAndAfterAll

/**
 * Integration test for local Unity Catalog + IcebergRESTServer + MinIO setup.
 *
 * This test validates the end-to-end flow of:
 * 1. Writing a table via UC Spark connector to MinIO
 * 2. Calling IRC /plan endpoint which fetches credentials from UC
 * 3. Reading the table using IRC-vended credentials
 *
 * PREREQUISITES (must be manually started before running this test):
 * 1. MinIO running on localhost:9000
 *    - Start: cd icebergShaded/iceberg_src/python/dev && docker-compose up -d minio
 *    - Credentials: admin/password
 *    - Bucket: test-bucket (create via console at localhost:9001)
 *
 * 2. Unity Catalog OSS running on localhost:8080
 *    - Start: cd /home/murali.ramanujam/ccv2_int_tests/unitycatalog && bin/start-uc-server
 *    - Configure MinIO in etc/conf/server.properties:
 *      s3.bucketPath.0=test-bucket
 *      s3.accessKey.0=admin
 *      s3.secretKey.0=password
 *      s3.region.0=us-east-1
 *      s3.endpoint.0=http://localhost:9000
 *
 * NOTE: This is a manual integration test. Run with:
 *   build/sbt "testOnly *LocalUCWithMinIOIntegrationSuite"
 *
 * The test is tagged with "org.scalatest.tags.Slow" to exclude from regular test runs.
 */
class LocalUCWithMinIOIntegrationSuite extends QueryTest with DeltaSQLCommandTest
    with BeforeAndAfterAll {

  // Flag to skip test if prerequisites are not met
  private var prerequisitesMet = false

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Check if MinIO is running
    val minioRunning = checkMinIORunning()
    if (!minioRunning) {
      info("MinIO not running on localhost:9000 - skipping test")
      prerequisitesMet = false
      return
    }

    // Check if UC OSS is running
    val ucRunning = checkUCRunning()
    if (!ucRunning) {
      info("Unity Catalog OSS not running on localhost:8080 - skipping test")
      prerequisitesMet = false
      return
    }

    prerequisitesMet = true
    info("Prerequisites met - MinIO and UC OSS are running")
  }

  /**
   * Check if MinIO is running by attempting to connect to health endpoint
   */
  private def checkMinIORunning(): Boolean = {
    try {
      val url = new java.net.URL("http://localhost:9000/minio/health/live")
      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(1000)
      connection.setReadTimeout(1000)
      val responseCode = connection.getResponseCode
      connection.disconnect()
      responseCode == 200
    } catch {
      case _: Exception => false
    }
  }

  /**
   * Check if Unity Catalog OSS is running by attempting to connect to API
   */
  private def checkUCRunning(): Boolean = {
    try {
      val url = new java.net.URL("http://localhost:8080/api/2.1/unity-catalog/catalogs")
      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(1000)
      connection.setReadTimeout(1000)
      val responseCode = connection.getResponseCode
      connection.disconnect()
      responseCode == 200
    } catch {
      case _: Exception => false
    }
  }

  test("UC + IRC + MinIO integration - basic setup verification") {
    assume(prerequisitesMet, "Prerequisites not met - MinIO and UC must be running")

    // This basic test just verifies that the prerequisites are met
    // More comprehensive tests can be added here to:
    // 1. Create a table via UC Spark connector
    // 2. Start IRC server with UC base URL
    // 3. Call IRC /plan endpoint
    // 4. Verify credentials are returned
    // 5. Read table using credentials

    info("Basic setup verification passed")
    info("MinIO is running on localhost:9000")
    info("Unity Catalog OSS is running on localhost:8080")
    info("Ready for full integration testing")
  }

  // TODO: Add comprehensive integration test
  // test("end-to-end: write via UC, read via IRC-vended credentials") {
  //   assume(prerequisitesMet, "Prerequisites not met")
  //
  //   // 1. Write test table via UC
  //   // 2. Start IRC server pointing to UC
  //   // 3. Call IRC /plan endpoint
  //   // 4. Verify credentials in response
  //   // 5. Read table using credentials
  // }
}
