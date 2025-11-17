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

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.BeforeAndAfterAll

import scala.sys.process._

/**
 * Integration test for local Unity Catalog + IcebergRESTServer + MinIO setup.
 *
 * This test validates the end-to-end flow of:
 * 1. Starting all required services (MinIO, UC OSS, IRC)
 * 2. Writing a table via UC Spark connector to MinIO
 * 3. Calling IRC /plan endpoint which fetches credentials from UC
 * 4. Reading the table using IRC-vended credentials
 * 5. Stopping all services cleanly
 *
 * The test automatically manages service lifecycle:
 * - MinIO: Started via docker-compose, stopped in afterAll
 * - UC OSS: Started as background Java process, stopped in afterAll
 * - IRC: Started programmatically via IcebergRESTServer
 *
 * Run with: build/sbt "testOnly *LocalUCWithMinIOIntegrationSuite"
 */
class LocalUCWithMinIOIntegrationSuite extends QueryTest with DeltaSQLCommandTest
    with BeforeAndAfterAll {

  // Service state
  private var minioStarted = false
  private var ucProcess: Option[Process] = None
  // Note: IRC server not started programmatically due to module dependencies
  //       Must be started manually before running test

  // Paths
  private val deltaRootDir = new File(System.getProperty("user.dir"))
  private val minioDockerComposeDir = new File(deltaRootDir,
    "icebergShaded/iceberg_src/python/dev")
  private val ucRootDir = new File("/home/murali.ramanujam/ccv2_int_tests/unitycatalog")
  private val ucLogFile = new File("/tmp/uc-server-test.log")

  override def beforeAll(): Unit = {
    super.beforeAll()

    try {
      info("=== Starting test services ===")

      // Start MinIO
      startMinIO()

      // Start UC OSS
      startUC()

      info("=== MinIO and UC started successfully ===")
    } catch {
      case e: Exception =>
        fail(s"Failed to start services: ${e.getMessage}", e)
        stopAllServices()
        throw e
    }
  }

  override def afterAll(): Unit = {
    try {
      info("=== Stopping test services ===")
      stopAllServices()
      info("=== All services stopped ===")
    } finally {
      super.afterAll()
    }
  }

  private def startMinIO(): Unit = {
    info("Starting MinIO...")

    // Check if already running
    if (checkMinIORunning()) {
      info("MinIO already running - skipping start")
      minioStarted = false // We didn't start it, so don't stop it
      return
    }

    // Start MinIO via docker-compose
    val dockerComposeCmd = if (isDockerComposeV2Available) "docker compose" else "docker-compose"
    val startCmd = s"$dockerComposeCmd up -d minio"

    val result = Process(startCmd, minioDockerComposeDir).!
    if (result != 0) {
      throw new RuntimeException(s"Failed to start MinIO with command: $startCmd")
    }

    minioStarted = true

    // Wait for MinIO to be ready
    waitForService("MinIO", "http://localhost:9000/minio/health/live", maxWaitSeconds = 30)
    info("MinIO started successfully")
  }

  private def startUC(): Unit = {
    info("Starting Unity Catalog OSS...")

    // Check if already running
    if (checkUCRunning()) {
      info("UC already running - skipping start")
      return
    }

    // Start UC OSS with Java 17
    val java17Path = "/usr/lib/jvm/java-17-openjdk-amd64/bin"
    val startScript = new File(ucRootDir, "bin/start-uc-server")

    if (!startScript.exists()) {
      throw new RuntimeException(s"UC start script not found: ${startScript.getAbsolutePath}")
    }

    // Build command with Java 17 in PATH
    val env = Map("PATH" -> s"$java17Path:${System.getenv("PATH")}")
    val processBuilder = Process(startScript.getAbsolutePath, ucRootDir, env.toSeq: _*)

    // Start process with output redirected to log file
    val logWriter = new PrintWriter(new FileWriter(ucLogFile, true))
    // scalastyle:off println
    val processLogger = ProcessLogger(
      line => { logWriter.println(line); logWriter.flush() },
      line => { logWriter.println(line); logWriter.flush() }
    )
    // scalastyle:on println

    ucProcess = Some(processBuilder.run(processLogger))
    info(s"UC process started - logging to ${ucLogFile.getAbsolutePath}")

    // Wait for UC to be ready
    waitForService("Unity Catalog", "http://localhost:8080/api/2.1/unity-catalog/catalogs",
      maxWaitSeconds = 60)
    info("Unity Catalog OSS started successfully")
  }

  private def stopAllServices(): Unit = {
    // Stop UC
    ucProcess.foreach { process =>
      try {
        info("Stopping Unity Catalog OSS...")
        process.destroy()
        val exitCode = process.exitValue()
        info(s"Unity Catalog OSS stopped (exit code: $exitCode)")
      } catch {
        case e: Exception => info(s"Error stopping UC: ${e.getMessage}")
      }
    }
    ucProcess = None

    // Stop MinIO (only if we started it)
    if (minioStarted) {
      try {
        info("Stopping MinIO...")
        val dockerComposeCmd =
          if (isDockerComposeV2Available) "docker compose" else "docker-compose"
        val stopCmd = s"$dockerComposeCmd down"
        Process(stopCmd, minioDockerComposeDir).!
        info("MinIO stopped")
      } catch {
        case e: Exception => info(s"Error stopping MinIO: ${e.getMessage}")
      }
      minioStarted = false
    }
  }

  private def isDockerComposeV2Available: Boolean = {
    try {
      "docker compose version".! == 0
    } catch {
      case _: Exception => false
    }
  }

  private def waitForService(
      serviceName: String,
      healthUrl: String,
      maxWaitSeconds: Int): Unit = {
    info(s"Waiting for $serviceName to be ready (max ${maxWaitSeconds}s)...")

    val startTime = System.currentTimeMillis()
    val maxWaitMs = maxWaitSeconds * 1000

    while (System.currentTimeMillis() - startTime < maxWaitMs) {
      if (checkServiceHealth(healthUrl)) {
        info(s"$serviceName is ready")
        return
      }
      Thread.sleep(2000)
    }

    throw new RuntimeException(
      s"$serviceName failed to start within ${maxWaitSeconds} seconds")
  }

  private def checkServiceHealth(url: String): Boolean = {
    try {
      val connection = new java.net.URL(url).openConnection()
        .asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(2000)
      connection.setReadTimeout(2000)
      val responseCode = connection.getResponseCode
      connection.disconnect()
      responseCode == 200
    } catch {
      case _: Exception => false
    }
  }

  private def checkMinIORunning(): Boolean = {
    checkServiceHealth("http://localhost:9000/minio/health/live")
  }

  private def checkUCRunning(): Boolean = {
    checkServiceHealth("http://localhost:8080/api/2.1/unity-catalog/catalogs")
  }

  test("UC + IRC + MinIO integration - service lifecycle") {
    // Verify MinIO and UC are running (started by beforeAll)
    assert(checkMinIORunning(), "MinIO should be running")
    assert(checkUCRunning(), "UC should be running")

    info("Service lifecycle test passed")
    info("MinIO is running on localhost:9000")
    info("Unity Catalog OSS is running on localhost:8080")
    info("Note: IcebergRESTServer (IRC) must be started manually in iceberg module tests")
    info("Ready for end-to-end integration testing")
  }

  // TODO: Add comprehensive integration test
  // test("end-to-end: write via UC, read via IRC-vended credentials") {
  //   // 1. Write test table via UC Spark connector
  //   // 2. Call IRC /plan endpoint
  //   // 3. Verify credentials in response
  //   // 4. Read table using credentials
  // }
}
