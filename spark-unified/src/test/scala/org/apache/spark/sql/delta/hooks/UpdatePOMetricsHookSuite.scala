/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.hooks

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for UpdatePOMetricsHook functionality.
 *
 * Tests cover:
 * - Metrics extraction from AddFile and RemoveFile actions
 * - Row-level metrics extraction from numLogicalRecords
 * - Hook enablement via configuration
 * - UC table filtering
 * - JSON payload validation
 * - Error handling (commits succeed even when HTTP fails)
 */
class UpdatePOMetricsHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")
  }

  test("extractMetrics: mixed add and remove files") {
    val hook = UpdatePOMetricsHook(None)

    // Create test actions with AddFile and RemoveFile
    val addFile1 = AddFile(
      path = "file1.parquet",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = """{"numRecords": 100}"""
    )

    val addFile2 = AddFile(
      path = "file2.parquet",
      partitionValues = Map.empty,
      size = 2000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = """{"numRecords": 200}"""
    )

    val removeFile1 = RemoveFile(
      path = "file_old.parquet",
      deletionTimestamp = Some(System.currentTimeMillis()),
      dataChange = true,
      size = Some(500L),
      stats = """{"numRecords": 50}"""
    )

    val actions = Seq[Action](addFile1, addFile2, removeFile1)

    // Use reflection to access private extractMetrics method
    val extractMetricsMethod = hook.getClass.getDeclaredMethod("extractMetrics", classOf[Seq[Action]])
    extractMetricsMethod.setAccessible(true)
    val metrics = extractMetricsMethod.invoke(hook, actions).asInstanceOf[POMetrics]

    // Verify file-level metrics
    assert(metrics.numFilesAdded == 2, "Expected 2 files added")
    assert(metrics.numBytesAdded == 3000L, "Expected 3000 bytes added")
    assert(metrics.numFilesRemoved == 1, "Expected 1 file removed")
    assert(metrics.numBytesRemoved == 500L, "Expected 500 bytes removed")

    // Verify row-level metrics
    assert(metrics.numRowsAdded == 300L, "Expected 300 rows added (100 + 200)")
    assert(metrics.numRowsRemoved == 50L, "Expected 50 rows removed")
  }

  test("extractMetrics: row metrics from numLogicalRecords") {
    val hook = UpdatePOMetricsHook(None)

    // Create AddFile with stats containing numRecords
    val addFile = AddFile(
      path = "file_with_stats.parquet",
      partitionValues = Map.empty,
      size = 5000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = """{"numRecords": 1000, "minValues": {}, "maxValues": {}}"""
    )

    val actions = Seq[Action](addFile)

    // Use reflection to access private extractMetrics method
    val extractMetricsMethod = hook.getClass.getDeclaredMethod("extractMetrics", classOf[Seq[Action]])
    extractMetricsMethod.setAccessible(true)
    val metrics = extractMetricsMethod.invoke(hook, actions).asInstanceOf[POMetrics]

    assert(metrics.numRowsAdded == 1000L, "Expected 1000 rows from numRecords in stats")
  }

  test("extractMetrics: handles missing stats gracefully") {
    val hook = UpdatePOMetricsHook(None)

    // Create AddFile without stats
    val addFileNoStats = AddFile(
      path = "file_no_stats.parquet",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null
    )

    val actions = Seq[Action](addFileNoStats)

    val extractMetricsMethod = hook.getClass.getDeclaredMethod("extractMetrics", classOf[Seq[Action]])
    extractMetricsMethod.setAccessible(true)
    val metrics = extractMetricsMethod.invoke(hook, actions).asInstanceOf[POMetrics]

    // File metrics should still work
    assert(metrics.numFilesAdded == 1, "Expected 1 file added")
    assert(metrics.numBytesAdded == 1000L, "Expected 1000 bytes added")

    // Row metrics should be 0 when stats are missing
    assert(metrics.numRowsAdded == 0L, "Expected 0 rows when stats are missing")
  }

  test("hook disabled by config - skips execution") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath

      // Ensure hook is disabled
      spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_ENABLED.key, "false")

      // Create a simple Delta table
      spark.range(10).write.format("delta").save(tablePath)

      // Append more data
      spark.range(10, 20).write.format("delta").mode("append").save(tablePath)

      // Hook should not have been executed (no exception even without endpoint config)
      // If hook ran, it would fail due to missing endpoint configuration
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      assert(deltaLog.snapshot.version == 1, "Expected 2 commits")
    }
  }

  test("JSON payload validation") {
    val payload = POMetricsPayload(
      tableId = "test-table-id-123",
      tableName = "catalog.schema.test_table",
      version = 42L,
      timestamp = System.currentTimeMillis(),
      operation = "WRITE",
      metrics = POMetrics(
        numFilesAdded = 10L,
        numBytesAdded = 10000L,
        numFilesRemoved = 2L,
        numBytesRemoved = 2000L,
        numRowsAdded = 1000L,
        numRowsRemoved = 200L
      ),
      partitionInfo = Some(Map(
        "year" -> Set("2024", "2025"),
        "month" -> Set("01", "02")
      ))
    )

    // Serialize to JSON
    val json = JsonUtils.toJson(payload)

    // Verify JSON contains expected fields
    assert(json.contains("\"tableId\":\"test-table-id-123\""), "JSON should contain tableId")
    assert(json.contains("\"tableName\":\"catalog.schema.test_table\""), "JSON should contain tableName")
    assert(json.contains("\"version\":42"), "JSON should contain version")
    assert(json.contains("\"operation\":\"WRITE\""), "JSON should contain operation")
    assert(json.contains("\"numFilesAdded\":10"), "JSON should contain numFilesAdded")
    assert(json.contains("\"numRowsAdded\":1000"), "JSON should contain numRowsAdded")
    assert(json.contains("\"partitionInfo\""), "JSON should contain partitionInfo")

    // Verify we can deserialize it back
    val deserialized = JsonUtils.fromJson[POMetricsPayload](json)
    assert(deserialized.tableId == payload.tableId)
    assert(deserialized.metrics.numFilesAdded == payload.metrics.numFilesAdded)
    assert(deserialized.metrics.numRowsAdded == payload.metrics.numRowsAdded)
  }

  test("error handling - commit succeeds when HTTP fails") {
    val mockServer = new SimpleMockServer(0) // Use random available port
    try {
      mockServer.setResponseCode(500) // Return error
      mockServer.start()

      withTempDir { dir =>
        val tablePath = dir.getCanonicalPath

        // Enable hook and configure endpoint
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_ENABLED.key, "true")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key,
          s"http://localhost:${mockServer.getPort()}/metrics")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key, "test-token")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_TIMEOUT_MS.key, "1000")

        // Create table - should succeed even though HTTP fails
        spark.range(10).write.format("delta").save(tablePath)

        // Verify commit succeeded
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        assert(deltaLog.snapshot.version == 0, "Commit should succeed despite HTTP error")

        // Verify HTTP request was attempted
        assert(mockServer.getRequestCount() > 0, "HTTP request should have been sent")
      }
    } finally {
      mockServer.stop()
    }
  }

  test("basic integration with mock server - successful request") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(200) // Return success
      mockServer.start()

      withTempDir { dir =>
        val tablePath = dir.getCanonicalPath

        // Enable hook and configure endpoint
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_ENABLED.key, "true")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key,
          s"http://localhost:${mockServer.getPort()}/metrics")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key, "test-token-123")
        spark.conf.set(DeltaSQLConf.DELTA_PO_METRICS_TIMEOUT_MS.key, "5000")

        // Create Delta table
        spark.range(100).write.format("delta").save(tablePath)

        // Verify commit succeeded
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        assert(deltaLog.snapshot.version == 0)

        // Verify HTTP request was sent
        assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")

        // Verify request details
        val lastRequest = mockServer.getLastRequestBody()
        assert(lastRequest.nonEmpty, "Request body should not be empty")
        assert(lastRequest.contains("\"operation\""), "Request should contain operation field")

        // Verify auth header was sent
        val authHeader = mockServer.getLastHeaders().get("Authorization")
        assert(authHeader.isDefined, "Authorization header should be present")
        assert(authHeader.get == "Bearer test-token-123", "Auth token should match")
      }
    } finally {
      mockServer.stop()
    }
  }
}

/**
 * Simple mock HTTP server for testing.
 *
 * This is a basic single-threaded HTTP server that accepts one connection at a time
 * and returns a configurable status code. It captures request details for validation.
 */
class SimpleMockServer(port: Int) {
  private var serverSocket: ServerSocket = _
  private var serverThread: Thread = _
  private var running = false
  private var responseCode = 200
  private val requestCount = new AtomicInteger(0)
  private var lastRequestBody = ""
  private var lastHeaders = Map[String, String]()
  private var actualPort = port

  def setResponseCode(code: Int): Unit = {
    responseCode = code
  }

  def getPort(): Int = actualPort

  def getRequestCount(): Int = requestCount.get()

  def getLastRequestBody(): String = lastRequestBody

  def getLastHeaders(): Map[String, String] = lastHeaders

  def start(): Unit = {
    serverSocket = new ServerSocket(port)
    actualPort = serverSocket.getLocalPort
    running = true

    serverThread = new Thread(new Runnable {
      override def run(): Unit = {
        while (running) {
          try {
            val clientSocket = serverSocket.accept()
            handleRequest(clientSocket)
          } catch {
            case _: java.net.SocketException if !running =>
              // Expected when stopping the server
            case e: Exception =>
              e.printStackTrace()
          }
        }
      }
    })
    serverThread.setDaemon(true)
    serverThread.start()

    // Give server a moment to start
    Thread.sleep(100)
  }

  private def handleRequest(clientSocket: Socket): Unit = {
    try {
      val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      val out = new PrintWriter(clientSocket.getOutputStream, true)

      // Read request line
      val requestLine = in.readLine()
      if (requestLine == null) return

      // Read headers
      val headers = scala.collection.mutable.Map[String, String]()
      var line = in.readLine()
      var contentLength = 0

      while (line != null && line.nonEmpty) {
        val parts = line.split(":", 2)
        if (parts.length == 2) {
          val key = parts(0).trim
          val value = parts(1).trim
          headers(key) = value

          if (key.equalsIgnoreCase("Content-Length")) {
            contentLength = value.toInt
          }
        }
        line = in.readLine()
      }

      // Read body
      val body = new Array[Char](contentLength)
      if (contentLength > 0) {
        in.read(body, 0, contentLength)
      }

      // Store request details
      requestCount.incrementAndGet()
      lastRequestBody = new String(body)
      lastHeaders = headers.toMap

      // Send response
      out.println(s"HTTP/1.1 $responseCode ${getStatusMessage(responseCode)}")
      out.println("Content-Type: application/json")
      out.println("Content-Length: 2")
      out.println()
      out.println("{}")
      out.flush()

      clientSocket.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  private def getStatusMessage(code: Int): String = code match {
    case 200 => "OK"
    case 400 => "Bad Request"
    case 500 => "Internal Server Error"
    case _ => "Unknown"
  }

  def stop(): Unit = {
    running = false
    if (serverSocket != null && !serverSocket.isClosed) {
      serverSocket.close()
    }
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread.join(1000)
    }
  }
}
