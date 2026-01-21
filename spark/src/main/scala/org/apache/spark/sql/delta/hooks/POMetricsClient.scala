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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession

/**
 * Case class representing the metrics payload to send to the PO endpoint.
 *
 * @param tableId The unique identifier for the table (from DeltaLog.tableId)
 * @param tableName The fully qualified table name (catalog.schema.table)
 * @param version The Delta table version after the commit
 * @param timestamp The commit timestamp in milliseconds
 * @param operation The operation type (e.g., WRITE, DELETE, MERGE)
 * @param metrics The file and row-level metrics
 * @param partitionInfo Optional partition information (map of column name to set of values)
 */
case class POMetricsPayload(
    tableId: String,
    tableName: String,
    version: Long,
    timestamp: Long,
    operation: String,
    metrics: POMetrics,
    partitionInfo: Option[Map[String, Set[String]]])

/**
 * Case class representing file and row-level metrics.
 *
 * @param numFilesAdded Number of files added in the commit
 * @param numBytesAdded Total bytes added in the commit
 * @param numFilesRemoved Number of files removed in the commit
 * @param numBytesRemoved Total bytes removed in the commit
 * @param numRowsAdded Number of rows added in the commit (from numLogicalRecords)
 * @param numRowsRemoved Number of rows removed in the commit (from numLogicalRecords)
 */
case class POMetrics(
    numFilesAdded: Long,
    numBytesAdded: Long,
    numFilesRemoved: Long,
    numBytesRemoved: Long,
    numRowsAdded: Long,
    numRowsRemoved: Long)

/**
 * HTTP client for sending commit metrics to the PO endpoint.
 *
 * This client is responsible for:
 * - Building and sending HTTP POST requests to the UC PO metrics endpoint
 * - Handling authentication via bearer token
 * - Managing timeouts and error handling
 * - Best-effort delivery (failures should not block commits)
 */
object POMetricsClient {

  /**
   * Sends commit metrics to the PO endpoint synchronously.
   *
   * @param spark The SparkSession (used to read configuration)
   * @param payload The metrics payload to send
   * @throws Exception if the HTTP request fails (caller should handle gracefully)
   */
  def sendMetrics(spark: SparkSession, payload: POMetricsPayload): Unit = {
    val endpointUrl = getEndpointUrl(spark)
    val authToken = getAuthToken(spark)
    val timeoutMs = getTimeoutMs(spark)

    // Build HTTP client with timeout configuration
    val requestConfig = RequestConfig.custom()
      .setConnectTimeout(timeoutMs.toInt)
      .setSocketTimeout(timeoutMs.toInt)
      .setConnectionRequestTimeout(timeoutMs.toInt)
      .build()

    val httpClient: CloseableHttpClient = HttpClientBuilder.create()
      .setDefaultRequestConfig(requestConfig)
      .build()

    try {
      // Create POST request
      val httpPost = new HttpPost(endpointUrl)

      // Set headers
      httpPost.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType)
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $authToken")

      // Serialize payload to JSON
      val jsonPayload = JsonUtils.toJson(payload)
      httpPost.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))

      // Execute request
      val response = httpClient.execute(httpPost)

      try {
        val statusCode = response.getStatusLine.getStatusCode

        // Check for successful response (2xx)
        if (statusCode < 200 || statusCode >= 300) {
          val responseBody = if (response.getEntity != null) {
            EntityUtils.toString(response.getEntity)
          } else {
            "<no response body>"
          }
          throw new RuntimeException(
            s"PO metrics endpoint returned error status $statusCode: $responseBody")
        }
      } finally {
        response.close()
      }
    } finally {
      httpClient.close()
    }
  }

  /**
   * Gets the PO metrics endpoint URL from Spark configuration.
   *
   * @param spark The SparkSession
   * @return The endpoint URL
   * @throws IllegalArgumentException if the endpoint URL is not configured
   */
  private def getEndpointUrl(spark: SparkSession): String = {
    spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key) match {
      case Some(url) if url.nonEmpty => url
      case _ =>
        val key = DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key
        throw new IllegalArgumentException(
          s"PO metrics endpoint URL not configured. Set $key")
    }
  }

  /**
   * Gets the authentication token from Spark configuration or environment variable.
   *
   * @param spark The SparkSession
   * @return The authentication token
   * @throws IllegalArgumentException if no token is available
   */
  private def getAuthToken(spark: SparkSession): String = {
    // Try Spark config first, then fall back to environment variable
    spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key)
      .orElse(Option(System.getenv("DATABRICKS_TOKEN"))) match {
      case Some(token) if token.nonEmpty => token
      case _ =>
        val key = DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key
        throw new IllegalArgumentException(
          s"PO metrics auth token not configured. Set $key or " +
          "DATABRICKS_TOKEN environment variable")
    }
  }

  /**
   * Gets the HTTP request timeout from Spark configuration.
   *
   * @param spark The SparkSession
   * @return The timeout in milliseconds
   */
  private def getTimeoutMs(spark: SparkSession): Long = {
    spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_TIMEOUT_MS)
  }
}
