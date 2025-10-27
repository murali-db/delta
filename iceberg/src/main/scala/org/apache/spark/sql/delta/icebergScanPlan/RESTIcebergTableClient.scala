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

package org.apache.spark.sql.delta.icebergScanPlan

import java.io.IOException
import java.lang.reflect.Method
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.rest.requests.{PlanTableScanRequest, PlanTableScanRequestParser}
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse

/**
 * REST implementation of IcebergTableClient that calls Iceberg REST catalog server.
 * This class lives in the iceberg module where Iceberg libraries are available.
 */
class RESTIcebergTableClient(
    icebergRestCatalogUriRoot: String,
    token: String) extends IcebergTableClient {

  private val httpHeaders = Map(
    // HttpHeaders.AUTHORIZATION -> s"Bearer $token",
    HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
    HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
  ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

  private lazy val httpClient = HttpClientBuilder.create()
    .setDefaultHeaders(httpHeaders)
    .build();

  override def planTableScan(namespace: String, table: String): ScanPlan = {
    val planTableScanUri =
      s"$icebergRestCatalogUriRoot/v1/namespaces/$namespace/tables/$table/plan"
    val request = new PlanTableScanRequest.Builder().withSnapshotId(0).build()

    val requestJson = PlanTableScanRequestParser.toJson(request)
    val httpPost = new HttpPost(planTableScanUri)
    httpPost.setEntity(new StringEntity(requestJson, ContentType.APPLICATION_JSON))
    val httpResponse = httpClient.execute(httpPost)
    val partitionSpecById = Map(0 -> PartitionSpec.unpartitioned())

    try {
      val statusCode = httpResponse.getStatusLine.getStatusCode
      val responseBody = EntityUtils.toString(httpResponse.getEntity)
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        val icebergResponse = parsePlanTableScanResponse(
          responseBody, partitionSpecById, caseSensitive = true)
        convertToScanPlan(icebergResponse)
      } else {
        throw new IOException(s"Failed to plan table scan. Status code: $statusCode, " +
          s"Response body: $responseBody")
      }
    } finally {
      httpResponse.close()
    }
  }

  /**
   * Convert Iceberg PlanTableScanResponse to simple ScanPlan data class.
   */
  private def convertToScanPlan(response: PlanTableScanResponse): ScanPlan = {
    val files = response.fileScanTasks().asScala.map { task =>
      ScanFile(
        filePath = task.file().path().toString,
        fileSizeInBytes = task.file().fileSizeInBytes(),
        fileFormat = task.file().format().toString.toLowerCase(Locale.ROOT),
        partitionData = Map.empty  // TODO: Extract partition data if needed
      )
    }.toSeq

    // TODO: Extract schema from response properly
    // For now, return empty JSON object as placeholder
    val schemaJson = "{}"

    ScanPlan(files = files, schema = schemaJson)
  }

  private def parsePlanTableScanResponse(
    json: String,
    specsById: Map[Int, PartitionSpec],
    caseSensitive: Boolean): PlanTableScanResponse = {

    // scalastyle:off classforname
    val parserClass = Class.forName(
      "shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponseParser")
    // scalastyle:on classforname

    val fromJsonMethod: Method = parserClass.getDeclaredMethod(
      "fromJson",
      classOf[String],
      classOf[java.util.Map[_, _]],
      classOf[Boolean])

    fromJsonMethod.setAccessible(true)

    fromJsonMethod.invoke(
      null,  // static method
      json,
      specsById.map { case (k, v) => Int.box(k) -> v }.asJava,
      Boolean.box(caseSensitive)
    ).asInstanceOf[PlanTableScanResponse]
  }
}
