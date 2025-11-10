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

package org.apache.spark.sql.delta

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClient
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Schema, Table}
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer
import shadedForDelta.org.apache.iceberg.types.Types

class IcebergRESTCatalogPlanningClientSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get),
    Types.NestedField.required(2, "name", Types.StringType.get))
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces].createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException("Catalog does not support namespaces")
    }
  }

  test("basic plan table scan via IcebergRESTCatalogPlanningClient") {
    withTempTable("testTable") { table =>
      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      val scanPlan = client.planScan(defaultNamespace.toString, "testTable")
      // Verify we get a valid scan plan back
      assert(scanPlan != null)
      assert(scanPlan.files != null)
      // Empty table should have 0 files
      assert(scanPlan.files.isEmpty, s"Expected 0 files for empty table, got ${scanPlan.files.length}")
    }
  }

  private def startServer(): IcebergRESTServer = {
    val config = Map(IcebergRESTServer.REST_PORT -> "0").asJava
    val newServer = new IcebergRESTServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start IcebergRESTServer")
    }
    newServer
  }

  private def isServerReachable(server: IcebergRESTServer): Boolean = {
    val httpHeaders = Map(
      // HttpHeaders.AUTHORIZATION -> s"Bearer $token",
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

    val httpClient = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .build();
    val httpGet = new HttpGet(s"http://localhost:${server.getPort}/v1/config")
    val httpResponse = httpClient.execute(httpGet)
    val statusCode = httpResponse.getStatusLine.getStatusCode
    httpResponse.close()
    statusCode == 200
  }

  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    val tableId = TableIdentifier.of(defaultNamespace, tableName)
    val table = catalog.createTable(tableId, defaultSchema, defaultSpec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
    }
  }
}
