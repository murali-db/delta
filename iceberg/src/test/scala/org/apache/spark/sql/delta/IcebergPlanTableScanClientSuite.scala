package org.apache.spark.sql.delta

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.icebergScanPlan.RESTIcebergTableClient
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
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogServer
import shadedForDelta.org.apache.iceberg.types.Types

class IcebergPlanTableScanClientSuite extends AnyFunSuite with BeforeAndAfterAll {

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

  test("basic plan table scan via RESTIcebergTableClient") {
    withTempTable("testTable") { table =>
      // scalastyle:off println
      println("Table created: " + table)
      val icebergClient = new RESTIcebergTableClient(serverUri, null)
      val scanPlan = icebergClient.planTableScan(defaultNamespace.toString, "testTable")
      println("Scan plan received with " + scanPlan.files.length + " files")
      println("Schema: " + scanPlan.schema)
      // Verify we get a valid scan plan back
      assert(scanPlan != null)
      assert(scanPlan.files != null)
      // scalastyle:on println
    }
  }

  private def startServer(): RESTCatalogServer = {
    val config = Map(RESTCatalogServer.REST_PORT -> "0").asJava
    val newServer = new RESTCatalogServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start RESTCatalogServer")
    }
    println("Server started on port " + newServer.getPort)
    newServer
  }

  private def isServerReachable(server: RESTCatalogServer): Boolean = {
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


