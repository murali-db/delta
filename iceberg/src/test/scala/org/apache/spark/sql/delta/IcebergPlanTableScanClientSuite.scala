package org.apache.spark.sql.delta


import scala.collection.immutable.Map
import scala.jdk.CollectionConverters._

import io.delta.scan.RESTIcebergTableClient
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.apache.hadoop.conf.Configuration
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.rest.{HTTPHeaders, HTTPRequest, ImmutableHTTPRequest, ResourcePaths}
import shadedForDelta.org.apache.iceberg.inmemory.InMemoryCatalog
import shadedForDelta.org.apache.iceberg.{CatalogProperties, PartitionSpec, Schema, Table}
import shadedForDelta.org.apache.iceberg.rest.{HTTPClient, HTTPHeaders, RESTCatalog, RESTUtil}
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogServer
import shadedForDelta.org.apache.iceberg.catalog.SessionCatalog
import shadedForDelta.org.apache.iceberg.jdbc.JdbcCatalog
import shadedForDelta.org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
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
      Thread.sleep(60 * 1000)
      val icebergClient = new RESTIcebergTableClient(serverUri, null)
      icebergClient.planTableScan(defaultNamespace.toString, "testTable")
      // scalastyle:on println
    }
  }

  private def startServer(): RESTCatalogServer = {
    var serverStarted = false
    var attemptCountLeft = 3
    var server: RESTCatalogServer = null
    while (!serverStarted && attemptCountLeft > 0) {
      attemptCountLeft += 1
      val port = 0
      val config = Map(RESTCatalogServer.REST_PORT -> "0").asJava
      server = new RESTCatalogServer(config)
      try {
        server.start(/* join = */ false)
        serverStarted = isServerReachable(server)
      } finally {
        if (server != null && !serverStarted) server.stop()
      }
    }
    if (!serverStarted) throw new IllegalStateException("Failed to start RESTCatalogServer")
    server
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

  /*

  private var restCatalog: RESTCatalog = null
  private var backendCatalog: InMemoryCatalog = null
  private var httpServer: Server = null

  def initCatalog(): Unit = {
    val warehouse = File.createTempFile("warehouse", "")
    this.backendCatalog = new InMemoryCatalog
    this.backendCatalog.initialize("in-memory",
      ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath))
    val catalogHeaders = HTTPHeaders.of(util.Map.of(
      "Authorization", "Bearer client-credentials-token:sub=catalog", "test-header", "test-value"))
    val contextHeaders = HTTPHeaders.of(
      util.Map.of("Authorization", "Bearer client-credentials-token:sub=user",
        "test-header", "test-value"))
    val adaptor = new RESTCatalogAdapter(backendCatalog) {

    }
    val servletContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
   */

    // servletContext.addServlet(new ServletHolder(new RESTCatalogServlet(adaptor)), "/*")
    /*
    servletContext.setHandler(new GzipHandler)

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    httpServer.setHandler(servletContext)
    httpServer.start()
    this.restCatalog = initRESTCatalog("prod", ImmutableMap.of)
  }


  private def initRESTCatalog(
      catalogName: String,
      additionalProperties: util.Map[String, String]): RESTCatalog = {
    val conf: Configuration = new Configuration
    val context: SessionCatalog.SessionContext =
      new SessionCatalog.SessionContext(
        UUID.randomUUID.toString,
        "user",
        ImmutableMap.of("credential", "user:12345"), ImmutableMap.of)

    val restCatalog: RESTCatalog = new RESTCatalog(
      context,
      (config: util.Map[String, String]) =>
        HTTPClient.builder(config)
          .uri(config.get(CatalogProperties.URI))
          .withHeaders(RESTUtil.configHeaders(config))
          .build())
    restCatalog.setConf(conf)
    val properties: util.Map[String, String] = ImmutableMap.of(
      CatalogProperties.URI, httpServer.getURI.toString,
      CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO",
      CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1",
      CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2",
      CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3", "catalog-default-key3",
      CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3", "catalog-override-key3",
      CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4", "catalog-override-key4",
      "credential", "catalog:12345", "header.test-header", "test-value")
    restCatalog.initialize(
      catalogName,
      ImmutableMap.builder[String, String].putAll(properties).putAll(additionalProperties).build)
    restCatalog
  }

   */
}


