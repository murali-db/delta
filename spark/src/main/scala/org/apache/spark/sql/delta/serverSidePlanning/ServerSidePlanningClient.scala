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

import org.apache.spark.sql.SparkSession

/**
 * Simple data class representing a file to scan.
 * No dependencies on Iceberg types.
 */
case class ScanFile(
  filePath: String,
  fileSizeInBytes: Long,
  fileFormat: String  // "parquet", "orc", etc.
)

/**
 * Result of a table scan plan operation.
 */
case class ScanPlan(
  files: Seq[ScanFile]
)

/**
 * Interface for planning table scans via server-side planning (e.g., Iceberg REST catalog).
 * This interface is intentionally simple and has no dependencies
 * on Iceberg libraries, allowing it to live in delta-spark module.
 */
trait ServerSidePlanningClient {
  /**
   * Plan a table scan and return the list of files to read.
   *
   * @param database The database or schema name
   * @param table The table name
   * @return ScanPlan containing files to read
   */
  def planScan(database: String, table: String): ScanPlan
}

/**
 * Factory for creating ServerSidePlanningClient instances.
 * This allows for configurable implementations (REST, mock, Spark-based, etc.)
 */
trait ServerSidePlanningClientFactory {
  def createClient(spark: SparkSession): ServerSidePlanningClient

  /**
   * Create a client for a specific catalog by reading catalog-specific configuration.
   * This method reads configuration from spark.sql.catalog.<catalogName>.uri and
   * spark.sql.catalog.<catalogName>.token.
   *
   * @param spark The SparkSession
   * @param catalogName The name of the catalog (e.g., "spark_catalog", "unity")
   * @return A ServerSidePlanningClient configured for the specified catalog
   */
  def buildForCatalog(spark: SparkSession, catalogName: String): ServerSidePlanningClient
}

/**
 * Default factory that uses reflection to load IcebergRESTCatalogPlanningClient
 * from the iceberg module (if available).
 */
class IcebergRESTCatalogPlanningClientFactory extends ServerSidePlanningClientFactory {
  override def createClient(spark: SparkSession): ServerSidePlanningClient = {
    val catalogUri = spark.conf.get("spark.delta.iceberg.rest.catalog.uri", "")
    val token = spark.conf.get("spark.delta.iceberg.rest.catalog.token", "")

    if (catalogUri.isEmpty) {
      throw new IllegalStateException(
        "Iceberg REST catalog URI not configured. " +
        "Please set spark.delta.iceberg.rest.catalog.uri")
    }

    createClientWithUriAndToken(catalogUri, token)
  }

  override def buildForCatalog(
      spark: SparkSession,
      catalogName: String): ServerSidePlanningClient = {
    val catalogUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val token = spark.conf.get(s"spark.sql.catalog.$catalogName.token", "")

    if (catalogUri.isEmpty) {
      throw new IllegalStateException(
        s"Catalog URI not configured for catalog '$catalogName'. " +
        s"Please set spark.sql.catalog.$catalogName.uri")
    }

    createClientWithUriAndToken(catalogUri, token)
  }

  private def createClientWithUriAndToken(
      catalogUri: String,
      token: String): ServerSidePlanningClient = {
    // Use reflection to avoid compile-time dependency on iceberg module
    // scalastyle:off classforname
    val clientClass = Class.forName(
      "org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClient")
    // scalastyle:on classforname
    val constructor = clientClass.getConstructor(classOf[String], classOf[String])
    constructor.newInstance(catalogUri, token).asInstanceOf[ServerSidePlanningClient]
  }
}

/**
 * Registry for client factories. Can be configured for testing.
 */
object ServerSidePlanningClientFactory {
  @volatile private var customFactory: Option[ServerSidePlanningClientFactory] = None

  /**
   * Set a custom factory for testing or alternative implementations.
   */
  def setFactory(factory: ServerSidePlanningClientFactory): Unit = {
    customFactory = Some(factory)
  }

  /**
   * Clear the custom factory and return to default behavior.
   */
  def clearFactory(): Unit = {
    customFactory = None
  }

  /**
   * Get the current factory (custom or default).
   */
  def getFactory(): ServerSidePlanningClientFactory = {
    customFactory.getOrElse(new IcebergRESTCatalogPlanningClientFactory())
  }

  /**
   * Convenience method to create a client using the current factory.
   */
  def createClient(spark: SparkSession): ServerSidePlanningClient = {
    getFactory().createClient(spark)
  }

  /**
   * Convenience method to create a client for a specific catalog using the current factory.
   */
  def buildForCatalog(spark: SparkSession, catalogName: String): ServerSidePlanningClient = {
    getFactory().buildForCatalog(spark, catalogName)
  }
}
