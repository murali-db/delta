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

import org.apache.spark.sql.SparkSession

/**
 * Simple data class representing a file to scan.
 * No dependencies on Iceberg types.
 */
case class ScanFile(
  filePath: String,
  fileSizeInBytes: Long,
  fileFormat: String,  // "parquet", "orc", etc.
  partitionData: Map[String, String] = Map.empty
)

/**
 * Result of a table scan plan operation.
 */
case class ScanPlan(
  files: Seq[ScanFile],
  schema: String  // JSON-encoded schema
)

/**
 * Interface for planning table scans via Iceberg REST catalog.
 * This interface is intentionally simple and has no dependencies
 * on Iceberg libraries, allowing it to live in delta-spark module.
 */
trait IcebergTableClient {
  /**
   * Plan a table scan and return the list of files to read.
   *
   * @param namespace The namespace/database name
   * @param table The table name
   * @return ScanPlan containing files and schema
   */
  def planTableScan(namespace: String, table: String): ScanPlan
}

/**
 * Factory for creating IcebergTableClient instances.
 * This allows for configurable implementations (REST, mock, Spark-based, etc.)
 */
trait IcebergTableClientFactory {
  def createClient(spark: SparkSession): IcebergTableClient
}

/**
 * Default factory that uses reflection to load RESTIcebergTableClient
 * from the iceberg module (if available).
 */
class RESTIcebergTableClientFactory extends IcebergTableClientFactory {
  override def createClient(spark: SparkSession): IcebergTableClient = {
    val catalogUri = spark.conf.get("spark.delta.iceberg.rest.catalog.uri", "")
    val token = spark.conf.get("spark.delta.iceberg.rest.catalog.token", "")

    if (catalogUri.isEmpty) {
      throw new IllegalStateException(
        "Iceberg REST catalog URI not configured. " +
        "Please set spark.delta.iceberg.rest.catalog.uri")
    }

    // Use reflection to avoid compile-time dependency on iceberg module
    // scalastyle:off classforname
    val clientClass = Class.forName(
      "org.apache.spark.sql.delta.icebergScanPlan.RESTIcebergTableClient")
    // scalastyle:on classforname
    val constructor = clientClass.getConstructor(classOf[String], classOf[String])
    constructor.newInstance(catalogUri, token).asInstanceOf[IcebergTableClient]
  }
}

/**
 * Registry for client factories. Can be configured for testing.
 */
object IcebergTableClientFactory {
  @volatile private var customFactory: Option[IcebergTableClientFactory] = None

  /**
   * Set a custom factory for testing or alternative implementations.
   */
  def setFactory(factory: IcebergTableClientFactory): Unit = {
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
  def getFactory(): IcebergTableClientFactory = {
    customFactory.getOrElse(new RESTIcebergTableClientFactory())
  }

  /**
   * Convenience method to create a client using the current factory.
   */
  def createClient(spark: SparkSession): IcebergTableClient = {
    getFactory().createClient(spark)
  }
}
