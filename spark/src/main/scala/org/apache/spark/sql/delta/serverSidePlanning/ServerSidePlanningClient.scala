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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter

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
 * Interface for planning table scans via server-side planning.
 * This interface uses Spark's standard `org.apache.spark.sql.sources.Filter` as the universal
 * representation for filter pushdown. This keeps the interface catalog-agnostic while allowing
 * each server-side planning catalog implementation to convert filters to their own native format.
 */
trait ServerSidePlanningClient {
  /**
   * Plan a table scan and return the list of files to read.
   *
   * @param databaseName The database or schema name
   * @param table The table name
   * @param filterOption Optional filter expression to push down to server (Spark Filter format)
   * @param projectionOption Optional projection (column names) to push down to server
   * @param limitOption Optional limit to push down to server
   * @return ScanPlan containing files to read
   */
  def planScan(
      databaseName: String,
      table: String,
      filterOption: Option[Filter] = None,
      projectionOption: Option[Seq[String]] = None,
      limitOption: Option[Int] = None): ScanPlan

  /**
   * Check if all given filters can be converted to the server's native filter format.
   * This is used during filter pushdown to determine whether to return residuals to Spark.
   *
   * @param filters Array of Spark filters to check
   * @return true if ALL filters can be converted, false if ANY filter cannot be converted
   */
  def canConvertFilters(filters: Array[Filter]): Boolean
}

/**
 * Factory for creating ServerSidePlanningClient instances.
 * This allows for configurable implementations (REST, mock, Spark-based, etc.)
 */
private[serverSidePlanning] trait ServerSidePlanningClientFactory {
  /**
   * Create a client using metadata necessary for server-side planning.
   *
   * @param spark The SparkSession
   * @param metadata Metadata necessary for server-side planning
   * @return A ServerSidePlanningClient configured with the metadata
   */
  def buildClient(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient
}

/**
 * Registry for client factories. Can be configured for testing or to provide
 * production implementations (e.g., IcebergRESTCatalogPlanningClientFactory).
 *
 * The factory is auto-discovered and registered using Java's ServiceLoader mechanism.
 * When delta-iceberg JAR is on the classpath, IcebergRESTCatalogPlanningClientFactory
 * is automatically registered. If delta-iceberg is not present, the factory remains
 * unregistered and server-side planning will not be available.
 *
 * To register a custom factory implementation:
 * 1. Create META-INF/services/org.apache.spark.sql.delta.serverSidePlanning
 *    .ServerSidePlanningClientFactory
 * 2. Add your implementation's fully-qualified class name to the file
 * 3. Ensure your JAR is on the classpath
 */
private[serverSidePlanning] object ServerSidePlanningClientFactory {
  @volatile private var registeredFactory: Option[ServerSidePlanningClientFactory] = None

  // ========== SERVICE LOADER AUTO-REGISTRATION ==========
  // This block runs ONCE when ServerSidePlanningClientFactory is first accessed.
  // It uses Java's ServiceLoader to discover and register factories declared in
  // META-INF/services/org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlanningClientFactory
  {
    import java.util.ServiceLoader
    import scala.jdk.CollectionConverters._

    try {
      // Use ServiceLoader to discover all implementations on the classpath
      val loader = ServiceLoader.load(
        classOf[ServerSidePlanningClientFactory],
        Thread.currentThread().getContextClassLoader)

      // Convert Java Iterator to Scala and collect all factories
      val factories = loader.iterator().asScala.toList

      if (factories.nonEmpty) {
        // Use the first discovered factory
        // In practice, there should only be one (IcebergRESTCatalogPlanningClientFactory)
        registeredFactory = Some(factories.head)

        // Optional: Log successful registration for debugging
        // Uncomment for visibility during development
        // System.err.println(s"[Delta] Auto-registered ${factories.head.getClass.getName} " +
        //   s"via ServiceLoader (found ${factories.size} implementation(s))")

        // Optional: Warn if multiple implementations found
        if (factories.size > 1) {
          // scalastyle:off println
          System.err.println(
            s"[Delta] Warning: Multiple ServerSidePlanningClientFactory implementations found. " +
            s"Using ${factories.head.getClass.getName}. " +
            s"Others: ${factories.tail.map(_.getClass.getName).mkString(", ")}")
          // scalastyle:on println
        }
      } else {
        // No factories discovered - delta-iceberg not on classpath
        // This is fine, server-side planning just won't be available
        // System.err.println("[Delta] No ServerSidePlanningClientFactory found, FGAC disabled")
      }

    } catch {
      case e: Exception =>
        // Unexpected error during service loading - log but don't fail
        // Delta should still work for non-FGAC tables
        // scalastyle:off println
        System.err.println(
          s"[Delta] Warning: Failed to auto-discover server-side planning factory: ${e.getMessage}")
        // scalastyle:on println
    }
  }
  // ========== END SERVICE LOADER AUTO-REGISTRATION ==========

  /**
   * Set a factory for production use or testing.
   * This overrides any auto-registered factory.
   *
   * Useful for:
   * - Testing with mock implementations
   * - Programmatically configuring a specific factory
   * - Overriding the ServiceLoader-discovered factory
   */
  private[serverSidePlanning] def setFactory(factory: ServerSidePlanningClientFactory): Unit = {
    registeredFactory = Some(factory)
  }

  /**
   * Clear the registered factory.
   * Useful for testing to reset state between tests.
   */
  private[serverSidePlanning] def clearFactory(): Unit = {
    registeredFactory = None
  }

  /**
   * Get the currently registered factory.
   * Throws IllegalStateException if no factory has been registered.
   *
   * @throws IllegalStateException if no factory is available
   */
  def getFactory(): ServerSidePlanningClientFactory = {
    registeredFactory.getOrElse {
      throw new IllegalStateException(
        "No ServerSidePlanningClientFactory has been registered. " +
        "To enable FGAC support, ensure delta-iceberg JAR is on the classpath, " +
        "or call ServerSidePlanningClientFactory.setFactory() manually.")
    }
  }

  /**
   * Check if a factory is registered.
   * Useful for feature detection and testing.
   *
   * @return true if a factory is available, false otherwise
   */
  def isFactoryRegistered(): Boolean = {
    registeredFactory.isDefined
  }

  /**
   * Get information about the registered factory.
   * Returns None if no factory is registered.
   *
   * @return Optional factory class name
   */
  def getFactoryInfo(): Option[String] = {
    registeredFactory.map(_.getClass.getName)
  }

  /**
   * Convenience method to create a client from metadata using the registered factory.
   *
   * @param spark The SparkSession
   * @param metadata Metadata for configuring the client
   * @return A configured ServerSidePlanningClient
   * @throws IllegalStateException if no factory is registered
   */
  def buildClient(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    getFactory().buildClient(spark, metadata)
  }
}

/**
 * Temporary storage credentials from server-side planning response.
 */
sealed trait ScanPlanStorageCredentials

object ScanPlanStorageCredentials {

  /** IRC config key mappings for each credential type. */
  private val S3_KEYS = Seq("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
  private val AZURE_KEYS = Seq("azure.account-name", "azure.sas-token", "azure.container-name")
  private val GCS_KEYS = Seq("gcs.oauth2.token")

  /**
   * Factory method to create credentials from IRC config map.
   * Tries each credential type and returns the first complete match.
   * Throws IllegalStateException if credentials are incomplete or unrecognized.
   */
  def fromConfig(config: Map[String, String]): ScanPlanStorageCredentials = {
    def get(key: String): String =
      config.getOrElse(key, throw new IllegalStateException(s"Missing required credential: $key"))

    def hasAny(keys: Seq[String]): Boolean = keys.exists(config.contains)

    // Try each sealed trait subtype in priority order
    if (hasAny(S3_KEYS)) {
      S3Credentials(
        get("s3.access-key-id"),
        get("s3.secret-access-key"),
        get("s3.session-token"))
    } else if (hasAny(AZURE_KEYS)) {
      AzureCredentials(
        get("azure.account-name"),
        get("azure.sas-token"),
        get("azure.container-name"))
    } else if (hasAny(GCS_KEYS)) {
      GcsCredentials(get("gcs.oauth2.token"))
    } else {
      throw new IllegalStateException(
        s"Unrecognized credential keys: ${config.keys.mkString(", ")}. " +
          "Expected S3, Azure, or GCS properties.")
    }
  }
}

/**
 * AWS S3 temporary credentials.
 */
case class S3Credentials(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String) extends ScanPlanStorageCredentials

/**
 * Azure ADLS Gen2 credentials with SAS token.
 */
case class AzureCredentials(
    accountName: String,
    sasToken: String,
    containerName: String) extends ScanPlanStorageCredentials

/**
 * Google Cloud Storage OAuth2 token credentials.
 */
case class GcsCredentials(
    oauth2Token: String) extends ScanPlanStorageCredentials

/**
 * Result of a table scan plan operation.
 */
case class ScanPlan(
    files: Seq[ScanFile],
    credentials: Option[ScanPlanStorageCredentials] = None)
