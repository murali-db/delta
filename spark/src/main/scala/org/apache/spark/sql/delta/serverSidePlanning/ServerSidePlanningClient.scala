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
 * The factory is auto-discovered and registered using reflection-based lazy initialization.
 * When delta-iceberg JAR is on the classpath, IcebergRESTCatalogPlanningClientFactory
 * is automatically registered on first use. If delta-iceberg is not present, the factory
 * remains unregistered and server-side planning will not be available.
 *
 * This approach uses lazy initialization (only loads when getFactory() is called) instead
 * of eager ServiceLoader initialization, making it compatible with Spark Connect.
 */
private[serverSidePlanning] object ServerSidePlanningClientFactory extends io.delta.sql.DeltaLogging {
  @volatile private var registeredFactory: Option[ServerSidePlanningClientFactory] = None
  @volatile private var autoRegistrationAttempted: Boolean = false

  /**
   * Lazily attempts to auto-register IcebergRESTCatalogPlanningClientFactory.
   * Uses double-checked locking to ensure thread-safe initialization.
   * Only attempts registration once per JVM session.
   */
  private def tryAutoRegisterFactory(): Unit = {
    if (!autoRegistrationAttempted) {
      synchronized {
        if (!autoRegistrationAttempted) {
          autoRegistrationAttempted = true
          try {
            val clazz = Class.forName(
              "org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClientFactory",
              true,  // initialize
              Thread.currentThread().getContextClassLoader)
            val factory = clazz.getConstructor().newInstance()
              .asInstanceOf[ServerSidePlanningClientFactory]
            registeredFactory = Some(factory)
            logInfo(s"Auto-registered ${factory.getClass.getName} for server-side planning")
          } catch {
            case _: ClassNotFoundException =>
              logInfo("IcebergRESTCatalogPlanningClientFactory not found on classpath. " +
                "Server-side planning will not be available.")
            case e: Exception =>
              throw new IllegalStateException(
                "Failed to auto-register IcebergRESTCatalogPlanningClientFactory", e)
          }
        }
      }
    }
  }

  /**
   * Get the registered factory, attempting auto-registration if not yet attempted.
   */
  def getFactory(): Option[ServerSidePlanningClientFactory] = {
    tryAutoRegisterFactory()
    registeredFactory
  }

  /**
   * Manually register a factory (for testing or custom implementations).
   */
  private[serverSidePlanning] def setFactory(factory: ServerSidePlanningClientFactory): Unit = {
    registeredFactory = Some(factory)
  }

  /**
   * Clear the registered factory (for testing).
   */
  private[serverSidePlanning] def clearFactory(): Unit = {
    synchronized {
      registeredFactory = None
      autoRegistrationAttempted = false
    }
  }

  /**
   * Check if a factory is currently registered.
   */
  private[serverSidePlanning] def isFactoryRegistered(): Boolean = {
    tryAutoRegisterFactory()
    registeredFactory.isDefined
  }

  /**
   * Get factory info for debugging.
   */
  private[serverSidePlanning] def getFactoryInfo(): String = {
    tryAutoRegisterFactory()
    registeredFactory.map(_.getClass.getName).getOrElse("No factory registered")
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
    getFactory().map(_.buildClient(spark, metadata)).getOrElse {
      throw new IllegalStateException(
        "No ServerSidePlanningClientFactory has been registered. " +
        "To enable FGAC support, ensure delta-iceberg JAR is on the classpath, " +
        "or call ServerSidePlanningClientFactory.setFactory() manually.")
    }
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
