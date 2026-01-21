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

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.mutable

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Post-commit hook that sends commit metrics to the PO (Predictive Optimization) endpoint
 * for Unity Catalog managed Delta tables.
 *
 * This hook is triggered after each successful Delta commit and sends file-level and row-level
 * metrics to the UC endpoint for PO analysis. The hook follows a best-effort delivery model:
 * failures are logged but do not block the commit.
 *
 * The hook is only activated for tables that:
 * - Have the PO metrics feature enabled (spark.databricks.delta.po.metrics.enabled)
 * - Are Unity Catalog managed tables
 *
 * @param catalogTable The catalog table metadata (if available)
 */
case class UpdatePOMetricsHook(catalogTable: Option[CatalogTable])
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update PO Metrics"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    // Check if hook is enabled
    if (!spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_ENABLED)) {
      logDebug("PO metrics hook is disabled, skipping")
      return
    }

    // Check if table is UC-managed
    if (!isUCManagedTable(txn.deltaLog, catalogTable)) {
      logDebug("Table is not a UC-managed table, skipping PO metrics")
      return
    }

    // Check if endpoint is configured
    if (!spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key).exists(_.nonEmpty)) {
      logInfo("PO metrics endpoint not configured, skipping")
      return
    }

    try {
      // Extract metrics from the committed transaction
      val metrics = extractMetrics(txn.committedActions)

      // Get operation from CommitInfo
      val operation = getOperation(txn.committedActions)

      // Get partition info if table is partitioned
      val partitionInfo = extractPartitionInfo(txn)

      // Get table metadata
      val tableId = txn.deltaLog.tableId.getOrElse {
        throw new IllegalStateException("UC-managed table must have a table ID")
      }

      val tableName = getTableName(catalogTable)

      // Get timestamp from CommitInfo or use current time
      val timestamp = getTimestamp(txn.committedActions)

      // Build payload
      val payload = POMetricsPayload(
        tableId = tableId,
        tableName = tableName,
        version = txn.committedVersion,
        timestamp = timestamp,
        operation = operation,
        metrics = metrics,
        partitionInfo = partitionInfo
      )

      // Send metrics to PO endpoint
      POMetricsClient.sendMetrics(spark, payload)

      logInfo(
        log"Successfully sent PO metrics for table ${MDC(DeltaLogKeys.TABLE_NAME, tableName)} " +
        log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}")

    } catch {
      case e: Exception =>
        logWarning(
          log"Failed to send PO metrics for table " +
          log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
          log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}: " +
          log"${MDC(DeltaLogKeys.ERROR, e.getMessage)}", e)
    }
  }

  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    // Override default error handling to always log as warning and never throw
    // This ensures PO metrics failures don't block commits
    logWarning(
      log"PO metrics hook failed for version ${MDC(DeltaLogKeys.VERSION, version)}: " +
      log"${MDC(DeltaLogKeys.ERROR, error.getMessage)}", error)
  }

  /**
   * Checks if the table is a Unity Catalog managed table.
   *
   * A table is considered UC-managed if:
   * - It has a valid table ID (required for UC tables)
   * - It has catalog information in the CatalogTable metadata
   *
   * @param deltaLog The DeltaLog for the table
   * @param catalogTable The optional catalog table metadata
   * @return true if the table is UC-managed, false otherwise
   */
  private def isUCManagedTable(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable]): Boolean = {
    // Check if table has a valid table ID
    if (deltaLog.tableId.isEmpty) {
      return false
    }

    // Check if catalog information is present
    catalogTable match {
      case Some(ct) =>
        // Check if catalog is defined (UC tables have catalog.schema.table structure)
        ct.identifier.catalog.isDefined ||
        // Or check if table properties indicate it's a Delta table with UC support
        (ct.properties.get("provider").exists(_.toLowerCase(java.util.Locale.ROOT) == "delta") &&
          deltaLog.tableId.isDefined)
      case None =>
        false
    }
  }

  /**
   * Extracts file and row metrics from the committed actions.
   *
   * @param actions The actions committed in the transaction
   * @return POMetrics containing file and row-level statistics
   */
  private def extractMetrics(actions: Seq[Action]): POMetrics = {
    var numFilesAdded = 0L
    var numBytesAdded = 0L
    var numRowsAdded = 0L
    var numFilesRemoved = 0L
    var numBytesRemoved = 0L
    var numRowsRemoved = 0L

    actions.foreach {
      case add: AddFile =>
        numFilesAdded += 1
        numBytesAdded += add.size
        // Extract row count from stats if available
        add.numLogicalRecords.foreach { rowCount =>
          numRowsAdded += rowCount
        }

      case remove: RemoveFile =>
        numFilesRemoved += 1
        // Size is optional in RemoveFile
        remove.size.foreach { fileSize =>
          numBytesRemoved += fileSize
        }
        // Extract row count from stats if available
        remove.numLogicalRecords.foreach { rowCount =>
          numRowsRemoved += rowCount
        }

      case _ => // Ignore other action types
    }

    POMetrics(
      numFilesAdded = numFilesAdded,
      numBytesAdded = numBytesAdded,
      numFilesRemoved = numFilesRemoved,
      numBytesRemoved = numBytesRemoved,
      numRowsAdded = numRowsAdded,
      numRowsRemoved = numRowsRemoved
    )
  }

  /**
   * Extracts the operation name from CommitInfo action.
   *
   * @param actions The actions committed in the transaction
   * @return The operation name (e.g., "WRITE", "DELETE", "MERGE"), or "UNKNOWN" if not found
   */
  private def getOperation(actions: Seq[Action]): String = {
    actions.collectFirst {
      case ci: CommitInfo => ci.operation
    }.getOrElse("UNKNOWN")
  }

  /**
   * Extracts partition information from the committed transaction.
   *
   * Returns a map of partition column names to the set of unique values touched by this commit.
   * Only returns partition info if the table is partitioned and partitions were added.
   *
   * @param txn The committed transaction
   * @return Optional map of partition column name to set of values
   */
  private def extractPartitionInfo(
      txn: CommittedTransaction): Option[Map[String, Set[String]]] = {
    txn.partitionsAddedToOpt.flatMap { partitionsAdded =>
      if (partitionsAdded.isEmpty) {
        None
      } else {
        // Convert HashSet[Map[String, String]] to Map[String, Set[String]]
        // Group by partition column name and collect unique values
        val partitionMap = mutable.Map[String, mutable.Set[String]]()

        partitionsAdded.foreach { partitionValues =>
          partitionValues.foreach { case (columnName, value) =>
            partitionMap.getOrElseUpdate(columnName, mutable.Set[String]()) += value
          }
        }

        if (partitionMap.isEmpty) {
          None
        } else {
          Some(partitionMap.map { case (k, v) => (k, v.toSet) }.toMap)
        }
      }
    }
  }

  /**
   * Gets the fully qualified table name.
   *
   * @param catalogTable The optional catalog table metadata
   * @return The table name in format "catalog.schema.table" or "unknown"
   */
  private def getTableName(catalogTable: Option[CatalogTable]): String = {
    catalogTable.map { ct =>
      val catalog = ct.identifier.catalog.getOrElse("")
      val database = ct.identifier.database.getOrElse("")
      val table = ct.identifier.table

      if (catalog.nonEmpty) {
        s"$catalog.$database.$table"
      } else if (database.nonEmpty) {
        s"$database.$table"
      } else {
        table
      }
    }.getOrElse("unknown")
  }

  /**
   * Extracts the commit timestamp from CommitInfo action.
   *
   * @param actions The actions committed in the transaction
   * @return The commit timestamp in milliseconds, or current time if not found
   */
  private def getTimestamp(actions: Seq[Action]): Long = {
    actions.collectFirst {
      case ci: CommitInfo => ci.timestamp.getTime
    }.getOrElse(System.currentTimeMillis())
  }
}
