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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

/**
 * Implementation of ServerSidePlanningClient that uses Spark SQL with input_file_name()
 * to discover the list of files in a table. This allows end-to-end testing without
 * a real REST server.
 *
 * This implementation works with any Spark-readable table format (Delta, Parquet, Iceberg, etc.)
 */
class ServerSidePlanningTestClient(spark: SparkSession) extends ServerSidePlanningClient {

  override def planScan(namespace: String, table: String): ScanPlan = {
    val fullTableName = s"$namespace.$table"

    // Clone the Spark session to avoid modifying the shared session's config
    val clonedSession = spark.cloneSession()

    // Disable force server-side planning in the cloned session to avoid infinite loop
    clonedSession.conf.set("spark.databricks.delta.catalog.forceServerSidePlanning", "false")

    // Get the table schema
    val tableSchema = clonedSession.table(fullTableName).schema
    val schemaJson = tableSchema.json

    // Use input_file_name() to get the list of files
    // Query: SELECT DISTINCT input_file_name() FROM table
    val filesDF = clonedSession.table(fullTableName)
      .select(input_file_name().as("file_path"))
      .distinct()

    // Collect file paths
    val filePaths = filesDF.collect().map(_.getString(0))

    // Get file metadata (size, format) from filesystem
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val files = filePaths.map { filePath =>
      val path = new Path(filePath)
      val fs = path.getFileSystem(hadoopConf)
      val fileStatus = fs.getFileStatus(path)

      ScanFile(
        filePath = filePath,
        fileSizeInBytes = fileStatus.getLen,
        fileFormat = getFileFormat(path),
        partitionData = Map.empty // Could extract from path if needed
      )
    }.toSeq

    ScanPlan(files = files, schema = schemaJson)
  }

  private def getFileFormat(path: Path): String = {
    // scalastyle:off caselocale
    val name = path.getName.toLowerCase
    // scalastyle:on caselocale
    if (name.endsWith(".parquet")) "parquet"
    else if (name.endsWith(".orc")) "orc"
    else if (name.endsWith(".avro")) "avro"
    else "unknown"
  }
}

/**
 * Factory for creating ServerSidePlanningTestClient instances.
 */
class ServerSidePlanningTestClientFactory extends ServerSidePlanningClientFactory {
  override def createClient(spark: SparkSession): ServerSidePlanningClient = {
    new ServerSidePlanningTestClient(spark)
  }
}
