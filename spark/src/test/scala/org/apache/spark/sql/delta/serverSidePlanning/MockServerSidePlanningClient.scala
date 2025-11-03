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
 * Mock implementation of ServerSidePlanningClient for testing.
 * Returns pre-configured file lists without requiring a real server.
 */
class MockServerSidePlanningClient(
    mockFiles: Map[(String, String), Seq[ScanFile]] = Map.empty,
    mockSchema: String =
      """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true}]}"""
) extends ServerSidePlanningClient {

  override def planScan(namespace: String, table: String): ScanPlan = {
    val files = mockFiles.getOrElse(
      (namespace, table),
      throw new RuntimeException(s"No mock data configured for $namespace.$table")
    )
    ScanPlan(files = files, schema = mockSchema)
  }
}

object MockServerSidePlanningClient {
  /**
   * Creates a mock client with a simple single-file scan plan.
   */
  def withSingleFile(
      namespace: String,
      table: String,
      filePath: String,
      fileSize: Long = 1000): MockServerSidePlanningClient = {
    val file = ScanFile(
      filePath = filePath,
      fileSizeInBytes = fileSize,
      fileFormat = "parquet",
      partitionData = Map.empty
    )
    new MockServerSidePlanningClient(Map((namespace, table) -> Seq(file)))
  }

  /**
   * Creates a mock client with multiple files.
   */
  def withFiles(
      namespace: String,
      table: String,
      files: Seq[ScanFile]): MockServerSidePlanningClient = {
    new MockServerSidePlanningClient(Map((namespace, table) -> files))
  }
}

/**
 * Factory for creating MockServerSidePlanningClient instances.
 */
class MockServerSidePlanningClientFactory(client: MockServerSidePlanningClient)
    extends ServerSidePlanningClientFactory {
  override def createClient(spark: SparkSession): ServerSidePlanningClient = client
}
