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

package com.sparkuctest

import java.io.File

import org.apache.spark.SparkConf

/**
 * Integration test suite for Delta Table operations through Unity Catalog.
 *
 * This suite uses the default SparkSQLExecutor to execute queries via Spark SQL
 * and verify that Delta table operations work correctly through Unity Catalog.
 *
 * All table operations are performed through the Unity Catalog server,
 * and the SQLExecutor framework allows for easy verification of results.
 */
class UCDeltaTableIntegrationSuite extends UCDeltaTableIntegrationSuiteBase {

  /**
   * Configure Spark with Unity Catalog settings.
   */
  override protected def sparkConf: SparkConf = {
    configureSparkWithUnityCatalog(super.sparkConf)
  }

  /**
   * Provide the SQL executor implementation using SparkSession.
   * Lazy evaluation ensures spark is initialized before creating the executor.
   */
  override protected def sqlExecutor: UCDeltaTableIntegrationSuiteBase.SQLExecutor = {
    new UCDeltaTableIntegrationSuiteBase.SparkSQLExecutor(spark)
  }

  test("CREATE TABLE and INSERT - verify table state") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_create_insert").getAbsolutePath
      val tableName = s"$unityCatalogName.default.test_create_insert"

      // Create table via executor
      sqlExecutor.runSQL(s"""
        CREATE TABLE $tableName (
          id INT,
          name STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      try {
        // Insert data via executor
        sqlExecutor.runSQL(s"""
          INSERT INTO $tableName VALUES
          (1, 'Alice'),
          (2, 'Bob'),
          (3, 'Charlie')
        """)

        // Verify table state after INSERT
        sqlExecutor.checkTable(
          tableName,
          Seq(
            Seq("1", "Alice"),
            Seq("2", "Bob"),
            Seq("3", "Charlie")
          )
        )
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("UPDATE - verify table state after modification") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_update").getAbsolutePath
      val tableName = s"$unityCatalogName.default.test_update"

      // Create and populate table
      sqlExecutor.runSQL(s"""
        CREATE TABLE $tableName (
          id INT,
          status STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      sqlExecutor.runSQL(s"""
        INSERT INTO $tableName VALUES
        (1, 'pending'),
        (2, 'pending'),
        (3, 'completed')
      """)

      try {
        // Perform UPDATE operation
        sqlExecutor.runSQL(s"""
          UPDATE $tableName
          SET status = 'completed'
          WHERE id <= 2
        """)

        // Verify table state after UPDATE
        sqlExecutor.checkTable(
          tableName,
          Seq(
            Seq("1", "completed"),
            Seq("2", "completed"),
            Seq("3", "completed")
          )
        )
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("DELETE - verify table state after modification") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_delete").getAbsolutePath
      val tableName = s"$unityCatalogName.default.test_delete"

      // Create and populate table
      sqlExecutor.runSQL(s"""
        CREATE TABLE $tableName (
          id INT,
          active BOOLEAN
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      sqlExecutor.runSQL(s"""
        INSERT INTO $tableName VALUES
        (1, true),
        (2, false),
        (3, true),
        (4, false)
      """)

      try {
        // Perform DELETE operation
        sqlExecutor.runSQL(s"""
          DELETE FROM $tableName
          WHERE active = false
        """)

        // Verify table state after DELETE
        sqlExecutor.checkTable(
          tableName,
          Seq(
            Seq("1", "true"),
            Seq("3", "true")
          )
        )
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("MERGE - verify table state after modification") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_merge").getAbsolutePath
      val tableName = s"$unityCatalogName.default.test_merge"

      // Create and populate target table
      sqlExecutor.runSQL(s"""
        CREATE TABLE $tableName (
          id INT,
          value STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      sqlExecutor.runSQL(s"""
        INSERT INTO $tableName VALUES
        (1, 'old1'),
        (2, 'old2')
      """)

      try {
        // Create a source table for MERGE
        val sourcePath = new File(dir, "test_merge_source").getAbsolutePath
        val sourceTable = s"$unityCatalogName.default.test_merge_source"

        sqlExecutor.runSQL(s"""
          CREATE TABLE $sourceTable (
            id INT,
            value STRING
          ) USING DELTA
          LOCATION '$sourcePath'
        """)

        sqlExecutor.runSQL(s"""
          INSERT INTO $sourceTable VALUES (2, 'updated2'), (3, 'new3')
        """)

        // Perform MERGE operation
        sqlExecutor.runSQL(s"""
          MERGE INTO $tableName AS target
          USING $sourceTable AS source
          ON target.id = source.id
          WHEN MATCHED THEN UPDATE SET value = source.value
          WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
        """)

        // Clean up source table
        spark.sql(s"DROP TABLE IF EXISTS $sourceTable")

        // Verify table state after MERGE
        sqlExecutor.checkTable(
          tableName,
          Seq(
            Seq("1", "old1"),
            Seq("2", "updated2"),
            Seq("3", "new3")
          )
        )
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }
}

