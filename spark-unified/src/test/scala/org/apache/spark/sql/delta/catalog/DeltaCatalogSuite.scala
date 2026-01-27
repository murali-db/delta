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

package org.apache.spark.sql.delta.catalog

import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.DeltaOptions

import java.io.File
import java.util.Locale

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.internal.SQLConf

/**
 * Unit tests for DeltaCatalog's V2 connector routing logic.
 *
 * Verifies that DeltaCatalog correctly routes table loading based on
 * DeltaSQLConf.V2_ENABLE_MODE:
 * - STRICT mode: Kernel's SparkTable (V2 connector)
 * - NONE mode (default): DeltaTableV2 (V1 connector)
 */
class DeltaCatalogSuite extends DeltaSQLCommandTest {

  private val modeTestCases = Seq(
    ("STRICT", classOf[SparkTable], "Kernel SparkTable"),
    ("NONE", classOf[DeltaTableV2], "DeltaTableV2")
  )

  modeTestCases.foreach { case (mode, expectedClass, description) =>
    test(s"catalog-based table with mode=$mode returns $description") {
      withTempDir { tempDir =>
        val tableName = s"test_catalog_${mode.toLowerCase(Locale.ROOT)}"
        val location = new File(tempDir, tableName).getAbsolutePath

        withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> mode) {
          sql(s"CREATE TABLE $tableName (id INT, name STRING) USING delta LOCATION '$location'")

          val catalog = spark.sessionState.catalogManager.v2SessionCatalog
            .asInstanceOf[DeltaCatalog]
          val ident = org.apache.spark.sql.connector.catalog.Identifier
            .of(Array("default"), tableName)
          val table = catalog.loadTable(ident)

          assert(table.getClass == expectedClass,
            s"Mode $mode should return ${expectedClass.getSimpleName}")
        }
      }
    }
  }

  modeTestCases.foreach { case (mode, expectedClass, description) =>
    test(s"path-based table with mode=$mode returns $description") {
      withTempDir { tempDir =>
        val path = tempDir.getAbsolutePath

        withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> mode) {
          sql(s"CREATE TABLE delta.`$path` (id INT, name STRING) USING delta")

          val catalog = spark.sessionState.catalogManager.v2SessionCatalog
            .asInstanceOf[DeltaCatalog]
          val ident = org.apache.spark.sql.connector.catalog.Identifier
            .of(Array("delta"), path)
          val table = catalog.loadTable(ident)

          assert(table.getClass == expectedClass,
            s"Mode $mode should return ${expectedClass.getSimpleName} for path-based table")
        }
      }
    }
  }

  /**
   * Tests for the fix to prevent data loss in dynamic partition overwrite with saveAsTable.
   * 
   * These tests verify that when using SaveMode.Overwrite with partitionOverwriteMode=dynamic
   * on a managed table via saveAsTable, the catalog correctly routes to the safe
   * StagedDeltaTableV2 path instead of the dangerous DROP+CREATE sequence.
   */
  test("dynamic partition overwrite with saveAsTable - option-based") {
    withTempDir { tempDir =>
      val tableName = "test_dynamic_partition_overwrite"
      val location = new File(tempDir, tableName).getAbsolutePath

      // Create initial partitioned table with data
      sql(s"CREATE TABLE $tableName (id INT, value STRING, part INT) " +
        s"USING delta PARTITIONED BY (part) LOCATION '$location'")
      
      sql(s"INSERT INTO $tableName VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 1)")
      
      // Verify initial data
      checkAnswer(
        sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(1, "a", 1),
          org.apache.spark.sql.Row(2, "b", 2),
          org.apache.spark.sql.Row(3, "c", 1)
        )
      )

      // Perform dynamic partition overwrite using saveAsTable with option
      spark.range(10, 12)
        .selectExpr("CAST(id AS INT) as id", "'x' as value", "1 as part")
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
        .saveAsTable(tableName)
      
      // Verify that only partition 1 was overwritten, partition 2 remains
      checkAnswer(
        sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(2, "b", 2),   // partition 2 preserved
          org.apache.spark.sql.Row(10, "x", 1),  // partition 1 overwritten
          org.apache.spark.sql.Row(11, "x", 1)   // partition 1 overwritten
        )
      )

      // Verify table still exists and is functional
      sql(s"INSERT INTO $tableName VALUES (4, 'd', 3)")
      assert(sql(s"SELECT COUNT(*) FROM $tableName").head().getLong(0) == 4)
    }
  }

  test("dynamic partition overwrite with saveAsTable - session conf") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
      withTempDir { tempDir =>
        val tableName = "test_dynamic_partition_overwrite_conf"
        val location = new File(tempDir, tableName).getAbsolutePath

        // Create initial partitioned table with data
        sql(s"CREATE TABLE $tableName (id INT, value STRING, part STRING) " +
          s"USING delta PARTITIONED BY (part) LOCATION '$location'")
        
        sql(s"INSERT INTO $tableName VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")

        // Perform dynamic partition overwrite using saveAsTable (mode from session conf)
        spark.range(10, 12)
          .selectExpr("CAST(id AS INT) as id", "'z' as value", "'x' as part")
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
        
        // Verify that only partition 'x' was overwritten, partition 'y' remains
        checkAnswer(
          sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
          Seq(
            org.apache.spark.sql.Row(2, "b", "y"),   // partition 'y' preserved
            org.apache.spark.sql.Row(10, "z", "x"),  // partition 'x' overwritten
            org.apache.spark.sql.Row(11, "z", "x")   // partition 'x' overwritten
          )
        )
      }
    }
  }

  test("dynamic partition overwrite with saveAsTable - multiple partitions") {
    withTempDir { tempDir =>
      val tableName = "test_dynamic_partition_overwrite_multi"
      val location = new File(tempDir, tableName).getAbsolutePath

      // Create table with multiple partition columns
      sql(s"CREATE TABLE $tableName (id INT, value STRING, part1 INT, part2 STRING) " +
        s"USING delta PARTITIONED BY (part1, part2) LOCATION '$location'")
      
      sql(s"INSERT INTO $tableName VALUES " +
        s"(1, 'a', 1, 'x'), (2, 'b', 1, 'y'), (3, 'c', 2, 'x'), (4, 'd', 2, 'y')")

      // Overwrite only part1=1, part2='x'
      spark.range(10, 11)
        .selectExpr("CAST(id AS INT) as id", "'z' as value", "1 as part1", "'x' as part2")
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
        .saveAsTable(tableName)
      
      // Verify only the matching partition was overwritten
      checkAnswer(
        sql(s"SELECT id, value, part1, part2 FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(2, "b", 1, "y"),   // preserved
          org.apache.spark.sql.Row(3, "c", 2, "x"),   // preserved
          org.apache.spark.sql.Row(4, "d", 2, "y"),   // preserved
          org.apache.spark.sql.Row(10, "z", 1, "x")   // overwritten
        )
      )
    }
  }

  test("static partition overwrite with saveAsTable still works") {
    withTempDir { tempDir =>
      val tableName = "test_static_partition_overwrite"
      val location = new File(tempDir, tableName).getAbsolutePath

      // Create initial partitioned table
      sql(s"CREATE TABLE $tableName (id INT, value STRING, part INT) " +
        s"USING delta PARTITIONED BY (part) LOCATION '$location'")
      
      sql(s"INSERT INTO $tableName VALUES (1, 'a', 1), (2, 'b', 2)")

      // Perform static partition overwrite (default behavior)
      spark.range(10, 11)
        .selectExpr("CAST(id AS INT) as id", "'x' as value", "1 as part")
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
      
      // Verify all partitions were overwritten (static mode)
      checkAnswer(
        sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(10, "x", 1)  // all data replaced
        )
      )
    }
  }

  test("dynamic partition overwrite on non-partitioned table is ignored") {
    withTempDir { tempDir =>
      val tableName = "test_dynamic_no_partition"
      val location = new File(tempDir, tableName).getAbsolutePath

      // Create non-partitioned table
      sql(s"CREATE TABLE $tableName (id INT, value STRING) " +
        s"USING delta LOCATION '$location'")
      
      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b')")

      // Try dynamic partition overwrite on non-partitioned table
      // Should behave like static overwrite (replace all data)
      spark.range(10, 11)
        .selectExpr("CAST(id AS INT) as id", "'x' as value")
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
        .saveAsTable(tableName)
      
      // Verify all data was replaced (dynamic mode ignored for non-partitioned tables)
      checkAnswer(
        sql(s"SELECT id, value FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(10, "x")
        )
      )
    }
  }

  test("saveAsTable with mode overwrite on existing Delta table (FGAC scenario)") {
    withTempDir { tempDir =>
      val tableName = "test_fgac_overwrite"
      val location = new File(tempDir, tableName).getAbsolutePath

      // Create initial Delta table (simulating an FGAC/UC table)
      sql(s"CREATE TABLE $tableName (id INT, value STRING, part INT) " +
        s"USING delta PARTITIONED BY (part) LOCATION '$location'")
      
      sql(s"INSERT INTO $tableName VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 1)")
      
      // Verify initial data
      checkAnswer(
        sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(1, "a", 1),
          org.apache.spark.sql.Row(2, "b", 2),
          org.apache.spark.sql.Row(3, "c", 1)
        )
      )

      // Perform overwrite using saveAsTable (the FGAC bug scenario)
      // This should NOT drop the table even if provider properties are missing/incorrect
      spark.range(10, 12)
        .selectExpr("CAST(id AS INT) as id", "'x' as value", "1 as part")
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
      
      // Verify table was replaced (all data overwritten in static mode)
      checkAnswer(
        sql(s"SELECT id, value, part FROM $tableName ORDER BY id"),
        Seq(
          org.apache.spark.sql.Row(10, "x", 1),
          org.apache.spark.sql.Row(11, "x", 1)
        )
      )

      // Most importantly: verify table still exists and is functional
      sql(s"INSERT INTO $tableName VALUES (4, 'd', 2)")
      assert(sql(s"SELECT COUNT(*) FROM $tableName").head().getLong(0) == 3)
      
      // Verify it's still a Delta table
      val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, location)
      assert(deltaLog.update().version >= 0, "Table should still be a valid Delta table")
    }
  }
}
