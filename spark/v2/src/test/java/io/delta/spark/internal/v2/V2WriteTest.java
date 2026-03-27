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
package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for V2 batch write (blind append) operations. */
public class V2WriteTest extends V2TestBase {

  @Test
  public void testBasicAppendUnpartitioned() {
    String table = str("dsv2.%s.write_basic", nameSpace);
    spark.sql(str("CREATE TABLE %s (id INT, name STRING, value DOUBLE)", table));

    spark.sql(str("INSERT INTO %s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", table));

    check(
        str("SELECT * FROM %s ORDER BY id", table),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
  }

  @Test
  public void testMultipleAppends() {
    String table = str("dsv2.%s.write_multi", nameSpace);
    spark.sql(str("CREATE TABLE %s (id INT, name STRING)", table));

    spark.sql(str("INSERT INTO %s VALUES (1, 'Alice')", table));
    spark.sql(str("INSERT INTO %s VALUES (2, 'Bob')", table));

    check(str("SELECT * FROM %s ORDER BY id", table), List.of(row(1, "Alice"), row(2, "Bob")));
  }

  @Test
  public void testAppendPartitionedTable() {
    String table = str("dsv2.%s.write_part", nameSpace);
    spark.sql(
        str(
            "CREATE TABLE %s (id INT, name STRING, country STRING) PARTITIONED BY (country)",
            table));

    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK'), (3, 'Charlie', 'US')",
            table));

    check(
        str("SELECT id, name, country FROM %s ORDER BY id", table),
        List.of(row(1, "Alice", "US"), row(2, "Bob", "UK"), row(3, "Charlie", "US")));
  }

  @Test
  public void testAppendEmptyData() {
    String table = str("dsv2.%s.write_empty", nameSpace);
    spark.sql(str("CREATE TABLE %s (id INT, name STRING)", table));

    // Insert with a subquery that returns no rows
    spark.sql(str("INSERT INTO %s SELECT * FROM %s WHERE id < 0", table, table));

    check(str("SELECT * FROM %s", table), List.of());
  }

  @Test
  public void testAppendVariousDataTypes() {
    String table = str("dsv2.%s.write_types", nameSpace);
    spark.sql(
        str(
            "CREATE TABLE %s (int_col INT, long_col LONG, str_col STRING, "
                + "double_col DOUBLE, bool_col BOOLEAN)",
            table));

    spark.sql(str("INSERT INTO %s VALUES (42, 9999999999, 'hello', 3.14, true)", table));

    check(str("SELECT * FROM %s", table), List.of(row(42, 9999999999L, "hello", 3.14, true)));
  }

  @Test
  public void testWriteV2ReadV2() {
    String table = str("dsv2.%s.write_read_v2", nameSpace);
    spark.sql(str("CREATE TABLE %s (id INT, value STRING)", table));

    spark.sql(str("INSERT INTO %s VALUES (1, 'one'), (2, 'two')", table));

    // Read back through the same V2 catalog
    check(str("SELECT * FROM %s ORDER BY id", table), List.of(row(1, "one"), row(2, "two")));
  }

  @Test
  public void testMultiplePartitionAppends() {
    String table = str("dsv2.%s.write_part_multi", nameSpace);
    spark.sql(
        str("CREATE TABLE %s (id INT, value DOUBLE, part STRING) PARTITIONED BY (part)", table));

    spark.sql(str("INSERT INTO %s VALUES (1, 10.0, 'a')", table));
    spark.sql(str("INSERT INTO %s VALUES (2, 20.0, 'b')", table));
    spark.sql(str("INSERT INTO %s VALUES (3, 30.0, 'a')", table));

    check(
        str("SELECT id, value, part FROM %s ORDER BY id", table),
        List.of(row(1, 10.0, "a"), row(2, 20.0, "b"), row(3, 30.0, "a")));
  }
}
