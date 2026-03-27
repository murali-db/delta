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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Entry point for DSv2 batch writes to Delta tables. Creates a kernel Transaction and returns a
 * {@link SparkWrite} that orchestrates the write.
 *
 * <p>For milestone 1, only blind append is supported (no overwrite, truncate, or dynamic
 * overwrite).
 */
public class SparkWriteBuilder implements WriteBuilder {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final StructType tableSchema;
  private final List<String> partitionColumnNames;
  private final LogicalWriteInfo writeInfo;

  public SparkWriteBuilder(
      String tablePath,
      Configuration hadoopConf,
      StructType tableSchema,
      List<String> partitionColumnNames,
      LogicalWriteInfo writeInfo) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.tableSchema = tableSchema;
    this.partitionColumnNames = partitionColumnNames;
    this.writeInfo = writeInfo;
  }

  @Override
  public Write build() {
    // The dataSchema is the full table schema (what the rows will look like)
    StructType dataSchema = tableSchema;

    // The writeSchema is the schema without partition columns (what goes into Parquet files)
    StructType writeSchema = buildWriteSchema(dataSchema, partitionColumnNames);

    // Create kernel Transaction
    Engine engine = DefaultEngine.create(hadoopConf);
    Table table = Table.forPath(engine, tablePath);
    Transaction txn =
        table.createTransactionBuilder(engine, "delta-spark-dsv2", Operation.WRITE).build(engine);
    Row txnState = txn.getTransactionState(engine);

    return new SparkWrite(
        txn,
        txnState,
        engine,
        tablePath,
        hadoopConf,
        dataSchema,
        writeSchema,
        partitionColumnNames);
  }

  /** Builds the write schema by removing partition columns from the full data schema. */
  private static StructType buildWriteSchema(
      StructType dataSchema, List<String> partitionColumnNames) {
    if (partitionColumnNames.isEmpty()) {
      return dataSchema;
    }
    Set<String> partCols = new HashSet<>(partitionColumnNames);
    List<StructField> writeFields = new ArrayList<>();
    for (StructField field : dataSchema.fields()) {
      if (!partCols.contains(field.name())) {
        writeFields.add(field);
      }
    }
    return new StructType(writeFields.toArray(new StructField[0]));
  }
}
