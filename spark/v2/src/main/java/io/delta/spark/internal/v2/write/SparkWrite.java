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

import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

/**
 * Represents a logical Delta write operation. Returns a {@link SparkBatchWrite} for batch append
 * writes.
 */
public class SparkWrite implements Write {

  private final Transaction transaction;
  private final Row transactionState;
  private final Engine engine;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final StructType dataSchema;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;

  SparkWrite(
      Transaction transaction,
      Row transactionState,
      Engine engine,
      String tablePath,
      Configuration hadoopConf,
      StructType dataSchema,
      StructType writeSchema,
      List<String> partitionColumnNames) {
    this.transaction = transaction;
    this.transactionState = transactionState;
    this.engine = engine;
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.dataSchema = dataSchema;
    this.writeSchema = writeSchema;
    this.partitionColumnNames = partitionColumnNames;
  }

  @Override
  public BatchWrite toBatch() {
    return new SparkBatchWrite(
        transaction,
        transactionState,
        engine,
        tablePath,
        hadoopConf,
        dataSchema,
        writeSchema,
        partitionColumnNames);
  }

  @Override
  public String description() {
    return "DeltaDSv2Write(path=" + tablePath + ")";
  }
}
