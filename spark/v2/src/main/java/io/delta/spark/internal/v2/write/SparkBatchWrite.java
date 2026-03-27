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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static io.delta.spark.internal.v2.utils.ScalaUtils.toScalaMap;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Driver-side orchestrator for DSv2 batch writes. Creates the writer factory that gets serialized
 * to executors, then commits the written files via the kernel Transaction API.
 */
public class SparkBatchWrite implements BatchWrite {

  private final Transaction transaction;
  private final Row transactionState;
  private final Engine engine;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final StructType dataSchema;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;

  SparkBatchWrite(
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
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    // Create the OutputWriterFactory on the driver using ParquetFileFormat
    SparkSession spark = SparkSession.active();
    ParquetFileFormat parquetFormat = new ParquetFileFormat();
    try {
      Job job = Job.getInstance(hadoopConf);
      OutputWriterFactory writerFactory =
          parquetFormat.prepareWrite(spark, job, toScalaMap(Collections.emptyMap()), writeSchema);

      // Capture the Job's modified Configuration (includes Parquet settings)
      SerializableConfiguration serializableConf =
          new SerializableConfiguration(job.getConfiguration());

      return new SparkDataWriterFactory(
          tablePath,
          serializableConf,
          dataSchema,
          writeSchema,
          partitionColumnNames,
          writerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create write factory", e);
    }
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    try {
      List<Row> allActions = new ArrayList<>();

      // Group files by partition values
      Map<String, List<DeltaWriterCommitMessage.WrittenFileInfo>> partitionGroups =
          new LinkedHashMap<>();
      for (WriterCommitMessage message : messages) {
        if (message == null) continue;
        DeltaWriterCommitMessage deltaMsg = (DeltaWriterCommitMessage) message;
        for (DeltaWriterCommitMessage.WrittenFileInfo fileInfo : deltaMsg.getWrittenFiles()) {
          String partKey = buildPartitionKey(fileInfo.getPartitionValues());
          partitionGroups.computeIfAbsent(partKey, k -> new ArrayList<>()).add(fileInfo);
        }
      }

      // Generate Delta AddFile actions for each partition group
      for (Map.Entry<String, List<DeltaWriterCommitMessage.WrittenFileInfo>> entry :
          partitionGroups.entrySet()) {

        List<DeltaWriterCommitMessage.WrittenFileInfo> files = entry.getValue();
        // Get partition values as kernel Literals
        Map<String, Literal> partitionValues = toKernelPartitionValues(files.get(0));

        DataWriteContext writeContext =
            Transaction.getWriteContext(engine, transactionState, partitionValues);

        // Convert file infos to DataFileStatus objects
        List<DataFileStatus> fileStatuses = new ArrayList<>();
        for (DeltaWriterCommitMessage.WrittenFileInfo fileInfo : files) {
          fileStatuses.add(
              new DataFileStatus(
                  fileInfo.getPath(),
                  fileInfo.getSizeInBytes(),
                  fileInfo.getModificationTime(),
                  Optional.empty()));
        }

        try (CloseableIterator<Row> actions =
            Transaction.generateAppendActions(
                engine,
                transactionState,
                toCloseableIterator(fileStatuses.iterator()),
                writeContext)) {
          while (actions.hasNext()) {
            allActions.add(actions.next());
          }
        }
      }

      // Commit the transaction
      CloseableIterable<Row> actionsIterable =
          CloseableIterable.inMemoryIterable(toCloseableIterator(allActions.iterator()));
      TransactionCommitResult result = transaction.commit(engine, actionsIterable);

      // Run post-commit hooks (e.g., checkpointing)
      for (PostCommitHook hook : result.getPostCommitHooks()) {
        hook.threadSafeInvoke(engine);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to commit Delta transaction", e);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // Best-effort cleanup: the written files will be orphaned but not committed
    // to the Delta log, so they won't affect table reads. A VACUUM operation
    // will clean them up.
  }

  private String buildPartitionKey(Map<String, String> partitionValues) {
    if (partitionValues.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
      if (sb.length() > 0) sb.append("/");
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  private Map<String, Literal> toKernelPartitionValues(
      DeltaWriterCommitMessage.WrittenFileInfo fileInfo) {
    Map<String, String> stringValues = fileInfo.getPartitionValues();
    if (stringValues.isEmpty()) return Collections.emptyMap();

    Map<String, Literal> kernelValues = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : stringValues.entrySet()) {
      String value = entry.getValue();
      if (value == null) {
        // Find the partition column's kernel type for a null literal
        kernelValues.put(entry.getKey(), Literal.ofNull(findKernelPartitionType(entry.getKey())));
      } else {
        kernelValues.put(entry.getKey(), Literal.ofString(value));
      }
    }
    return kernelValues;
  }

  private io.delta.kernel.types.DataType findKernelPartitionType(String colName) {
    // For null values, we need the kernel DataType. For simplicity in the prototype,
    // use StringType since partition values are string-encoded.
    return io.delta.kernel.types.StringType.STRING;
  }
}
