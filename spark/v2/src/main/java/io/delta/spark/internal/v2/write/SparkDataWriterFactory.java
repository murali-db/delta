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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Serializable factory that is shipped to executors to create {@link SparkDataWriter} instances.
 *
 * <p>Created on the driver by {@link SparkBatchWrite#createBatchWriterFactory} and serialized to
 * each executor. Contains all the configuration needed for Parquet file writing.
 */
public class SparkDataWriterFactory implements DataWriterFactory {

  private final String tablePath;
  private final SerializableConfiguration hadoopConf;
  private final StructType dataSchema;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;
  private final OutputWriterFactory outputWriterFactory;

  // Precomputed partition column metadata
  private final int[] partitionColumnIndices;
  private final DataType[] partitionColumnTypes;
  private final int[] dataColumnIndices;

  SparkDataWriterFactory(
      String tablePath,
      SerializableConfiguration hadoopConf,
      StructType dataSchema,
      StructType writeSchema,
      List<String> partitionColumnNames,
      OutputWriterFactory outputWriterFactory) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.dataSchema = dataSchema;
    this.writeSchema = writeSchema;
    this.partitionColumnNames = partitionColumnNames;
    this.outputWriterFactory = outputWriterFactory;

    // Precompute partition column indices and types
    this.partitionColumnIndices = new int[partitionColumnNames.size()];
    this.partitionColumnTypes = new DataType[partitionColumnNames.size()];
    Set<Integer> partitionIndexSet = new HashSet<>();
    for (int i = 0; i < partitionColumnNames.size(); i++) {
      int idx = dataSchema.fieldIndex(partitionColumnNames.get(i));
      partitionColumnIndices[i] = idx;
      partitionColumnTypes[i] = dataSchema.fields()[idx].dataType();
      partitionIndexSet.add(idx);
    }

    // Precompute data (non-partition) column indices
    List<Integer> dataColIdxList = new ArrayList<>();
    for (int i = 0; i < dataSchema.fields().length; i++) {
      if (!partitionIndexSet.contains(i)) {
        dataColIdxList.add(i);
      }
    }
    this.dataColumnIndices = dataColIdxList.stream().mapToInt(Integer::intValue).toArray();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new SparkDataWriter(
        tablePath,
        hadoopConf.value(),
        dataSchema,
        writeSchema,
        partitionColumnNames,
        partitionColumnIndices,
        partitionColumnTypes,
        dataColumnIndices,
        partitionId,
        taskId,
        outputWriterFactory);
  }
}
