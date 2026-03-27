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

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * Executor-side writer that writes {@link InternalRow} data to Parquet files.
 *
 * <p>For unpartitioned tables, all rows go to a single Parquet file. For partitioned tables, rows
 * are routed to partition-specific subdirectories based on partition column values.
 */
public class SparkDataWriter implements DataWriter<InternalRow> {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final StructType dataSchema;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;
  private final int[] partitionColumnIndices;
  private final DataType[] partitionColumnTypes;
  private final int[] dataColumnIndices;
  private final int partitionId;
  private final long taskId;
  private final OutputWriterFactory outputWriterFactory;
  private final TaskAttemptContext taskAttemptContext;

  /** Maps partition key string -> OutputWriter for partitioned tables. */
  private final Map<String, OutputWriter> partitionWriters = new LinkedHashMap<>();

  /** Maps partition key string -> partition values for the commit message. */
  private final Map<String, Map<String, String>> partitionValuesMap = new LinkedHashMap<>();

  /** Maps partition key string -> output file path. */
  private final Map<String, String> partitionFilePaths = new LinkedHashMap<>();

  /** Single writer for unpartitioned tables. */
  private OutputWriter singleWriter;

  private String singleFilePath;
  private boolean committed = false;

  SparkDataWriter(
      String tablePath,
      Configuration hadoopConf,
      StructType dataSchema,
      StructType writeSchema,
      List<String> partitionColumnNames,
      int[] partitionColumnIndices,
      DataType[] partitionColumnTypes,
      int[] dataColumnIndices,
      int partitionId,
      long taskId,
      OutputWriterFactory outputWriterFactory) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.dataSchema = dataSchema;
    this.writeSchema = writeSchema;
    this.partitionColumnNames = partitionColumnNames;
    this.partitionColumnIndices = partitionColumnIndices;
    this.partitionColumnTypes = partitionColumnTypes;
    this.dataColumnIndices = dataColumnIndices;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.outputWriterFactory = outputWriterFactory;

    TaskAttemptID taskAttemptID = new TaskAttemptID("", 0, TaskType.MAP, partitionId, 0);
    this.taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptID);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (partitionColumnNames.isEmpty()) {
      writeUnpartitioned(record);
    } else {
      writePartitioned(record);
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    List<DeltaWriterCommitMessage.WrittenFileInfo> files = new ArrayList<>();

    if (partitionColumnNames.isEmpty()) {
      if (singleWriter != null) {
        singleWriter.close();
        files.add(buildFileInfo(singleFilePath, Collections.emptyMap()));
      }
    } else {
      for (Map.Entry<String, OutputWriter> entry : partitionWriters.entrySet()) {
        String partKey = entry.getKey();
        entry.getValue().close();
        String filePath = partitionFilePaths.get(partKey);
        Map<String, String> partValues = partitionValuesMap.get(partKey);
        files.add(buildFileInfo(filePath, partValues));
      }
    }

    committed = true;
    return new DeltaWriterCommitMessage(files);
  }

  @Override
  public void abort() throws IOException {
    close();
    // Best-effort cleanup of written files
    cleanupFiles();
  }

  @Override
  public void close() throws IOException {
    if (!committed) {
      if (singleWriter != null) {
        singleWriter.close();
        singleWriter = null;
      }
      for (OutputWriter writer : partitionWriters.values()) {
        writer.close();
      }
      partitionWriters.clear();
    }
  }

  private void writeUnpartitioned(InternalRow record) throws IOException {
    if (singleWriter == null) {
      singleFilePath = generateFilePath(tablePath);
      singleWriter =
          outputWriterFactory.newInstance(singleFilePath, writeSchema, taskAttemptContext);
    }
    singleWriter.write(record);
  }

  private void writePartitioned(InternalRow record) throws IOException {
    // Extract partition values and build partition key
    Map<String, String> partitionValues = new LinkedHashMap<>();
    StringBuilder partKeyBuilder = new StringBuilder();
    for (int i = 0; i < partitionColumnNames.size(); i++) {
      int colIdx = partitionColumnIndices[i];
      String colName = partitionColumnNames.get(i);
      String value;
      if (record.isNullAt(colIdx)) {
        value = null;
      } else {
        value = record.get(colIdx, partitionColumnTypes[i]).toString();
      }
      partitionValues.put(colName, value);
      if (i > 0) partKeyBuilder.append("/");
      partKeyBuilder
          .append(colName)
          .append("=")
          .append(value == null ? "__HIVE_DEFAULT_PARTITION__" : value);
    }
    String partKey = partKeyBuilder.toString();

    OutputWriter writer = partitionWriters.get(partKey);
    if (writer == null) {
      String partDir = buildPartitionDir(partitionValues);
      String filePath = generateFilePath(partDir);
      writer = outputWriterFactory.newInstance(filePath, writeSchema, taskAttemptContext);
      partitionWriters.put(partKey, writer);
      partitionValuesMap.put(partKey, partitionValues);
      partitionFilePaths.put(partKey, filePath);
    }

    // Write the row with partition columns projected out
    writer.write(projectRow(record));
  }

  /**
   * Projects out partition columns from the row, returning only data columns to match writeSchema.
   */
  private InternalRow projectRow(InternalRow record) {
    Object[] values = new Object[dataColumnIndices.length];
    for (int i = 0; i < dataColumnIndices.length; i++) {
      int srcIdx = dataColumnIndices[i];
      if (record.isNullAt(srcIdx)) {
        values[i] = null;
      } else {
        values[i] = record.get(srcIdx, dataSchema.fields()[srcIdx].dataType());
      }
    }
    return new GenericInternalRow(values);
  }

  private String buildPartitionDir(Map<String, String> partitionValues) {
    StringBuilder dirBuilder = new StringBuilder(tablePath);
    for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
      String value = entry.getValue();
      String encodedValue = value == null ? "__HIVE_DEFAULT_PARTITION__" : escapePathName(value);
      dirBuilder.append("/").append(entry.getKey()).append("=").append(encodedValue);
    }
    return dirBuilder.toString();
  }

  private DeltaWriterCommitMessage.WrittenFileInfo buildFileInfo(
      String filePath, Map<String, String> partitionValues) throws IOException {
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(hadoopConf);
    long size = fs.getFileStatus(path).getLen();
    long modTime = System.currentTimeMillis();
    return new DeltaWriterCommitMessage.WrittenFileInfo(filePath, size, modTime, partitionValues);
  }

  private String generateFilePath(String dir) {
    String fileName = String.format("part-%05d-%s.snappy.parquet", partitionId, UUID.randomUUID());
    return dir + "/" + fileName;
  }

  private void cleanupFiles() {
    try {
      if (singleFilePath != null) {
        Path path = new Path(singleFilePath);
        path.getFileSystem(hadoopConf).delete(path, false);
      }
      for (String filePath : partitionFilePaths.values()) {
        Path path = new Path(filePath);
        path.getFileSystem(hadoopConf).delete(path, false);
      }
    } catch (IOException e) {
      // Best-effort cleanup
    }
  }

  /** Escapes special characters in partition values for use in directory names. */
  private static String escapePathName(String value) {
    StringBuilder sb = new StringBuilder();
    for (char c : value.toCharArray()) {
      if (c == '/' || c == '=' || c == '%' || c == ' ' || c < 0x20) {
        sb.append(String.format("%%%02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
