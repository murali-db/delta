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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Carries file metadata from executor-side {@link SparkDataWriter} back to the driver-side {@link
 * SparkBatchWrite} for the Delta commit.
 */
public class DeltaWriterCommitMessage implements WriterCommitMessage {

  private final List<WrittenFileInfo> writtenFiles;

  public DeltaWriterCommitMessage(List<WrittenFileInfo> writtenFiles) {
    this.writtenFiles = Collections.unmodifiableList(writtenFiles);
  }

  public List<WrittenFileInfo> getWrittenFiles() {
    return writtenFiles;
  }

  /** Metadata about a single Parquet file written by a task. */
  public static class WrittenFileInfo implements Serializable {
    private final String path;
    private final long sizeInBytes;
    private final long modificationTime;
    private final Map<String, String> partitionValues;

    public WrittenFileInfo(
        String path, long sizeInBytes, long modificationTime, Map<String, String> partitionValues) {
      this.path = path;
      this.sizeInBytes = sizeInBytes;
      this.modificationTime = modificationTime;
      this.partitionValues = Collections.unmodifiableMap(partitionValues);
    }

    public String getPath() {
      return path;
    }

    public long getSizeInBytes() {
      return sizeInBytes;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public Map<String, String> getPartitionValues() {
      return partitionValues;
    }
  }
}
