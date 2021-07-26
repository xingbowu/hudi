/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorFactory;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Map;

import static org.apache.hudi.util.StreamerUtil.createHoodieDataStreamSink;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final Configuration conf;
  private final TableSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, TableSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) dataStream -> {
      // Read from kafka source
      RowType rowType = (RowType) schema.toRowDataType().notNull().getLogicalType();
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      int parallelism = dataStream.getExecutionConfig().getParallelism();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
      StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);
      return createHoodieDataStreamSink(rowType, conf, parallelism, operatorFactory, dataStream);
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    // ignore RowKind.UPDATE_BEFORE
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.schema);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite) {
      final String operationType;
      if (partition.size() > 0) {
        operationType = WriteOperationType.INSERT_OVERWRITE.value();
      } else {
        operationType = WriteOperationType.INSERT_OVERWRITE_TABLE.value();
      }
      this.conf.setString(FlinkOptions.OPERATION, operationType);
    }
  }

  @Override
  public void applyOverwrite(boolean b) {
    this.overwrite = b;
  }
}
