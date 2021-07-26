/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.streamer;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

import static org.apache.hudi.util.StreamerUtil.createHoodieDataStreamSink;

/**
 * An Utility which can incrementally consume data from Kafka and apply it to the target table.
 * currently, it only support COW table and insert, upsert operation.
 * <p>
 * note: HoodieFlinkStreamer is not suitable to initialize on large tables when we have no checkpoint to restore from.
 */
public class HoodieFlinkStreamer {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    env.enableCheckpointing(cfg.checkpointInterval);
    env.getConfig().setGlobalJobParameters(cfg);
    // We use checkpoint to trigger write operation, including instant generating and committing,
    // There can only be one checkpoint at one time.
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    if (cfg.flinkCheckPointPath != null) {
      env.setStateBackend(new FsStateBackend(cfg.flinkCheckPointPath));
    }

    Properties kafkaProps = StreamerUtil.appendKafkaProps(cfg);

    // Read from kafka source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(cfg))
            .getLogicalType();

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    int parallelism = env.getParallelism();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

    StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
        new StreamWriteOperatorFactory<>(conf);

    DataStream<RowData> dataStream = env.addSource(new FlinkKafkaConsumer<>(
        cfg.kafkaTopic,
        new JsonRowDataDeserializationSchema(
            rowType,
            InternalTypeInfo.of(rowType),
            false,
            true,
            TimestampFormat.ISO_8601
        ), kafkaProps))
        .name("kafka_source")
        .uid("uid_kafka_source");

    if (cfg.transformerClassNames != null && !cfg.transformerClassNames.isEmpty()) {
      Option<Transformer> transformer = StreamerUtil.createTransformer(cfg.transformerClassNames);
      if (transformer.isPresent()) {
        dataStream = transformer.get().apply(dataStream);
      }
    }
    createHoodieDataStreamSink(rowType, conf, parallelism, operatorFactory, dataStream);
    env.execute(cfg.targetTableName);
  }
}
