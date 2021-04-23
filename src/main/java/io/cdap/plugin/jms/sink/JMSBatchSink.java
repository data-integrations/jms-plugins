/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.jms.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * A class that produces {@link StructuredRecord} to a JMS Queue or Topic.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("JMS")
@Description("JMSSink")
public class JMSBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, StructuredRecord> {

  private final JMSBatchSinkConfig config;

  public JMSBatchSink(JMSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validateParams(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().getFailureCollector().getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    Schema schema = context.getInputSchema();

    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);
      if (schema.getFields() != null && !schema.getFields().isEmpty()) {
        lineageRecorder.recordWrite("Write", "Wrote to JMS topic.",
                          schema.getFields().stream()
                            .map(Schema.Field::getName)
                            .collect(Collectors.toList()));
      }
    }

    context.addOutput(Output.of(config.referenceName, new JMSOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter)
    throws IOException {
    emitter.emit(new KeyValue<>(null, input));
  }
}
