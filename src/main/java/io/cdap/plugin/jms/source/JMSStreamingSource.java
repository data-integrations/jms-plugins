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

package io.cdap.plugin.jms.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.stream.Collectors;

/**
 * This class is a plugin that allows consuming messages from a specified JMS Queue/Topic and generate
 * StructuredRecords out of them.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("JMS")
@Description("JMSSource")
public class JMSStreamingSource extends ReferenceStreamingSource<StructuredRecord> {

  private JMSStreamingSourceConfig config;

  public JMSStreamingSource(JMSStreamingSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = config.getSchema();
    context.registerLineage(config.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from jms",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    return JMSSourceUtils.getJavaDStream(context, config);
  }
}
