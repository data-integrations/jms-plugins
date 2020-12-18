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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSConnection;
import io.cdap.plugin.jms.sink.converters.SinkMessageConverterFacade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;

/**
 * Record writer to produce messages to a JMS Topic/Queue.
 */
public class JMSRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(JMSRecordWriter.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final JMSBatchSinkConfig config;
  private Connection connection;
  private Session session;
  private MessageProducer messageProducer;
  private JMSConnection jmsConnection;

  public JMSRecordWriter(TaskAttemptContext context) {
    Configuration config = context.getConfiguration();
    String configJson = config.get(JMSOutputFormatProvider.PROPERTY_CONFIG_JSON);
    this.config = GSON.fromJson(configJson, JMSBatchSinkConfig.class);
    this.jmsConnection = new JMSConnection(this.config);
    establishConnection();
  }

  @Override
  public void write(NullWritable key, StructuredRecord record) {
    String messageType = config.getMessageType();
    Schema outputSchema = config.getSchema();
    Message message = null;
    try {
      message = SinkMessageConverterFacade.toJmsMessage(session, record, outputSchema, messageType);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    produceMessage(message);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    this.jmsConnection.stopConnection(this.connection);
    this.jmsConnection.closeSession(this.session);
    this.jmsConnection.closeConnection(this.connection);
  }

  private void produceMessage(Message message) {
    try {
      messageProducer.send(message);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  private void establishConnection() {
    Context context = jmsConnection.getContext();
    ConnectionFactory factory = jmsConnection.getConnectionFactory(context);
    connection = jmsConnection.createConnection(factory);
    session = jmsConnection.createSession(connection);
    Destination destination = jmsConnection.getSink(context, session);
    messageProducer = jmsConnection.createProducer(session, destination);
  }
}
