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

package io.cdap.plugin.jms.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A class that specifies JMS message header fields.
 */
public class JMSMessageHeader {
  public static final String MESSAGE_ID = "messageId";
  public static final String MESSAGE_TIMESTAMP = "messageTimestamp";
  public static final String CORRELATION_ID = "correlationId";
  public static final String REPLY_TO = "replyTo";
  public static final String DESTINATION = "destination";
  public static final String DELIVERY_MODE = "deliveryNode";
  public static final String REDELIVERED = "redelivered";
  public static final String TYPE = "type";
  public static final String EXPIRATION = "expiration";
  public static final String PRIORITY = "priority";

  public static Schema.Field getMessageHeaderField() {
    return Schema.Field.of(JMSMessageParts.HEADER, Schema.recordOf(
      JMSMessageParts.HEADER,
      Schema.Field.of(MESSAGE_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(MESSAGE_TIMESTAMP, Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of(CORRELATION_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(REPLY_TO, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(DESTINATION, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(DELIVERY_MODE, Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of(REDELIVERED, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of(TYPE, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(EXPIRATION, Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of(PRIORITY, Schema.nullableOf(Schema.of(Schema.Type.INT)))));
  }

  public static List<String> getJMSMessageHeaderNames() {
    return Arrays.asList(MESSAGE_ID, MESSAGE_TIMESTAMP, CORRELATION_ID, REPLY_TO, DESTINATION, DELIVERY_MODE,
                         REDELIVERED, TYPE, EXPIRATION, PRIORITY);
  }

  public static String describe() {
    return getJMSMessageHeaderNames().stream().collect(Collectors.joining(", "));
  }


  /**
   * Gets header data fields from the JMS message and adds them to the passed record builder.
   *
   * @param schema  the entire schema of the record
   * @param builder the record builder that we set the header record into
   * @param message the incoming JMS message
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static void populateHeader(Schema schema, StructuredRecord.Builder builder, Message message)
    throws JMSException {
    Schema headerSchema = schema.getField(JMSMessageParts.HEADER).getSchema();
    StructuredRecord.Builder headerRecordBuilder = StructuredRecord.builder(headerSchema);

    for (Schema.Field field : headerSchema.getFields()) {

      switch (field.getName()) {
        case JMSMessageHeader.MESSAGE_ID:
          headerRecordBuilder.set(JMSMessageHeader.MESSAGE_ID, message.getJMSMessageID());
          break;

        case JMSMessageHeader.CORRELATION_ID:
          headerRecordBuilder.set(JMSMessageHeader.CORRELATION_ID, message.getJMSCorrelationID());
          break;

        case JMSMessageHeader.REPLY_TO:
          if (message.getJMSReplyTo() != null) {
            headerRecordBuilder.set(JMSMessageHeader.REPLY_TO, message.getJMSReplyTo().toString());
          } else {
            headerRecordBuilder.set(JMSMessageHeader.REPLY_TO, null);
          }
          break;

        case JMSMessageHeader.DESTINATION:
          if (message.getJMSDestination() != null) {
            headerRecordBuilder.set(JMSMessageHeader.DESTINATION, message.getJMSDestination().toString());
          } else {
            headerRecordBuilder.set(JMSMessageHeader.DESTINATION, null);
          }
          break;

        case JMSMessageHeader.TYPE:
          headerRecordBuilder.set(JMSMessageHeader.TYPE, message.getJMSType());
          break;

        case JMSMessageHeader.MESSAGE_TIMESTAMP:
          headerRecordBuilder.set(JMSMessageHeader.MESSAGE_TIMESTAMP, message.getJMSTimestamp());
          break;

        case JMSMessageHeader.DELIVERY_MODE:
          headerRecordBuilder.set(JMSMessageHeader.DELIVERY_MODE, message.getJMSDeliveryMode());
          break;

        case JMSMessageHeader.REDELIVERED:
          headerRecordBuilder.set(JMSMessageHeader.REDELIVERED, message.getJMSRedelivered());
          break;

        case JMSMessageHeader.EXPIRATION:
          headerRecordBuilder.set(JMSMessageHeader.EXPIRATION, message.getJMSExpiration());
          break;

        case JMSMessageHeader.PRIORITY:
          headerRecordBuilder.set(JMSMessageHeader.PRIORITY, message.getJMSPriority());
          break;
      }
    }
    builder.set(JMSMessageParts.HEADER, headerRecordBuilder.build());
  }
}

