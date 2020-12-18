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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Utils for {@link JMSBatchSink} plugin
 */
public class JMSBatchSinkUtils {

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link TextMessage}
   *
   * @param session the open session to the JMS broker
   * @param record the incoming record
   * @return a JMS text message
   */
  public static TextMessage convertStructuredRecordToTextMessage(Session session, StructuredRecord record) {
    TextMessage textMessage = null;
    String payload = null;

    try {
      payload = StructuredRecordStringConverter.toJsonString(record);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert record to json!", e);
    }

    try {
      textMessage = session.createTextMessage();
      textMessage.setText(payload);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return textMessage;
  }

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link MapMessage}
   *
   * @param outputSchema he output schema necessary to properly build the map message.
   * @param session the open session to the JMS broker
   * @param record the incoming record
   * @return a JMS map message
   */
  public static MapMessage convertStructuredRecordToMapMessage(Schema outputSchema, Session session,
                                                               StructuredRecord record) {
    MapMessage mapMessage = null;

    if (outputSchema == null) {
      throw new RuntimeException("Output schema is empty! Please provide the output schema.");
    }

    try {
      mapMessage = session.createMapMessage();
      for (Schema.Field field : outputSchema.getFields()) {
        String fieldName = field.getName();
        Object value = record.get(fieldName);

        switch (field.getSchema().getType()) {
          case INT:
            mapMessage.setInt(fieldName, cast(value, Integer.class));
            break;
          case LONG:
            mapMessage.setLong(fieldName, cast(value, Long.class));
            break;
          case DOUBLE:
            mapMessage.setDouble(fieldName, cast(value, Double.class));
            break;
          case FLOAT:
            mapMessage.setFloat(fieldName, cast(value, Float.class));
            break;
          case BOOLEAN:
            mapMessage.setBoolean(fieldName, cast(value, Boolean.class));
            break;
          case BYTES:
            mapMessage.setBytes(fieldName, cast(value, byte[].class));
            break;
          case ARRAY:
            mapMessage.setObject(fieldName, value);
            break;
          case RECORD:
            mapMessage.setObject(fieldName, value);
            break;
          case MAP:
            mapMessage.setObject(fieldName, value);
            break;
          default:
            mapMessage.setString(fieldName, cast(value, String.class));
        }
      }
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return mapMessage;
  }

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link BytesMessage}
   * @param session the open session to the JMS broker
   * @param record the incoming record
   * @return a JMS bytes message
   */
  public static BytesMessage convertStructuredRecordToBytesMessage(Session session, StructuredRecord record) {
    BytesMessage bytesMessage = null;
    byte[] payload = null;

    try {
      payload = StructuredRecordStringConverter.toJsonString(record).getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert record to json!", e);
    }

    try {
      bytesMessage = session.createBytesMessage();
      bytesMessage.writeBytes(payload);

    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return bytesMessage;
  }

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link ObjectMessage}
   *
   * @param session the open session to the JMS broker
   * @param record the incoming record
   * @return a JMS object message
   */
  public static ObjectMessage convertStructuredRecordToObjectMessage(Session session, StructuredRecord record) {
    ObjectMessage objectMessage = null;
    byte[] payload = null;

    try {
      payload = StructuredRecordStringConverter.toJsonString(record).getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert record to json!", e);
    }

    try {
      objectMessage = session.createObjectMessage();
      objectMessage.setObject(payload);

    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return objectMessage;
  }

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link Message}
   *
   * @param session the open session to the JMS broker
   * @param record the incoming record
   * @return a JMS message
   */
  public static Message convertStructuredRecordToMessage(Schema outputSchema, Session session,
                                                         StructuredRecord record) {
    Message message = null;

    if (outputSchema == null) {
      throw new RuntimeException("Output schema is empty! Please provide the output schema.");
    }

    try {
      message = session.createMessage();
      for (Schema.Field field : outputSchema.getFields()) {
        String fieldName = field.getName();
        Object value = record.get(fieldName);

        switch (field.getSchema().getType()) {
          case INT:
            message.setIntProperty(fieldName, cast(value, Integer.class));
            break;
          case LONG:
            message.setLongProperty(fieldName, cast(value, Long.class));
            break;
          case DOUBLE:
            message.setDoubleProperty(fieldName, cast(value, Double.class));
            break;
          case FLOAT:
            message.setFloatProperty(fieldName, cast(value, Float.class));
            break;
          case BOOLEAN:
            message.setBooleanProperty(fieldName, cast(value, Boolean.class));
            break;
          case ARRAY:
            message.setObjectProperty(fieldName, value);
            break;
          case RECORD:
            message.setObjectProperty(fieldName, value);
            break;
          case MAP:
            message.setObjectProperty(fieldName, value);
            break;
          default:
            message.setStringProperty(fieldName, cast(value, String.class));
        }
      }

    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return message;
  }

  public static <T> T cast(Object o, Class<T> clazz) {
    return o != null ? clazz.cast(o) : null;
  }
}
