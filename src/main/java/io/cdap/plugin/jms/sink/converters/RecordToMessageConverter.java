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

package io.cdap.plugin.jms.sink.converters;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A class with the functionality to convert StructuredRecords to Messages.
 */
public class RecordToMessageConverter {

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link Message}
   *
   * @param message the jms message to be populated with data
   * @param record the incoming record
   * @return a JMS message
   */
  public static Message toMessage(Message message, StructuredRecord record) {
    try {
      for (Schema.Field field : record.getSchema().getFields()) {
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
          case BYTES:
            message.setByteProperty(fieldName, cast(value, Byte.class));
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
