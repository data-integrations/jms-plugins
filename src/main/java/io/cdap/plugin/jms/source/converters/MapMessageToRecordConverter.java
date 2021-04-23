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

package io.cdap.plugin.jms.source.converters;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageProperties;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;

import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

/**
 * A class with the functionality to convert MapMessages to StructuredRecords.
 */
public class MapMessageToRecordConverter {

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link MapMessage}
   *
   * @param message the incoming JMS {@link MapMessage}
   * @param config  the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link MapMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord mapMessageToRecord(Message message, JMSStreamingSourceConfig config)
    throws JMSException {

    Schema schema = config.getSchema();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    if (config.getMessageHeader()) {
      JMSMessageHeader.populateHeader(schema, recordBuilder, message);
    }
    if (config.getMessageProperties()) {
      JMSMessageProperties.populateProperties(schema, recordBuilder, message, JMSMessageType.MESSAGE);
    }

    Schema.Type bodyFieldType = schema.getField(JMSMessageParts.BODY).getSchema().getType();

    if (bodyFieldType.equals(Schema.Type.STRING)) {
      withRecordBody(message, recordBuilder);
    } else if (bodyFieldType.equals(Schema.Type.RECORD)) {
      withRecordBody(message, schema, recordBuilder, config);
    }

    return recordBuilder.build();
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link MapMessage} when body is of data type string
   *
   * @param message the incoming JMS {@link MapMessage}
   * @param builder the {@link StructuredRecord.Builder} to enrich with body
   * @throws JMSException
   */
  private static void withRecordBody(Message message, StructuredRecord.Builder builder)
    throws JMSException {
    Map<String, Object> body = new LinkedHashMap<>();
    Enumeration<String> names = ((MapMessage) message).getMapNames();
    while (names.hasMoreElements()) {
      String key = names.nextElement();
      body.put(key, ((MapMessage) message).getObject(key));
    }
    builder.set(JMSMessageParts.BODY, new Gson().toJson(body));
  }

  /**
   * Converts a {@link MapMessage} to a {@link StructuredRecord} when body is of data type record
   *
   * @param message the incoming JMS map message
   * @param builder the {@link StructuredRecord.Builder} to enrich with body
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @throws JMSException
   */
  private static void withRecordBody(Message message, Schema schema, StructuredRecord.Builder builder,
                                     JMSStreamingSourceConfig config)
    throws JMSException {
    Schema bodySchema = schema.getField(JMSMessageParts.BODY).getSchema();
    StructuredRecord.Builder bodyRecordBuilder = StructuredRecord.builder(bodySchema);

    for (Schema.Field field : bodySchema.getFields()) {
      Schema.Type type = field.getSchema().getType();
      String name = field.getName();

      if (type.equals(Schema.Type.UNION)) {
        type = field.getSchema().getUnionSchema(0).getType();
      }

      switch (type) {
        case BOOLEAN:
          bodyRecordBuilder.set(name, ((MapMessage) message).getBoolean(name));
          break;
        case INT:
          bodyRecordBuilder.set(name, ((MapMessage) message).getInt(name));
          break;
        case LONG:
          bodyRecordBuilder.set(name, ((MapMessage) message).getLong(name));
          break;
        case FLOAT:
          bodyRecordBuilder.set(name, ((MapMessage) message).getFloat(name));
          break;
        case DOUBLE:
          bodyRecordBuilder.set(name, ((MapMessage) message).getDouble(name));
          break;
        case BYTES:
          bodyRecordBuilder.set(name, ((MapMessage) message).getByte(name));
          break;
        case STRING:
          bodyRecordBuilder.set(name, ((MapMessage) message).getString(name));
          break;
        case ARRAY: // byte array only
          Schema.Type itemType = field.getSchema().getComponentSchema().getType();
          if (itemType.equals(Schema.Type.UNION)) {
            itemType = field.getSchema().getComponentSchema().getUnionSchema(0).getType();
          }
          if (itemType.equals(Schema.Type.BYTES)) {
            bodyRecordBuilder.set(name, ((MapMessage) message).getBytes(name));
          }
          break;
      }
    }
    builder.set(JMSMessageParts.BODY, bodyRecordBuilder.build());
  }
}
