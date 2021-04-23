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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A class that specifies JMS message properties fields.
 */
public class JMSMessageProperties {

  /**
   * Adds properties values in the structured record.
   *
   * @param schema the structured record schema
   * @param builder the structured record builder
   * @param message the JMS message
   * @param messageType the JMS message type
   * @throws JMSException
   */
  public static void populateProperties(Schema schema, StructuredRecord.Builder builder, Message message,
                                        String messageType) throws JMSException {
    Schema.Type propertiesFieldType = schema.getField(JMSMessageParts.PROPERTIES).getSchema().getType();

    if (propertiesFieldType.equals(Schema.Type.STRING)) {
      populatePropertiesOnStringSchema(builder, message);
    } else if (propertiesFieldType.equals(Schema.Type.RECORD)) {
      populatePropertiesOnRecordSchema(schema, builder, message, messageType);
    } else {
      throw new RuntimeException(
        String.format("Failed to populate properties! Field %s can only support String or Record data types!",
                      JMSMessageParts.PROPERTIES)
      );
    }
  }


  /**
   * @param builder
   * @param message
   * @throws JMSException
   */
  public static void populatePropertiesOnStringSchema(StructuredRecord.Builder builder, Message message)
     throws JMSException {
    HashMap<String, Object> properties = new HashMap<>();
    List<String> listOfPropertyNames = Collections.list(message.getPropertyNames());

    for (String propertyName : listOfPropertyNames) {
      properties.put(propertyName, message.getObjectProperty(propertyName));
    }

    builder.set(JMSMessageParts.PROPERTIES, new Gson().toJson(properties));
  }


  /**
   * @param schema        the entire schema of the record
   * @param recordBuilder the record builder that we set the properties record into
   * @param message       the incoming JMS message
   * @param messageType   the incoming JMS message type
   * @throws JMSException
   */
  public static void populatePropertiesOnRecordSchema(Schema schema, StructuredRecord.Builder recordBuilder,
                                                      Message message, String messageType) throws JMSException {
    Schema propertiesSchema = schema.getField(JMSMessageParts.PROPERTIES).getSchema();

    StructuredRecord.Builder propertiesRecordBuilder = StructuredRecord.builder(propertiesSchema);

    for (Schema.Field field : propertiesSchema.getFields()) {
      String name = field.getName();
      Schema.Type type = field.getSchema().getType();

      if (!message.propertyExists(field.getName())) {
        throw new RuntimeException(
          String.format("Property \"%1$s\" does not exist in the incoming \"%2$s\" message! " +
                          "Make sure that you have specified a correct field name in the output schema that " +
                          "matches a property name in the incoming \"%2$s\" message.", field.getName(), messageType));
      }

      switch (type) {
        case BOOLEAN:
          propertiesRecordBuilder.set(name, message.getBooleanProperty(name));
          continue;
        case BYTES:
          propertiesRecordBuilder.set(name, message.getByteProperty(name));
          continue;
        case INT:
          propertiesRecordBuilder.set(name, message.getIntProperty(name));
//            short getShortProperty(String var1) throws JMSException;
          continue;
        case LONG:
          propertiesRecordBuilder.set(name, message.getLongProperty(name));
          continue;
        case FLOAT:
          propertiesRecordBuilder.set(name, message.getFloatProperty(name));
          continue;
        case DOUBLE:
          propertiesRecordBuilder.set(name, message.getDoubleProperty(name));
          continue;
        case STRING:
          propertiesRecordBuilder.set(name, message.getStringProperty(name));
          continue;
        default:
          propertiesRecordBuilder.set(name, message.getObjectProperty(name));
          continue;
      }
    }
    recordBuilder.set(JMSMessageParts.PROPERTIES, propertiesRecordBuilder.build());
  }
}
