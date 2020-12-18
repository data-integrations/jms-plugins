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

package io.cdap.plugin.jms.source.utils;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSMessageParts;

import java.util.HashMap;
import javax.jms.JMSException;
import javax.jms.Message;

public class PropertiesTestUtils {

  private static final Boolean BOOLEAN_PROPERTY = true;
  private static final Byte BYTE_PROPERTY = (byte) 1;
  private static final Integer INT_PROPERTY = 3;
  private static final Long LONG_PROPERTY = 4L;
  private static final Float FLOAT_PROPERTY = 5F;
  private static final Double DOUBLE_PROPERTY = 6D;
  private static final String STRING_PROPERTY = "Hello World!";

  public static void addPropertiesToMessage(Message message) throws JMSException {
    message.setBooleanProperty("booleanProperty", BOOLEAN_PROPERTY);
    message.setByteProperty("byteProperty", BYTE_PROPERTY);
    message.setIntProperty("intProperty", INT_PROPERTY);
    message.setLongProperty("longProperty", LONG_PROPERTY);
    message.setFloatProperty("floatProperty", FLOAT_PROPERTY);
    message.setDoubleProperty("doubleProperty", DOUBLE_PROPERTY);
    message.setStringProperty("stringProperty", STRING_PROPERTY);
  }

  public static void addPropertiesToRecord(StructuredRecord.Builder builder, Schema schema) {
    Schema.Type propertiesFieldType = schema.getField(JMSMessageParts.PROPERTIES).getSchema().getType();
    if (propertiesFieldType.equals(Schema.Type.STRING)) {
      withStringPropertiesField(builder);
    } else if (propertiesFieldType.equals(Schema.Type.RECORD)) {
      withRecordPropertiesField(builder, schema);
    }
  }

  private static void withStringPropertiesField(StructuredRecord.Builder builder) {
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("booleanProperty", BOOLEAN_PROPERTY);
    properties.put("byteProperty", BYTE_PROPERTY);
    properties.put("intProperty", INT_PROPERTY);
    properties.put("longProperty", LONG_PROPERTY);
    properties.put("floatProperty", FLOAT_PROPERTY);
    properties.put("doubleProperty", DOUBLE_PROPERTY);
    properties.put("stringProperty", STRING_PROPERTY);
    builder.set(JMSMessageParts.PROPERTIES, new Gson().toJson(properties));
  }

  private static void withRecordPropertiesField(StructuredRecord.Builder builder, Schema schema) {
    StructuredRecord propertiesRecord = StructuredRecord
      .builder(schema.getField(JMSMessageParts.PROPERTIES).getSchema())
      .set("booleanProperty", BOOLEAN_PROPERTY)
      .set("byteProperty", BYTE_PROPERTY)
      .set("intProperty", INT_PROPERTY)
      .set("longProperty", LONG_PROPERTY)
      .set("floatProperty", FLOAT_PROPERTY)
      .set("doubleProperty", DOUBLE_PROPERTY)
      .set("stringProperty", STRING_PROPERTY)
      .build();
    builder.set(JMSMessageParts.PROPERTIES, propertiesRecord);
  }

  public static Schema.Field getPropertiesField() {
    return Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.recordOf(
      JMSMessageParts.PROPERTIES,
      Schema.Field.of("booleanProperty", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("byteProperty", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("intProperty", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longProperty", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("floatProperty", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleProperty", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("stringProperty", Schema.of(Schema.Type.STRING))
                           )
    );
  }
}
