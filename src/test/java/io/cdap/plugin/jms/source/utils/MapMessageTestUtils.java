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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSMessageParts;

import javax.jms.JMSException;
import javax.jms.MapMessage;

public class MapMessageTestUtils {

  private static final Boolean BOOLEAN_VAL = true;
  private static final Byte BYTE_VAL = (byte) 1;
  private static final Integer INT_VAL = 1;
  private static final Long LONG_VAL = 2L;
  private static final Float FLOAT_VAL = 3F;
  private static final Double DOUBLE_VAL = 4D;
  private static final String STRING_VAL = "Hello World!";

  public static Schema.Field getBodyFields() {
    return Schema.Field.of(
      JMSMessageParts.BODY,
      Schema.recordOf(
        JMSMessageParts.BODY,
        Schema.Field.of("booleanVal", Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of("byteVal", Schema.of(Schema.Type.BYTES)),
        Schema.Field.of("intVal", Schema.of(Schema.Type.INT)),
        Schema.Field.of("longVal", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("floatVal", Schema.of(Schema.Type.FLOAT)),
        Schema.Field.of("doubleVal", Schema.of(Schema.Type.DOUBLE)),
        Schema.Field.of("stringVal", Schema.of(Schema.Type.STRING))
      )
    );
  }

  public static void addBodyValuesToMessage(MapMessage message) throws JMSException {
    message.setBoolean("booleanVal", BOOLEAN_VAL);
    message.setByte("byteVal", BYTE_VAL);
    message.setInt("intVal", INT_VAL);
    message.setLong("longVal", LONG_VAL);
    message.setFloat("floatVal", FLOAT_VAL);
    message.setDouble("doubleVal", DOUBLE_VAL);
    message.setString("stringVal", STRING_VAL);
  }

  public static void addBodyValuesToRecord(StructuredRecord.Builder builder, Schema schema) {
    StructuredRecord bodyRecord = StructuredRecord.builder(schema.getField(JMSMessageParts.BODY).getSchema())
      .set("booleanVal", BOOLEAN_VAL)
      .set("byteVal", BYTE_VAL)
      .set("intVal", INT_VAL)
      .set("longVal", LONG_VAL)
      .set("floatVal", FLOAT_VAL)
      .set("doubleVal", DOUBLE_VAL)
      .set("stringVal", STRING_VAL)
      .build();
    builder.set(JMSMessageParts.BODY, bodyRecord);
  }

  public static String geBodyValuesAsJson() {
    return "{\"intVal\":1,\"doubleVal\":4.0,\"byteVal\":1,\"stringVal\":\"Hello World!\",\"booleanVal\":true," +
      "\"floatVal\":3.0,\"longVal\":2}";
  }
}
