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
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;
import org.junit.Assert;

import javax.jms.Destination;

public class CommonTestUtils {

  public static JMSStreamingSourceConfig getSourceConfig(boolean messageHeader, boolean messageProperties,
                                                         String messageType, String schema) {
    return new JMSStreamingSourceConfig("referenceName", "Connection Factory", "jms-username", "jms-password",
                                        "tcp://0.0.0.0:61616", "Queue", "jndi-context-factory", "jndi-username",
                                        "jndi-password", String.valueOf(messageHeader),
                                        String.valueOf(messageProperties), messageType, "MyQueue", schema);
  }

  public static Destination getDummyDestination() {
    return new Destination() {
      @Override
      public String toString() {
        return "Destination";
      }
    };
  }

  public static void assertEqualsStructuredRecords(StructuredRecord expected, StructuredRecord actual) {
    for (Schema.Field field : expected.getSchema().getFields()) {
      Schema.Type type = field.getSchema().getType();

      if (type.equals(Schema.Type.RECORD)) {
        assertEqualsStructuredRecords(expected.get(field.getName()), actual.get(field.getName()));
      }

      if (type.equals(Schema.Type.UNION)) {
        type = field.getSchema().getUnionSchema(0).getType();
      }

      switch (type) {
        case BOOLEAN:
          Assert.assertEquals(expected.<Boolean>get(field.getName()), actual.<Boolean>get(field.getName()));
          break;
        case INT:
          Assert.assertEquals(expected.<Integer>get(field.getName()), actual.<Integer>get(field.getName()));
          break;
        case LONG:
          Assert.assertEquals(expected.<Long>get(field.getName()), actual.<Long>get(field.getName()));
          break;
        case FLOAT:
          Assert.assertEquals(expected.<Float>get(field.getName()), actual.<Float>get(field.getName()));
          break;
        case DOUBLE:
          Assert.assertEquals(expected.<Double>get(field.getName()), actual.<Double>get(field.getName()));
          break;
        case BYTES:
          Assert.assertEquals(expected.<Byte>get(field.getName()), actual.<Byte>get(field.getName()));
          break;
        case STRING:
          Assert.assertEquals(expected.<String>get(field.getName()), actual.<String>get(field.getName()));
          break;
        case ARRAY:
          Assert.assertArrayEquals(expected.<byte[]>get(field.getName()),
                             actual.<byte[]>get(field.getName()));
          break;
      }
    }
  }
}
