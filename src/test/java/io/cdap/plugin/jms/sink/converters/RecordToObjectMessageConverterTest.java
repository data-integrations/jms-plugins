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
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;

public class RecordToObjectMessageConverterTest {

  @Test
  public void convertRecordToObjectMessage_Successfully() throws JMSException {
    // expected
    String expectedStringBody = "{\"City\":\"Denver\",\"Population\":705000}";

    // actual
    ObjectMessage objectMessage = new ActiveMQObjectMessage();
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("City", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Population", Schema.of(Schema.Type.INT))
    );

    StructuredRecord record = StructuredRecord
      .builder(schema)
      .set("City", "Denver")
      .set("Population", 705000)
      .build();

    ObjectMessage actualMessage = RecordToObjectMessageConverter.toObjectMessage(objectMessage, record);
    byte[] actualBody = SerializationUtils.serialize(actualMessage.getObject());
    String actualStringBody = new String(actualBody, StandardCharsets.UTF_8);

    // assert
    assertContains(expectedStringBody, actualStringBody);
  }

  private static void assertContains(String expected, String actual) {
    Assert.assertTrue(actual.contains(expected));
  }
}
