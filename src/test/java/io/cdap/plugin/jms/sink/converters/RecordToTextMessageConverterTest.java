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
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.TextMessage;

public class RecordToTextMessageConverterTest {

  @Test
  public void convertRecordToTextMessage_WithSingleStringField_Successfully() throws JMSException {
    // expected
    String expectedBody = "Hello World!";

    // actual
    TextMessage textMessage = new ActiveMQTextMessage();
    Schema schema = Schema.recordOf("record", Schema.Field.of("text", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(schema).set("text", "Hello World!").build();
    TextMessage actualMessage = RecordToTextMessageConverter.toTextMessage(textMessage, record);
    String actualBody = actualMessage.getText();

    // assert
    Assert.assertEquals(expectedBody, actualBody);
  }

  @Test
  public void convertRecordToTextMessage_WithMultiplesFields_Successfully() throws JMSException {
    // expected
    String expectedBody = "{\"Name\":\"James\",\"Surname\":\"Bond\"}";

    // actual
    TextMessage textMessage = new ActiveMQTextMessage();
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Surname", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord record = StructuredRecord
      .builder(schema)
      .set("Name", "James")
      .set("Surname", "Bond")
      .build();

    TextMessage actualMessage = RecordToTextMessageConverter.toTextMessage(textMessage, record);
    String actualBody = actualMessage.getText();

    // assert
    Assert.assertEquals(expectedBody, actualBody);
  }
}
