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
import org.apache.activemq.command.ActiveMQMapMessage;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;

public class RecordToMessageConverterTest {

  @Test
  public void convertRecordToMessage_Successfully() throws JMSException {
    // actual
    Message message = new ActiveMQMapMessage();

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("IsWorkingStudent", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("HasWorkExperience", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("Position", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord record = StructuredRecord
      .builder(schema)
      .set("IsWorkingStudent", true)
      .set("HasWorkExperience", false)
      .set("Position", "Frontend developer")
      .build();

    Message actualMessage = RecordToMessageConverter.toMessage(message, record);

    // Asserts
    Assert.assertTrue(actualMessage.getBooleanProperty("IsWorkingStudent"));
    Assert.assertFalse(actualMessage.getBooleanProperty("HasWorkExperience"));
    Assert.assertEquals("Frontend developer", actualMessage.getStringProperty("Position"));
  }
}
