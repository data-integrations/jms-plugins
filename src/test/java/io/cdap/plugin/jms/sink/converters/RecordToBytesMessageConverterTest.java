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
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static org.mockito.Mockito.when;

public class RecordToBytesMessageConverterTest {

  @Test
  public void convertRecordToBytesMessage_Successfully() throws JMSException {
    // actual
    BytesMessage bytesMessage = new ActiveMQBytesMessage();

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("FullName", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Height", Schema.of(Schema.Type.DOUBLE))
    );

    StructuredRecord record = StructuredRecord
      .builder(schema)
      .set("FullName", "Shaquille O'Neal")
      .set("Height", 2.17)
      .build();

    BytesMessage actualMessage = RecordToBytesMessageConverter.toBytesMessage(bytesMessage, record);
    BytesMessage mockedBytesMessage = Mockito.mock(actualMessage.getClass());
    when(mockedBytesMessage.readUTF()).thenReturn("Shaquille O'Neal");
    when(mockedBytesMessage.readDouble()).thenReturn(2.17);

        // assert
    Assert.assertEquals(mockedBytesMessage.readUTF(), record.get("FullName"));
    Assert.assertEquals((Double) mockedBytesMessage.readDouble(), record.get("Height"));
  }
}
