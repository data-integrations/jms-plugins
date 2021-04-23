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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;
import io.cdap.plugin.jms.source.utils.BytesMessageTestUtils;
import io.cdap.plugin.jms.source.utils.CommonTestUtils;
import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static org.mockito.Mockito.mock;

public class BytesMessageToRecordConverterTest {

  @Test
  public void bytesMessageToRecord_WithNoHeaderNoPropertiesNoSchema_Successfully() throws JMSException {
    BytesMessage bytesMessage = mock(BytesMessage.class);
    BytesMessageTestUtils.mockBytesMessage(bytesMessage);

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(false, false, JMSMessageType.BYTES, null);

    // expected
    Schema expectedSchema = Schema.recordOf(
      "record",
      Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING))
    );

    String expectedValue = BytesMessageTestUtils.getBytesMessageAsJsonString();
    StructuredRecord expectedRecord = StructuredRecord
      .builder(expectedSchema)
      .set(JMSMessageParts.BODY, expectedValue)
      .build();

    // actual
    StructuredRecord actualRecord = BytesMessageToRecordConverter.bytesMessageToRecord(bytesMessage, config);

    // asserts
    CommonTestUtils.assertEqualsStructuredRecords(expectedRecord, actualRecord);
  }

  @Test
  public void bytesMessageToRecord_WithNoHeaderNoPropertiesAndSchema_Successfully() throws JMSException {
    BytesMessage bytesMessage = mock(BytesMessage.class);
    BytesMessageTestUtils.mockBytesMessage(bytesMessage);

    // expected
    Schema.Field bodyField = Schema.Field.of(
      JMSMessageParts.BODY,
      Schema.recordOf(JMSMessageParts.BODY,
        Schema.Field.of("string_payload", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("int_payload", Schema.of(Schema.Type.INT))
      )
    );

    Schema outputSchema = Schema.recordOf("record", bodyField);

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(false, false, JMSMessageType.BYTES,
                                                                      outputSchema.toString());

    StructuredRecord expectedBodyRecord = StructuredRecord
      .builder(bodyField.getSchema())
      .set("string_payload", "Hello!")
      .set("int_payload", 1)
      .build();

    StructuredRecord expectedRecord = StructuredRecord
      .builder(outputSchema)
      .set(JMSMessageParts.BODY, expectedBodyRecord)
      .build();

    // actual
    StructuredRecord actualRecord = BytesMessageToRecordConverter.bytesMessageToRecord(bytesMessage, config);

    // asserts
    CommonTestUtils.assertEqualsStructuredRecords(expectedRecord, actualRecord);
  }
}
