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
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;
import io.cdap.plugin.jms.source.utils.CommonTestUtils;
import io.cdap.plugin.jms.source.utils.HeaderTestUtils;
import io.cdap.plugin.jms.source.utils.PropertiesTestUtils;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.junit.Test;

import java.util.Objects;
import javax.jms.Message;

public class MessageToRecordConverterTest {

  @Test
  public void messageToRecord_WithMetadataAndPropertiesAndNoSchema_Successfully() throws Exception {
    Message message = new ActiveMQBytesMessage();
    HeaderTestUtils.addHeaderValuesToMessage(message);
    PropertiesTestUtils.addPropertiesToMessage(message);

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(true, true, JMSMessageType.MESSAGE, null);

    // expected
    Schema expectedSchema = Schema.recordOf(
      "record",
      Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)),
      JMSMessageHeader.getMessageHeaderField()
    );
    StructuredRecord.Builder expectedRecordBuilder = StructuredRecord.builder(expectedSchema);
    HeaderTestUtils.addHeaderValuesToRecord(expectedRecordBuilder, expectedSchema);
    PropertiesTestUtils.addPropertiesToRecord(expectedRecordBuilder, expectedSchema);
    StructuredRecord expectedRecord = expectedRecordBuilder.build();

    // actual
    StructuredRecord actualRecord = MessageToRecordConverter.messageToRecord(message, config);

    CommonTestUtils.assertEqualsStructuredRecords(
      Objects.requireNonNull(expectedRecord.get(JMSMessageParts.HEADER)),
      Objects.requireNonNull(actualRecord.get(JMSMessageParts.HEADER))
    );
  }

  @Test
  public void messageToRecord_WithMetadataAndPropertiesAndSchema_Successfully() throws Exception {
    Message message = new ActiveMQBytesMessage();
    HeaderTestUtils.addHeaderValuesToMessage(message);
    PropertiesTestUtils.addPropertiesToMessage(message);

    Schema outputSchema = Schema.recordOf(
      "record",
      PropertiesTestUtils.getPropertiesField(),
      JMSMessageHeader.getMessageHeaderField()
    );

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(true, true, JMSMessageType.MESSAGE,
                                                                      outputSchema.toString());

    // expected
    StructuredRecord.Builder expectedRecordBuilder = StructuredRecord.builder(outputSchema);
    HeaderTestUtils.addHeaderValuesToRecord(expectedRecordBuilder, outputSchema);
    PropertiesTestUtils.addPropertiesToRecord(expectedRecordBuilder, outputSchema);
    StructuredRecord expectedRecord = expectedRecordBuilder.build();

    // actual
    StructuredRecord actualRecord = MessageToRecordConverter.messageToRecord(message, config);

    CommonTestUtils.assertEqualsStructuredRecords(
      Objects.requireNonNull(expectedRecord.get(JMSMessageParts.HEADER)),
      Objects.requireNonNull(actualRecord.get(JMSMessageParts.HEADER))
    );
  }
}
