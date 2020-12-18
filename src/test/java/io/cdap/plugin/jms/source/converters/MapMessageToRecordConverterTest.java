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
import io.cdap.plugin.jms.source.utils.MapMessageTestUtils;
import io.cdap.plugin.jms.source.utils.PropertiesTestUtils;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.MapMessage;

public class MapMessageToRecordConverterTest {


  @Test
  public void mapMessageToRecord_WithHeaderAndPropertiesAndNoSchema_Successfully() throws JMSException {
    MapMessage mapMessage = new ActiveMQMapMessage();
    MapMessageTestUtils.addBodyValuesToMessage(mapMessage);
    HeaderTestUtils.addHeaderValuesToMessage(mapMessage);
    PropertiesTestUtils.addPropertiesToMessage(mapMessage);

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(true, true, JMSMessageType.MAP, null);

    // expected
    Schema expectedSchema = Schema.recordOf("record", JMSMessageHeader.getMessageHeaderField(),
                                            Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING)));
    StructuredRecord.Builder expectedRecordBuilder = StructuredRecord.builder(expectedSchema);
    HeaderTestUtils.addHeaderValuesToRecord(expectedRecordBuilder, expectedSchema);
    PropertiesTestUtils.addPropertiesToRecord(expectedRecordBuilder, expectedSchema);
    String expectedBody = MapMessageTestUtils.geBodyValuesAsJson();
    expectedRecordBuilder.set(JMSMessageParts.BODY, expectedBody);
    StructuredRecord expectedRecord = expectedRecordBuilder.build();

    // actual
    StructuredRecord actualRecord = MapMessageToRecordConverter.mapMessageToRecord(mapMessage, config);

    // assert
    CommonTestUtils.assertEqualsStructuredRecords(expectedRecord, actualRecord);
  }

  @Test
  public void mapMessageToRecord_WithHeaderAndPropertiesAndSchema_Successfully() throws JMSException {
    MapMessage mapMessage = new ActiveMQMapMessage();
    MapMessageTestUtils.addBodyValuesToMessage(mapMessage);
    HeaderTestUtils.addHeaderValuesToMessage(mapMessage);
    PropertiesTestUtils.addPropertiesToMessage(mapMessage);

    Schema outputSchema = Schema.recordOf(
      "record",
      JMSMessageHeader.getMessageHeaderField(),
      PropertiesTestUtils.getPropertiesField(),
      MapMessageTestUtils.getBodyFields()
    );

    JMSStreamingSourceConfig config = CommonTestUtils
      .getSourceConfig(true, true, JMSMessageType.MAP, outputSchema.toString());

    // expected
    StructuredRecord.Builder expectedRecordBuilder = StructuredRecord.builder(outputSchema);
    HeaderTestUtils.addHeaderValuesToRecord(expectedRecordBuilder, outputSchema);
    PropertiesTestUtils.addPropertiesToRecord(expectedRecordBuilder, outputSchema);
    MapMessageTestUtils.addBodyValuesToRecord(expectedRecordBuilder, outputSchema);
    StructuredRecord expectedRecord = expectedRecordBuilder.build();

    // actual
    StructuredRecord actualRecord = MapMessageToRecordConverter.mapMessageToRecord(mapMessage, config);

    // assert
    CommonTestUtils.assertEqualsStructuredRecords(actualRecord, expectedRecord);
  }
}
