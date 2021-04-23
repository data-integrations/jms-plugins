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
import io.cdap.plugin.jms.source.utils.CommonTestUtils;
import io.cdap.plugin.jms.source.utils.DummyObject;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

public class ObjectMessageToRecordConverterTest {

  @Test
  public void objectMessageToRecord_WithNoHeaderNoPropertiesNoSchema_Successfully() throws JMSException {
    DummyObject dummyObject = new DummyObject("Boeing", 777);
    ObjectMessage objectMessage = new ActiveMQObjectMessage();
    objectMessage.setObject(dummyObject);

    JMSStreamingSourceConfig config = CommonTestUtils.getSourceConfig(false, false, JMSMessageType.OBJECT, null);

    // expected
    Schema expectedSchema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
    StructuredRecord expectedRecord = StructuredRecord
      .builder(expectedSchema)
      .set(JMSMessageParts.BODY, SerializationUtils.serialize(dummyObject)).build();

    // actual
    StructuredRecord actualRecord = ObjectMessageToRecordConverter.objectMessageToRecord(objectMessage, config);

    // asserts
    CommonTestUtils.assertEqualsStructuredRecords(expectedRecord, actualRecord);
  }
}
