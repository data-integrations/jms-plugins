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
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

public class HeaderTestUtils {

  private static final String VAL_MESSAGE_ID = "JMSMessageID";
  private static final Long VAL_MESSAGE_TIMESTAMP =  1619096400L;
  private static final String VAL_CORRELATION_ID = "JMSCorrelationID";
  private static final Destination VAL_REPLY_TO = null;
  private static final Destination VAL_DESTINATION = VAL_REPLY_TO;
  private static final String VAL_TYPE = "JMSType";
  private static final Integer VAL_DELIVERY_MODE = 1;
  private static final Boolean VAL_REDELIVERED = false;
  private static final Long VAL_EXPIRATION = VAL_MESSAGE_TIMESTAMP;
  private static final Integer VAL_PRIORITY = 0;

  public static void addHeaderValuesToRecord(StructuredRecord.Builder builder, Schema schema) {
    StructuredRecord.Builder headerBuilder = StructuredRecord
      .builder(schema.getField(JMSMessageParts.HEADER).getSchema());

    headerBuilder.set(JMSMessageHeader.MESSAGE_ID, "ID:" + VAL_MESSAGE_ID);
    headerBuilder.set(JMSMessageHeader.MESSAGE_TIMESTAMP, VAL_MESSAGE_TIMESTAMP);
    headerBuilder.set(JMSMessageHeader.CORRELATION_ID, VAL_CORRELATION_ID);
    headerBuilder.set(JMSMessageHeader.REPLY_TO, VAL_REPLY_TO);
    headerBuilder.set(JMSMessageHeader.DESTINATION, VAL_DESTINATION);
    headerBuilder.set(JMSMessageHeader.TYPE, VAL_TYPE);
    headerBuilder.set(JMSMessageHeader.DELIVERY_MODE, VAL_DELIVERY_MODE);
    headerBuilder.set(JMSMessageHeader.REDELIVERED, VAL_REDELIVERED);
    headerBuilder.set(JMSMessageHeader.EXPIRATION, VAL_EXPIRATION);
    headerBuilder.set(JMSMessageHeader.PRIORITY, VAL_PRIORITY);

    builder.set(JMSMessageParts.HEADER, headerBuilder.build());
  }

  public static void addHeaderValuesToMessage(Message message) throws JMSException {
    message.setJMSMessageID(VAL_MESSAGE_ID);
    message.setJMSTimestamp(VAL_MESSAGE_TIMESTAMP);
    message.setJMSCorrelationID(VAL_CORRELATION_ID);
    message.setJMSReplyTo(VAL_REPLY_TO);
    message.setJMSDestination(VAL_DESTINATION);
    message.setJMSType(VAL_TYPE);
    message.setJMSDeliveryMode(VAL_DELIVERY_MODE);
    message.setJMSRedelivered(VAL_REDELIVERED);
    message.setJMSExpiration(VAL_EXPIRATION);
    message.setJMSPriority(VAL_PRIORITY);
  }
}
