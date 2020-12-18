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
import io.cdap.plugin.jms.common.JMSMessageType;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A facade class that provides the functionality to convert different type of JMS messages to structured records.
 */
public class SinkMessageConverterFacade {

  /**
   *
   * @param session
   * @param record
   * @param outputSchema
   * @param messageType
   * @return
   * @throws JMSException
   */
  public static Message toJmsMessage(Session session, StructuredRecord record, Schema outputSchema,
                                     String messageType) throws JMSException {
    if (outputSchema == null) {
      TextMessage textMessage = session.createTextMessage();
      return RecordToTextMessageConverter.toTextMessage(textMessage, record);
    } else {
      switch (messageType) {
        case JMSMessageType.MAP:
          MapMessage mapMessage = session.createMapMessage();
          return RecordToMapMessageConverter.toMapMessage(mapMessage, record);

        case JMSMessageType.BYTES:
          BytesMessage bytesMessage = session.createBytesMessage();
          return RecordToBytesMessageConverter.toBytesMessage(bytesMessage, record);

        case JMSMessageType.MESSAGE:
          Message message = session.createMessage();
          return RecordToMessageConverter.toMessage(message, record);

        case JMSMessageType.OBJECT:
          ObjectMessage objectMessage = session.createObjectMessage();
          return RecordToObjectMessageConverter.toObjectMessage(objectMessage, record);

        case JMSMessageType.TEXT:
        default:
          TextMessage textMessage = session.createTextMessage();
          return RecordToTextMessageConverter.toTextMessage(textMessage, record);
      }
    }
  }
}
