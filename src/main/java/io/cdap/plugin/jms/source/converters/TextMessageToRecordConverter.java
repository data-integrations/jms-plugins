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
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageProperties;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * A class with the functionality to convert TextMessages to StructuredRecords.
 */
public class TextMessageToRecordConverter {

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link TextMessage}
   *
   * @param message the incoming JMS {@link TextMessage}
   * @param config  the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link TextMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord textMessageToRecord(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSchema());

    if (config.getMessageHeader()) {
      JMSMessageHeader.populateHeader(config.getSchema(), recordBuilder, message);
    }
    if (config.getMessageProperties()) {
      JMSMessageProperties.populateProperties(config.getSchema(), recordBuilder, message, config.getMessageType());
    }

    recordBuilder.set(JMSMessageParts.BODY, ((TextMessage) message).getText());
    return recordBuilder.build();
  }
}
