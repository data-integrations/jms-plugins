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
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;

/**
 * A class with the functionality to convert StructuredRecords to ObjectMessages.
 */
public class RecordToObjectMessageConverter {

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link ObjectMessage}
   *
   * @param objectMessage the incoming record
   * @param record the incoming record
   * @return a JMS object message
   */
  public static ObjectMessage toObjectMessage(ObjectMessage objectMessage, StructuredRecord record) {
    byte[] body = null;

    try {
      body = StructuredRecordStringConverter.toJsonString(record).getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert record to json!", e);
    }

    try {
      objectMessage.setObject(body);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return objectMessage;
  }
}
