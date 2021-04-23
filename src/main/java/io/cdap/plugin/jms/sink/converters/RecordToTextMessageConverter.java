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
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.io.IOException;
import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * A class with the functionality to convert StructuredRecords to TextMessages.
 */
public class RecordToTextMessageConverter {

  /**
   * Converts an incoming {@link StructuredRecord} to a JMS {@link TextMessage}
   *
   * @param textMessage the jms message to be populated with data
   * @param record      the incoming record
   * @return a JMS text message
   */
  public static TextMessage toTextMessage(TextMessage textMessage, StructuredRecord record) {
    int numFields = record.getSchema().getFields().size();

    if (numFields == 1) {
      return withSingleField(textMessage, record);
    }
    return withMultipleFields(textMessage, record);
  }

  /**
   * Converts an incoming {@link StructuredRecord} with a single field to a JMS {@link TextMessage} where the text
   * isn't wrapped in a json object.
   *
   * @param textMessage the jms message to be populated with data
   * @param record      the incoming record
   * @return a JMS text message
   */
  private static TextMessage withSingleField(TextMessage textMessage, StructuredRecord record) {
    Schema.Field singleField = record.getSchema().getFields().get(0);
    String body = record.get(singleField.getName()).toString();
    try {
      textMessage.setText(body);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return textMessage;
  }

  /**
   * Converts an incoming {@link StructuredRecord} with multiple fields to a JMS {@link TextMessage} where the text
   * is wrapped in a json object.
   *
   * @param textMessage the jms message to be populated with data
   * @param record      the incoming record
   * @return a JMS text message
   */
  private static TextMessage withMultipleFields(TextMessage textMessage, StructuredRecord record) {
    String body;

    try {
      body = StructuredRecordStringConverter.toJsonString(record);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert record to json!", e);
    }
    try {
      textMessage.setText(body);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return textMessage;
  }
}
