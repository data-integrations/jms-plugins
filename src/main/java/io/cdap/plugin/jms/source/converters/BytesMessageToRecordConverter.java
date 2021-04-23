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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageProperties;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;

/**
 * A class with the functionality to convert BytesMessages to StructuredRecords.
 */
public class BytesMessageToRecordConverter {

  /**
   * Converts a {@link BytesMessage} to a {@link StructuredRecord}
   *
   * @param message the incoming JMS bytes message
   * @param config  the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the structured record built out of the JMS bytes message fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord bytesMessageToRecord(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    Schema schema = config.getSchema();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    if (config.getMessageHeader()) {
      JMSMessageHeader.populateHeader(config.getSchema(), recordBuilder, message);
    }
    if (config.getMessageProperties()) {
      JMSMessageProperties.populateProperties(schema, recordBuilder, message, JMSMessageType.MESSAGE);
    }

    Schema.Type bodyFieldType = schema.getField(JMSMessageParts.BODY).getSchema().getType();

    if (bodyFieldType.equals(Schema.Type.STRING)) {
      byteMessageToRecordForStringBody(message, recordBuilder);
    } else if (bodyFieldType.equals(Schema.Type.RECORD)) {
      byteMessageToRecordForRecordBody(message, schema, recordBuilder, config);
    }
    return recordBuilder.build();
  }

  /**
   * Converts a {@link BytesMessage} to a {@link StructuredRecord} when body is of data type string
   *
   * @param message the incoming JMS bytes message
   * @param builder the {@link StructuredRecord.Builder} to enrich with body
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  private static void byteMessageToRecordForStringBody(Message message, StructuredRecord.Builder builder)
    throws JMSException {
    Map<String, Object> body = new LinkedHashMap<>();

    // handle text data
    try {
      body.put("string_body", ((BytesMessage) message).readUTF());
    } catch (MessageEOFException e) { /* do nothing */ }

//    try {
//      body.put("char_body", ((BytesMessage) message).readChar());
//    } catch (MessageEOFException e) { /* do nothing */ }

    // handle numerical data
    try {
      body.put("double_body", ((BytesMessage) message).readDouble());
    } catch (MessageEOFException e) { /* do nothing */ }

    try {
      body.put("float_body", ((BytesMessage) message).readFloat());
    } catch (MessageEOFException e) { /* do nothing */ }

    try {
      body.put("int_body", ((BytesMessage) message).readInt());
    } catch (MessageEOFException e) { /* do nothing */ }

    try {
      body.put("long_body", ((BytesMessage) message).readLong());
    } catch (MessageEOFException e) { /* do nothing */ }

//    try {
//      body.put("short_body", ((BytesMessage) message).readShort());
//    } catch (MessageEOFException e) { /* do nothing */ }

//    try {
//      body.put("unsigned_short_body", ((BytesMessage) message).readUnsignedShort());
//    } catch (MessageEOFException e) { /* do nothing */ }

    // other
    try {
      body.put("boolean_body", ((BytesMessage) message).readBoolean());
    } catch (MessageEOFException e) { /* do nothing */ }

    try {
      body.put("byte_body", ((BytesMessage) message).readByte());
    } catch (MessageEOFException e) { /* do nothing */ }

//    try {
//      body.put("unsigned_byte_body", ((BytesMessage) message).readUnsignedByte());
//    } catch (MessageEOFException e) { /* do nothing */ }
//
//    try {
//      body.put("unsigned_byte_body", ((BytesMessage) message).readUnsignedByte());
//    } catch (MessageEOFException e) { /* do nothing */ }

    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byte[] buffer = new byte[8096];
      int currentByte;
      while ((currentByte = ((BytesMessage) message).readBytes(buffer)) != -1) {
        byteArrayOutputStream.write(buffer, 0, currentByte);
      }

      body.put("bytes_body", byteArrayOutputStream.toByteArray());
    } catch (MessageEOFException e) { /* do nothing */ }

    builder.set(JMSMessageParts.BODY, new Gson().toJson(body));
  }

  /**
   * Converts a {@link BytesMessage} to a {@link StructuredRecord} when body is of data type record
   *
   * @param message the incoming JMS bytes message
   * @param schema  the record schema
   * @param builder the {@link StructuredRecord.Builder} to enrich with body
   * @param config  the {@link JMSStreamingSourceConfig} with all user provided property values
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  private static void byteMessageToRecordForRecordBody(
    Message message, Schema schema, StructuredRecord.Builder builder, JMSStreamingSourceConfig config
  ) throws JMSException {
    Schema bodySchema = schema.getField(JMSMessageParts.BODY).getSchema();
    StructuredRecord.Builder bodyRecordBuilder = StructuredRecord.builder(bodySchema);

    for (Schema.Field field : bodySchema.getFields()) {
      Schema.Type type = field.getSchema().getType();
      String name = field.getName();

      if (type.equals(Schema.Type.UNION)) {
        type = field.getSchema().getUnionSchema(0).getType();
      }

      switch (type) {
        case BOOLEAN:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readBoolean());
          break;
        case INT:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readInt());
          break;
        case LONG:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readLong());
          break;
        case FLOAT:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readFloat());
          break;
        case DOUBLE:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readDouble());
          break;
        case BYTES:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readByte());
          break;
        case STRING:
          bodyRecordBuilder.set(name, ((BytesMessage) message).readUTF());
          break;
        case ARRAY: // byte array only
          Schema.Type itemType = field.getSchema().getComponentSchema().getType();
          if (itemType.equals(Schema.Type.UNION)) {
            itemType = field.getSchema().getComponentSchema().getUnionSchema(0).getType();
          }
          if (itemType.equals(Schema.Type.BYTES)) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[8096];
            int currentByte;
            while ((currentByte = ((BytesMessage) message).readBytes(buffer)) != -1) {
              byteArrayOutputStream.write(buffer, 0, currentByte);
            }
            bodyRecordBuilder.set(name, byteArrayOutputStream.toByteArray());
          }
          break;
      }
    }
    builder.set(JMSMessageParts.BODY, bodyRecordBuilder.build());
  }
}
