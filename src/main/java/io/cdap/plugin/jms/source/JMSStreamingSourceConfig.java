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

package io.cdap.plugin.jms.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSMessageHeader;
import io.cdap.plugin.jms.common.JMSMessageParts;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.common.SchemaValidationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configs for {@link JMSStreamingSource}.
 */
public class JMSStreamingSourceConfig extends JMSConfig implements Serializable {
  public static final String NAME_SOURCE = "sourceName";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_MESSAGE_HEADER = "messageHeader";
  public static final String NAME_MESSAGE_PROPERTIES = "messageProperties";
  public static final String NAME_MESSAGE_TYPE = "messageType";


  @Name(NAME_SOURCE)
  @Description("Name of the source Queue/Topic. The Queue/Topic with the given name, should exist in order to read " +
    "messages from.")
  @Macro
  private String sourceName;

  @Name(NAME_MESSAGE_HEADER)
  @Description("If true, message header is also consumed. Otherwise, it is not.")
  @Nullable
  @Macro
  private String messageHeader;

  @Name(NAME_MESSAGE_PROPERTIES)
  @Description("If true, message properties are also consumed. Otherwise, they are not.")
  @Nullable
  @Macro
  private String messageProperties;

  @Name(NAME_MESSAGE_TYPE)
  @Description("Supports the following message types: Message, Text, Bytes, Map, Object.")
  @Nullable
  @Macro
  private String messageType; // default: Text

  @Name(NAME_SCHEMA)
  @Description("Specifies the schema of the records outputted from this plugin.")
  @Macro
  private String schema;

  public JMSStreamingSourceConfig() {
    super("");
    this.messageHeader = Strings.isNullOrEmpty(messageHeader) ? "true" : messageHeader;
    this.messageType = Strings.isNullOrEmpty(messageType) ? JMSMessageType.TEXT : messageType;
  }

  @VisibleForTesting
  public JMSStreamingSourceConfig(String referenceName, String connectionFactory, String jmsUsername,
                                  String jmsPassword, String providerUrl, String type, String jndiContextFactory,
                                  String jndiUsername, String jndiPassword, String messageHeader,
                                  String messageProperties, String messageType, String sourceName, String schema) {
    super(referenceName, connectionFactory, jmsUsername, jmsPassword, providerUrl, type, jndiContextFactory,
          jndiUsername, jndiPassword);
    this.sourceName = sourceName;
    this.messageHeader = messageHeader;
    this.messageProperties = messageProperties;
    this.messageType = messageType;
    this.schema = schema;
  }

  public void validate(FailureCollector failureCollector) {
    this.validateParams(failureCollector);

    if (Strings.isNullOrEmpty(messageType) && !containsMacro(NAME_MESSAGE_TYPE)) {
      failureCollector
        .addFailure("The source topic/queue name must be provided!", "Provide your topic/queue name.")
        .withConfigProperty(NAME_MESSAGE_TYPE);
    }

    if (Strings.isNullOrEmpty(sourceName) && !containsMacro(NAME_SOURCE)) {
      failureCollector
        .addFailure("The source topic/queue name must be provided!", "Provide your topic/queue name.")
        .withConfigProperty(NAME_SOURCE);
    }

    if (!containsMacro(NAME_SCHEMA)) {
      Schema schema = getSchema();

      SchemaValidationUtils.validateIfAnyNotSupportedRootFieldExists(schema, failureCollector);

      if (getMessageHeader()) {
        SchemaValidationUtils.validateHeaderSchema(schema, failureCollector);
      }

      if (getMessageProperties()) {
        SchemaValidationUtils.validatePropertiesSchema(schema, failureCollector);
      }

      switch (messageType) {
        case JMSMessageType.TEXT:
          SchemaValidationUtils.validateIfBodyNotInSchema(schema, failureCollector);
          SchemaValidationUtils.validateTextMessageSchema(schema, failureCollector);
          break;

        case JMSMessageType.OBJECT:
          SchemaValidationUtils.validateIfBodyNotInSchema(schema, failureCollector);
          SchemaValidationUtils.validateObjectMessageSchema(schema, failureCollector);
          break;

        case JMSMessageType.BYTES:
          SchemaValidationUtils.validateIfBodyNotInSchema(schema, failureCollector);
          SchemaValidationUtils.validateBytesMessageSchema(schema, failureCollector);
          break;

        case JMSMessageType.MAP:
          SchemaValidationUtils.validateIfBodyNotInSchema(schema, failureCollector);
          SchemaValidationUtils.validateMapMessageSchema(schema, failureCollector);
          break;

        case JMSMessageType.MESSAGE:
          SchemaValidationUtils.validateMessageSchema(schema, failureCollector);
      }
    }
  }

  public String getSourceName() {
    return sourceName;
  }

  /**
   * @return {@link io.cdap.cdap.api.data.schema.Schema} of the dataset if one was given
   * @throws IllegalArgumentException if the schema is not a valid JSON
   */
  public Schema getSchema() {

    if (!Strings.isNullOrEmpty(schema) && !containsMacro(schema)) {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Invalid schema : %s.", e.getMessage()), e);
      }
    }

    List<Schema.Field> fields = new ArrayList<>();

    if (getMessageHeader()) {
      fields.add(JMSMessageHeader.getMessageHeaderField());
    }

    if (getMessageProperties()) {
      fields.add(Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)));
    }

    switch (messageType) {
      case JMSMessageType.OBJECT:
        fields.add(Schema.Field.of(JMSMessageParts.BODY, Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
        return Schema.recordOf("record", fields);

      case JMSMessageType.MESSAGE:
        if (!getMessageProperties() && !getMessageHeader()) {
          SchemaValidationUtils.tell(null, SchemaValidationUtils.HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ERROR,
                                     SchemaValidationUtils.HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ACTION);
        }
        return Schema.recordOf("record", fields);

      case JMSMessageType.MAP:
      case JMSMessageType.TEXT:
      case JMSMessageType.BYTES:
        fields.add(Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING)));
        return Schema.recordOf("record", fields);
      default:
        return Schema.recordOf("record", fields);
    }
  }

  @Nullable
  public String getMessageType() {
    return messageType;
  }

  public boolean getMessageHeader() {
    return this.messageHeader.equalsIgnoreCase("true");
  }

  public boolean getMessageProperties() {
    return this.messageProperties.equalsIgnoreCase("true");
  }

  public List<Schema.Field> getDataFields(Schema schema, String skipFieldName) {
    return schema
      .getFields()
      .stream()
      .filter(field -> !JMSMessageHeader.getJMSMessageHeaderNames().contains(field.getName()))
      .filter(field -> !field.getName().equals(skipFieldName))
      .collect(Collectors.toList());
  }
}
