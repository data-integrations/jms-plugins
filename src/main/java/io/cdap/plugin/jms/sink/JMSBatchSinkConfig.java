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

package io.cdap.plugin.jms.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSMessageType;

import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link io.cdap.plugin.jms.source.JMSSourceUtils;} and
 * {@link JMSBatchSink}.
 */
public class JMSBatchSinkConfig extends JMSConfig implements Serializable {

  // Params
  public static final String NAME_DESTINATION = "destinationName";
  public static final String NAME_MESSAGE_TYPE = "messageType";
  public static final String NAME_OUTPUT_SCHEMA = "schema";

  public static final String DESC_DESTINATION = "Name of the destination Queue/Topic. If the given Queue/Topic name" +
    "is not resolved, a new Queue/Topic with the given name will get created.";
  public static final String DESC_MESSAGE_TYPE = "Supports the following message types: Message, Text, Bytes, Map.";
  public static final String DESC_OUTPUT_SCHEMA = "Output schema.";

  @Name(NAME_DESTINATION)
  @Description(DESC_DESTINATION)
  @Macro
  private String destinationName;

  @Name(NAME_MESSAGE_TYPE)
  @Description(DESC_MESSAGE_TYPE)
  @Nullable
  @Macro
  private String messageType; // default: Text

  @Name(NAME_OUTPUT_SCHEMA)
  @Description(DESC_OUTPUT_SCHEMA)
  @Nullable
  @Macro
  private String schema;

  public JMSBatchSinkConfig() {
    super("");
    this.messageType = Strings.isNullOrEmpty(messageType) ? JMSMessageType.TEXT : messageType;
  }

  public JMSBatchSinkConfig(String referenceName, String connectionFactory, String jmsUsername,
                            String jmsPassword, String providerUrl, String type, String jndiContextFactory,
                            String jndiUsername, String jndiPassword, String messageType, String destinationName) {
    super(referenceName, connectionFactory, jmsUsername, jmsPassword, providerUrl, type, jndiContextFactory,
          jndiUsername, jndiPassword);
    this.destinationName = destinationName;
    this.messageType = messageType;
  }

  public String getDestinationName() {
    return destinationName;
  }

  public void validateParams(FailureCollector failureCollector) {
    validate(failureCollector);

    if (Strings.isNullOrEmpty(destinationName) && !containsMacro(NAME_DESTINATION)) {
      failureCollector
        .addFailure("The destination topic/queue name must be provided!", "Provide your topic/queue name.")
        .withConfigProperty(NAME_DESTINATION);
    }
  }

  public String getMessageType() {
    if (!Strings.isNullOrEmpty(NAME_MESSAGE_TYPE) && !containsMacro(NAME_MESSAGE_TYPE)) {
      return messageType;
    }
    return JMSMessageType.TEXT;
  }

  /**
   * @return {@link io.cdap.cdap.api.data.schema.Schema} of the dataset if one was given
   * @throws IllegalArgumentException if the schema is not a valid JSON
   */
  public Schema getSchema() {
    if (!Strings.isNullOrEmpty(schema)) {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Invalid schema : %s", e.getMessage()), e);
      }
    }
    return null;
  }
}
