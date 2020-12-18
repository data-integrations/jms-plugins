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

package io.cdap.plugin.jms.common;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that validates the schema.
 */
public class SchemaValidationUtils {

  @VisibleForTesting
  public static final String
    SCHEMA_IS_NULL_ERROR = "Schema is null!",
    SCHEMA_IS_NULL_ACTION = "Please provide the schema.",

    NOT_SUPPORTED_ROOT_FIELDS_ERROR = "Not supported root fields in the schema!",
    NOT_SUPPORTED_ROOT_FIELDS_ACTION = "Only \"header\", \"properties\" and \"body\" are supported as root fields.",

    BODY_NOT_IN_SCHEMA_ERROR = "The mandatory field \"body\" is missing in the schema!",
    BODY_NOT_IN_SCHEMA_ACTION = "Please provide \"body\" field",

    WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ERROR = "The field \"body\" has a not supported data type set!",
    WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ACTION = "When JMS \"Text\" message type is selected, the field \"body\" is" +
      " mandatory to be of String datatype",

    WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ERROR = "The field \"body\" has a not supported data type set!",
    WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ACTION = "When JMS \"Map\" message type is selected, the field \"body\" is" +
      " mandatory to be of String or Record data type.",

    WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ERROR = "The field \"body\" has a not supported data type set!",
    WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ACTION = "When JMS \"Bytes\" message type is selected, the field \"body\"" +
      " is mandatory to be of String or Record data type.",

    WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ERROR = "The field \"body\" has a not supported data type set!",
    WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ACTION = "When JMS \"Object\" message type is selected, the field " +
      "\"body\" is mandatory to be of Array of Bytes data type.",

    WRONG_BODY_DATA_TYPE_FOR_HEADER_ERROR = "The field \"header\" has a not supported data type set!",
    WRONG_BODY_DATA_TYPE_FOR_HEADER_ACTION = "The \"header\" field must be of Record data type.",

    NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ERROR = "Not supported fields set in the header record!",
    NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ACTION = "The field \"header\" support only fields: " +
      JMSMessageHeader.describe(),

    WRONG_PROPERTIES_DATA_TYPE_ERROR = "The field \"properties\" has a not supported data type set!",
    WRONG_PROPERTIES_DATA_TYPE_ACTION = "The field \"properties\" is mandatory to be of String or Record data type.",

    NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ERROR = "Not supported root fields in the schema!",
    NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ACTION = "JMS \"Message\" message type supports only \"header\" and " +
      "\"properties\" as root fields.",

    HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ERROR = "Fields \"Header\" and \"Properties\" are missing!",
    HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ACTION = "When JMS \"Message\" message type is selected, it is " +
      "mandatory that either \"Header\" or \"Properties\" root fields to be present in schema. " +
      "Set at least one of \"Keep Message Header\" or \"Keep Message Properties\" to true.";

  /**
   * Throws an error if schema is null.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateIfSchemaIsNull(Schema schema, FailureCollector collector) {
    if (schema == null) {
      tell(collector, SCHEMA_IS_NULL_ERROR, SCHEMA_IS_NULL_ACTION);
    }
  }

  /**
   * Throws an error if the input schema contains any other root fields except of "header", "properties", and "body".
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateIfAnyNotSupportedRootFieldExists(Schema schema, FailureCollector collector) {
    boolean areNonSupportedFieldsPresent = schema
      .getFields()
      .stream()
      .map(field -> field.getName())
      .anyMatch(f -> !JMSMessageParts.getJMSMessageParts().contains(f));

    if (areNonSupportedFieldsPresent) {
      tell(collector, NOT_SUPPORTED_ROOT_FIELDS_ERROR, NOT_SUPPORTED_ROOT_FIELDS_ACTION);
    }
  }

  /**
   * Throws an error if the input schema does not contain the root field "body". JMS "Message" type is the only message
   * type allowed to have the schema without the root field "body".
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateIfBodyNotInSchema(Schema schema, FailureCollector collector) {
    boolean noBodyInSchema = !schema
      .getFields()
      .stream()
      .map(field -> field.getName())
      .collect(Collectors.toList())
      .contains(JMSMessageParts.BODY);

    if (noBodyInSchema) {
      tell(collector, BODY_NOT_IN_SCHEMA_ERROR, BODY_NOT_IN_SCHEMA_ACTION);
    }
  }

  /**
   * Throws an error if the root field "body" is not of type "string" when JMS "TextMessage" is selected.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateTextMessageSchema(Schema schema, FailureCollector collector) {
    Schema.Type type = schema.getField(JMSMessageParts.BODY).getSchema().getType();
    boolean isTypeString = type.equals(Schema.Type.STRING);

    if (!isTypeString) {
      tell(collector, WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ERROR, WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ACTION);
    }
  }

  /**
   * Throws an error if the root field "body" is not of type "string" or "record" when JMS "MapMessage" is selected.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateMapMessageSchema(Schema schema, FailureCollector collector) {
    Schema.Type type = schema.getField(JMSMessageParts.BODY).getSchema().getType();
    boolean isTypeString = type.equals(Schema.Type.STRING);
    boolean isTypeRecord = type.equals(Schema.Type.RECORD);

    if (!isTypeString && !isTypeRecord) {
      tell(collector, WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ERROR, WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ACTION);
    }
  }

  /**
   * Throws an error if the input schema contains any other root fields except of "header", and "properties" when JMS
   * "Message" type is selected.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateMessageSchema(Schema schema, FailureCollector collector) {
    List<String> fieldNames = schema.getFields().stream().map(field -> field.getName()).collect(Collectors.toList());

    boolean areNonSupportedRootFieldsPresent = false;
    for (String fieldName: fieldNames) {
      if (Arrays.asList(JMSMessageParts.PROPERTIES, JMSMessageParts.HEADER).contains(fieldName)) {
        areNonSupportedRootFieldsPresent = true;
        break;
      }
    }

    if (areNonSupportedRootFieldsPresent) {
      tell(collector, NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ERROR, NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ACTION);
    }

    boolean areHeaderAndPropertiesMissing = !fieldNames.contains(JMSMessageParts.HEADER) &&
      !fieldNames.contains(JMSMessageParts.PROPERTIES);
    if (areHeaderAndPropertiesMissing) {
      tell(collector, HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ERROR, HEADER_AND_PROPERTIES_MISSING_IN_MESSAGE_ACTION);
    }
  }

  /**
   * Throws an error if the root field "body" is not of type "array of bytes" when JMS "ObjectMessage" is selected.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateObjectMessageSchema(Schema schema, FailureCollector collector) {
    boolean shouldThrowError = true;
    boolean isTypeArray = schema
      .getField(JMSMessageParts.BODY)
      .getSchema()
      .getType()
      .equals(Schema.Type.ARRAY);

    if (isTypeArray) {
      boolean isSubTypeByte = schema
        .getField(JMSMessageParts.BODY)
        .getSchema()
        .getComponentSchema()
        .getType()
        .equals(Schema.Type.BYTES);

      if (isSubTypeByte) {
        shouldThrowError = false;
      }
    }

    if (shouldThrowError) {
      tell(collector, WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ERROR, WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ACTION);
    }
  }

  /**
   * Throws an error if the root field  "body" is not of type "string" or "record" when JMS "BytesMessage" is selected.
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateBytesMessageSchema(Schema schema, FailureCollector collector) {
    Schema.Type type = schema.getField(JMSMessageParts.BODY).getSchema().getType();
    boolean isTypeString = type.equals(Schema.Type.STRING);
    boolean isTypeRecord = type.equals(Schema.Type.RECORD);

    if (!isTypeString && !isTypeRecord) {
      tell(collector, WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ERROR, WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ACTION);
    }
  }

  /**
   * Throws an error if the root field "header" is not of type "record". Throws an error also if the header record
   * contains other fields except of "messageId", "messageTimestamp", "correlationId", "replyTo", "destination",
   * "deliveryNode", "redelivered", "type", "expiration", and "priority".
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validateHeaderSchema(Schema schema, FailureCollector collector) {

    if (!isFieldPresent(schema, JMSMessageParts.HEADER)) {
      return;
    }

    boolean isTypeRecord = schema
      .getField(JMSMessageParts.HEADER)
      .getSchema()
      .getType()
      .equals(Schema.Type.RECORD);

    if (!isTypeRecord) {
      tell(collector, WRONG_BODY_DATA_TYPE_FOR_HEADER_ERROR, WRONG_BODY_DATA_TYPE_FOR_HEADER_ACTION);
    }

    boolean areNonSupportedHeaderFieldsPresent = schema
      .getField(JMSMessageParts.HEADER)
      .getSchema()
      .getFields()
      .stream()
      .map(field -> field.getName())
      .anyMatch(f -> !JMSMessageHeader.getJMSMessageHeaderNames().contains(f));

    if (areNonSupportedHeaderFieldsPresent) {
      tell(collector, NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ERROR, NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ACTION);
    }
  }

  /**
   * Throws an error if the root field "properties" is not of type "string" or "record".
   *
   * @param schema    the user defined schema
   * @param collector the failure collector
   */
  public static void validatePropertiesSchema(Schema schema, FailureCollector collector) {

    if (!isFieldPresent(schema, JMSMessageParts.PROPERTIES)) {
      return;
    }

    Schema.Type type = schema.getField(JMSMessageParts.PROPERTIES).getSchema().getType();
    boolean isTypeString = type.equals(Schema.Type.STRING);
    boolean isTypeRecord = type.equals(Schema.Type.RECORD);

    if (!isTypeString && !isTypeRecord) {
      tell(collector, WRONG_PROPERTIES_DATA_TYPE_ERROR, WRONG_PROPERTIES_DATA_TYPE_ACTION);
    }
  }

  /**
   * Throws an error and also add the error in the failure collector if one is provided.
   *
   * @param collector        the failure collector
   * @param errorMessage     the error message
   * @param correctiveAction the action that the user should perform to resolve the error
   */
  public static void tell(FailureCollector collector, String errorMessage, String correctiveAction) {
    String errorNature = "Error during schema validation";

    if (collector != null) {
      collector.addFailure(errorNature + ": " + errorMessage, correctiveAction)
        .withConfigProperty(JMSStreamingSourceConfig.NAME_SCHEMA);
    } else {
      throw new RuntimeException(concatenate(errorNature + ": " + errorMessage, correctiveAction));
    }
  }

  private static boolean isFieldPresent(Schema schema, String fieldName) {
    return schema.getField(fieldName) != null;
  }

  /**
   * Concatenates two strings with a space in between
   *
   * @param left  the left string
   * @param right the right string
   * @return the concatenated string
   */
  @VisibleForTesting
  public static String concatenate(String left, String right) {
    return String.format("%s %s", left, right);
  }
}
