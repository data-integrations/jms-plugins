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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.cdap.plugin.jms.common.SchemaValidationUtils.concatenate;

/**
 * Unit tests for schema validation.
 */
public class SchemaValidationUtilsTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void validateIfSchemaIsNull_WithNullSchema_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.SCHEMA_IS_NULL_ERROR,
        SchemaValidationUtils.SCHEMA_IS_NULL_ACTION
      )
    );

    Schema schema = null;
    SchemaValidationUtils.validateIfSchemaIsNull(schema, null);
  }

  @Test
  public void validateIfSchemaIsNull_WithNotNullSchema_ShouldSucceed() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("test", Schema.of(Schema.Type.STRING)));
    SchemaValidationUtils.validateIfSchemaIsNull(schema, null);
  }

  @Test
  public void validateIfAnyNotSupportedRootFieldExists_WithNotSupportedFields_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.NOT_SUPPORTED_ROOT_FIELDS_ERROR,
        SchemaValidationUtils.NOT_SUPPORTED_ROOT_FIELDS_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record",
                Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING)),
                Schema.Field.of("other_field", Schema.of(Schema.Type.STRING))
      );

    SchemaValidationUtils.validateIfAnyNotSupportedRootFieldExists(schema, null);
  }

  @Test
  public void validateIfAnyNotSupportedRootFieldExists_WithOnlySupportedFields_ShouldSucceed() {
    Schema schema = Schema
      .recordOf("record",
                Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING))
      );

    SchemaValidationUtils.validateIfAnyNotSupportedRootFieldExists(schema, null);
  }

  @Test
  public void validateIfBodyNotInSchema_WithNoBody_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.BODY_NOT_IN_SCHEMA_ERROR,
        SchemaValidationUtils.BODY_NOT_IN_SCHEMA_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING)));

    SchemaValidationUtils.validateIfBodyNotInSchema(schema, null);
  }

  @Test
  public void validateIfBodyNotInSchema_WithBody_ShouldSucceed() {
    Schema schema = Schema
      .recordOf("record",
                Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING))
      );
    SchemaValidationUtils.validateIfBodyNotInSchema(schema, null);
  }

  @Test
  public void validateTextMessageSchema_WithNonStringBody_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ERROR,
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_TEXT_MESSAGE_ACTION
      )
    );
    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.INT)));

    SchemaValidationUtils.validateTextMessageSchema(schema, null);
  }

  @Test
  public void validateMapMessageSchema_WithNonStringOrRecordBodyRootField_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ERROR,
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_MAP_MESSAGE_ACTION
      )
    );
    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.INT)));

    SchemaValidationUtils.validateMapMessageSchema(schema, null);
  }

  @Test
  public void validateMessageSchema_WithNotSupportedRootFields_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ERROR,
        SchemaValidationUtils.NOT_SUPPORTED_ROOT_FIELDS_IN_MESSAGE_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record",
                Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.STRING)),
                Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.STRING)),
                Schema.Field.of("other-field", Schema.of(Schema.Type.STRING))
      );

    SchemaValidationUtils.validateMessageSchema(schema, null);
  }

  @Test
  public void validateByteMessageSchema_WithNonStringOrRecordBodyRootField_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ERROR,
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_BYTES_MESSAGE_ACTION
      )
    );
    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.of(Schema.Type.INT)));

    SchemaValidationUtils.validateBytesMessageSchema(schema, null);
  }

  @Test
  public void validateObjectMessageSchema_WithBodyNotArrayOfBytes_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ERROR,
        SchemaValidationUtils.WRONG_BODY_DATA_TYPE_IN_OBJECT_MESSAGE_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.arrayOf(Schema.of(Schema.Type.STRING)))
      );

    SchemaValidationUtils.validateObjectMessageSchema(schema, null);
  }

  @Test
  public void validateObjectMessageSchema_WithBodyArrayOfBytes_ShouldSucceed() {
    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.BODY, Schema.arrayOf(Schema.of(Schema.Type.BYTES)))
      );

    SchemaValidationUtils.validateObjectMessageSchema(schema, null);
  }

  @Test
  public void validateHeaderSchema_WithHeaderNotRecord_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
       SchemaValidationUtils.WRONG_BODY_DATA_TYPE_FOR_HEADER_ERROR,
       SchemaValidationUtils.WRONG_BODY_DATA_TYPE_FOR_HEADER_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.HEADER, Schema.of(Schema.Type.STRING))
      );

    SchemaValidationUtils.validateHeaderSchema(schema, null);
  }

  @Test
  public void validateHeaderSchema_WithNonSupportedHeaderFields_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ERROR,
        SchemaValidationUtils.NOT_SUPPORTED_FIELDS_IN_HEADER_RECORD_ACTION
      )
    );

    Schema schema = Schema
      .recordOf("record", Schema.Field.of(JMSMessageParts.HEADER, Schema.recordOf(
        "record",
        Schema.Field.of(JMSMessageHeader.MESSAGE_ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of("other_field", Schema.of(Schema.Type.STRING))
        ))
      );

    SchemaValidationUtils.validateHeaderSchema(schema, null);
  }

  @Test
  public void validateProperties_WithNonStringOrRecordPropertiesRootField_ShouldThrowError() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
      concatenate(
        SchemaValidationUtils.WRONG_PROPERTIES_DATA_TYPE_ERROR,
        SchemaValidationUtils.WRONG_PROPERTIES_DATA_TYPE_ACTION)
    );

    Schema schema = Schema
      .recordOf("record",
                Schema.Field.of(JMSMessageParts.PROPERTIES, Schema.of(Schema.Type.INT))
      );

    SchemaValidationUtils.validatePropertiesSchema(schema, null);
  }
}


