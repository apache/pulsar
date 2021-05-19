/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.schema.validator;

import org.apache.pulsar.broker.service.schema.KeyValueSchemaCompatibilityCheck;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.KeyValue;

/**
 * A validator to validate the schema data is well formed.
 */
public interface SchemaDataValidator {

    /**
     * Validate if the schema data is well formed.
     *
     * @param schemaData schema data to validate
     * @throws InvalidSchemaDataException if the schema data is not in a valid form.
     */
    static void validateSchemaData(SchemaData schemaData) throws InvalidSchemaDataException {
        switch (schemaData.getType()) {
            case AVRO:
            case JSON:
            case PROTOBUF:
                StructSchemaDataValidator.of().validate(schemaData);
                break;
            case PROTOBUF_NATIVE:
                ProtobufNativeSchemaDataValidator.of().validate(schemaData);
                break;
            case STRING:
                StringSchemaDataValidator.of().validate(schemaData);
                break;
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case INSTANT:
            case LOCAL_DATE:
            case LOCAL_TIME:
            case LOCAL_DATE_TIME:
                PrimitiveSchemaDataValidator.of().validate(schemaData);
                break;
            case NONE:
            case BYTES:
                // `NONE` and `BYTES` schema is not stored
                break;
            case AUTO:
            case AUTO_CONSUME:
            case AUTO_PUBLISH:
                throw new InvalidSchemaDataException(
                    "Schema " + schemaData.getType() + " is a client-side schema type");
            case KEY_VALUE:
                KeyValue<SchemaData, SchemaData> kvSchema =
                    KeyValueSchemaCompatibilityCheck.decodeKeyValueSchemaData(schemaData);
                validateSchemaData(kvSchema.getKey());
                validateSchemaData(kvSchema.getValue());
                break;
            default:
                throw new InvalidSchemaDataException("Unknown schema type : " + schemaData.getType());
        }
    }

    /**
     * Validate a schema data is in a valid form.
     *
     * @param schemaData schema data to validate
     * @throws InvalidSchemaDataException if the schema data is not in a valid form.
     */
    void validate(SchemaData schemaData) throws InvalidSchemaDataException;

}
