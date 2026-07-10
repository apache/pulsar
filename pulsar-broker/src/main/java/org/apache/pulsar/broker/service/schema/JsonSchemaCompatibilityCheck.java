/*
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
package org.apache.pulsar.broker.service.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.validator.StructSchemaDataValidator;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * {@link SchemaCompatibilityCheck} for {@link SchemaType#JSON}.
 */
@SuppressWarnings("unused")
public class JsonSchemaCompatibilityCheck extends AvroSchemaBasedCompatibilityCheck {

    private volatile boolean allowLegacyJacksonFormat = false;

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.JSON;
    }

    /**
     * Set whether to allow legacy Jackson JsonSchema format for backward compatibility.
     * When false (default), only valid Avro schema format is accepted (PIP-464).
     */
    public void setAllowLegacyJacksonFormat(boolean allowLegacyJacksonFormat) {
        this.allowLegacyJacksonFormat = allowLegacyJacksonFormat;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        if (isAvroSchema(from)) {
            if (isAvroSchema(to)) {
                // if both producer and broker have the schema in avro format
                super.checkCompatible(from, to, strategy);
            } else if (allowLegacyJacksonFormat && isJsonSchema(to)) {
                // if broker have the schema in avro format but producer sent a schema in the old json format
                // allow old schema format for backwards compatibility (only when legacy format is enabled)
            } else {
                throw new IncompatibleSchemaException(
                        "Incompatible schema: expected Avro schema format for SchemaType.JSON");
            }
        } else if (allowLegacyJacksonFormat && isJsonSchema(from)) {

            if (isAvroSchema(to)) {
                // if broker have the schema in old json format but producer sent a schema in the avro format
                // return true and overwrite the old format
            } else if (isJsonSchema(to)) {
                // if both producer and broker have the schema in old json format
                isCompatibleJsonSchema(from, to);
            } else {
                throw new IncompatibleSchemaException(
                        "Incompatible schema: expected Avro schema format for SchemaType.JSON");
            }
        } else if (!allowLegacyJacksonFormat && !isAvroSchema(from)) {
            // When legacy format is disabled, the existing schema must be valid Avro.
            // If it's not, this is a defense-in-depth rejection (PIP-464).
            throw new IncompatibleSchemaException(
                    "Incompatible schema: existing schema is not in valid Avro format for SchemaType.JSON");
        } else {
            // broker has schema format with unknown format
            // maybe corrupted?
            // return true to overwrite
        }
    }

    private static final ObjectReader JSON_SCHEMA_READER =
            ObjectMapperFactory.getMapper().reader().forType(JsonSchema.class);
    private void isCompatibleJsonSchema(SchemaData from, SchemaData to) throws IncompatibleSchemaException {
        try {
            JsonSchema fromSchema = JSON_SCHEMA_READER.readValue(from.getData());
            JsonSchema toSchema = JSON_SCHEMA_READER.readValue(to.getData());
            if (!fromSchema.getId().equals(toSchema.getId())) {
                throw new IncompatibleSchemaException(String.format("Incompatible Schema from %s + to %s",
                        new String(from.getData(), UTF_8), new String(to.getData(), UTF_8)));
            }
        } catch (IOException e) {
            throw new IncompatibleSchemaException(e);
        }
    }

    private boolean isAvroSchema(SchemaData schemaData) {
        try {

            Schema.Parser fromParser = new Schema.Parser(StructSchemaDataValidator.COMPATIBLE_NAME_VALIDATOR);
            fromParser.setValidateDefaults(false);
            Schema fromSchema = fromParser.parse(new String(schemaData.getData(), UTF_8));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isJsonSchema(SchemaData schemaData) {
        try {
            JsonSchema fromSchema = JSON_SCHEMA_READER.readValue(schemaData.getData());
            return true;
        } catch (IOException e) {
           return false;
        }
    }

}
