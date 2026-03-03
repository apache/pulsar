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
package org.apache.pulsar.broker.service.schema.validator;

import static java.nio.charset.StandardCharsets.UTF_8;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import java.io.IOException;
import org.apache.avro.NameValidator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Validate if the struct schema is in expected form.
 */
public class StructSchemaDataValidator implements SchemaDataValidator {

    public static StructSchemaDataValidator of() {
        return INSTANCE;
    }

    private static final StructSchemaDataValidator INSTANCE = new StructSchemaDataValidator();
    public static final NameValidator COMPATIBLE_NAME_VALIDATOR = new CompatibleNameValidator();

    private StructSchemaDataValidator() {}

    private static final ObjectReader JSON_SCHEMA_READER =
            ObjectMapperFactory.getMapper().reader().forType(JsonSchema.class);
    @Override
    public void validate(SchemaData schemaData) throws InvalidSchemaDataException {
        byte[] data = schemaData.getData();

        try {
            Schema.Parser avroSchemaParser = new Schema.Parser(COMPATIBLE_NAME_VALIDATOR);
            avroSchemaParser.setValidateDefaults(false);
            Schema schema = avroSchemaParser.parse(new String(data, UTF_8));
            if (SchemaType.AVRO.equals(schemaData.getType())) {
                checkAvroSchemaTypeSupported(schema);
            }
        } catch (SchemaParseException e) {
            if (schemaData.getType() == SchemaType.JSON) {
                // we used JsonSchema for storing the definition of a JSON schema
                // hence for backward compatibility consideration, we need to try
                // to use JsonSchema to decode the schema data
                try {
                    JSON_SCHEMA_READER.readValue(data);
                } catch (IOException ioe) {
                    throwInvalidSchemaDataException(schemaData, ioe);
                }
            } else {
                throwInvalidSchemaDataException(schemaData, e);
            }
        } catch (InvalidSchemaDataException invalidSchemaDataException) {
            throw invalidSchemaDataException;
        } catch (Exception e) {
            throwInvalidSchemaDataException(schemaData, e);
        }
    }

    static void checkAvroSchemaTypeSupported(Schema schema) throws InvalidSchemaDataException {
            switch (schema.getType()) {
                case RECORD: {
                    break;
                }
                case UNION: {
                    throw new InvalidSchemaDataException(
                            "Avro schema typed [UNION] is not supported");
                }
                default: {
                    // INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BYTES.
                    // ARRAY, MAP, FIXED, NULL.
                    LOGGER.info("Registering a special avro schema typed [{}]", schema.getType());
                }
            }
    }

    private static void throwInvalidSchemaDataException(SchemaData schemaData,
                                                        Throwable cause) throws InvalidSchemaDataException {
        throw new InvalidSchemaDataException("Invalid schema definition data for "
            + schemaData.getType() + " schema", cause);
    }

    static class CompatibleNameValidator implements NameValidator {

        @Override
        public Result validate(String name) {
            if (name == null) {
                return new Result("Null name");
            }
            final int length = name.length();
            if (length == 0) {
                return new Result("Empty name");
            }
            final char first = name.charAt(0);
            if (!(Character.isLetter(first) || first == '_' || first == '$')) {
                return new Result("Illegal initial character: " + name);
            }
            for (int i = 1; i < length; i++) {
                final char c = name.charAt(i);
                // we need to allow $ for the special case
                if (!(Character.isLetterOrDigit(c) || c == '_' || c == '$')) {
                    return new Result("Illegal character in: " + name);
                }
            }
            return OK;
        }
    }
}
