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
package org.apache.pulsar.broker.service.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * {@link SchemaCompatibilityCheck} for {@link SchemaType#JSON}.
 */
@SuppressWarnings("unused")
public class JsonSchemaCompatibilityCheck extends AvroSchemaBasedCompatibilityCheck {

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.JSON;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        if (isAvroSchema(from)) {
            if (isAvroSchema(to)) {
                // if both producer and broker have the schema in avro format
                return super.isCompatible(from, to, strategy);
            } else if (isJsonSchema(to)) {
                // if broker have the schema in avro format but producer sent a schema in the old json format
                // allow old schema format for backwards compatiblity
                return true;
            } else {
                // unknown schema format
                return false;
            }
        } else if (isJsonSchema(from)){

            if (isAvroSchema(to)) {
                // if broker have the schema in old json format but producer sent a schema in the avro format
                // return true and overwrite the old format
                return true;
            } else if (isJsonSchema(to)) {
                // if both producer and broker have the schema in old json format
                return isCompatibleJsonSchema(from, to);
            } else {
                // unknown schema format
                return false;
            }
        } else {
            // broker has schema format with unknown format
            // maybe corrupted?
            // return true to overwrite
            return true;
        }
    }

    private ObjectMapper objectMapper;
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    private boolean isCompatibleJsonSchema(SchemaData from, SchemaData to) {
        try {
            ObjectMapper objectMapper = getObjectMapper();
            JsonSchema fromSchema = objectMapper.readValue(from.getData(), JsonSchema.class);
            JsonSchema toSchema = objectMapper.readValue(to.getData(), JsonSchema.class);
            return fromSchema.getId().equals(toSchema.getId());
        } catch (IOException e) {
            return false;
        }
    }

    private boolean isAvroSchema(SchemaData schemaData) {
        try {

            Schema.Parser fromParser = new Schema.Parser();
            Schema fromSchema = fromParser.parse(new String(schemaData.getData(), UTF_8));
            return true;
        } catch (SchemaParseException e) {
            return false;
        }
    }

    private boolean isJsonSchema(SchemaData schemaData) {
        ObjectMapper objectMapper = getObjectMapper();
        try {
            JsonSchema fromSchema = objectMapper.readValue(schemaData.getData(), JsonSchema.class);
            return true;
        } catch (IOException e) {
           return false;
        }
    }

}
