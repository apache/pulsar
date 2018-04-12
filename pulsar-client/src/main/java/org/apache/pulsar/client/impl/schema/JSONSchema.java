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
package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class JSONSchema<T> implements Schema<T> {

    private final SchemaInfo info;
    private final ObjectMapper objectMapper;
    private final Class<T> pojo;

    private JSONSchema(SchemaInfo info, Class<T> pojo, ObjectMapper objectMapper) {
        this.info = info;
        this.pojo = pojo;
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] encode(T message) throws SchemaSerializationException {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T decode(byte[] bytes) throws SchemaSerializationException {
        try {
            return objectMapper.readValue(new String(bytes), pojo);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return info;
    }

    public static <T> JSONSchema<T> of(Class<T> pojo) throws JsonProcessingException {
        return of(pojo, Collections.emptyMap());
    }

    public static <T> JSONSchema<T> of(Class<T> pojo, Map<String, String> properties) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonSchema schema = schemaGen.generateSchema(pojo);

        SchemaInfo info = new SchemaInfo();
        info.setName("");
        info.setProperties(properties);
        info.setType(SchemaType.JSON);
        info.setSchema(mapper.writeValueAsBytes(schema));
        return new JSONSchema<>(info, pojo, mapper);
    }
}
