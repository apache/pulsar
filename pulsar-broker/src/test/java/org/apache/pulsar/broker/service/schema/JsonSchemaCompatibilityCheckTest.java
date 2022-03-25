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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import lombok.Data;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class JsonSchemaCompatibilityCheckTest extends BaseAvroSchemaCompatibilityTest{

    @Override
    public SchemaCompatibilityCheck getSchemaCheck() {
        return new JsonSchemaCompatibilityCheck();
    }

    @Test
    public void testJsonSchemaBackwardsCompatibility() throws JsonProcessingException {

        SchemaData from = SchemaData.builder().data(OldJSONSchema.of(Foo.class).getSchemaInfo().getSchema()).build();
        SchemaData to = SchemaData.builder().data(JSONSchema.of(SchemaDefinition.builder().withPojo(Foo.class).build()).getSchemaInfo().getSchema()).build();
        JsonSchemaCompatibilityCheck jsonSchemaCompatibilityCheck = new JsonSchemaCompatibilityCheck();
        Assert.assertTrue(jsonSchemaCompatibilityCheck.isCompatible(from, to, SchemaCompatibilityStrategy.FULL));

        from = SchemaData.builder().data(JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build()).getSchemaInfo().getSchema()).build();
        to = SchemaData.builder().data(OldJSONSchema.of(Foo.class).getSchemaInfo().getSchema()).build();
        Assert.assertTrue(jsonSchemaCompatibilityCheck.isCompatible(from, to, SchemaCompatibilityStrategy.FULL));
    }

    @Data
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
        private Bar field4;
    }

    @Data
    private static class Bar {
        private boolean field1;
    }

    public static class OldJSONSchema<T> implements Schema<T> {

        private final SchemaInfo info;
        private final ObjectMapper objectMapper;
        private final Class<T> pojo;

        private OldJSONSchema(SchemaInfo info, Class<T> pojo, ObjectMapper objectMapper) {
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
        public T decode(byte[] bytes) {
            try {
                return objectMapper.readValue(new String(bytes), pojo);
            } catch (IOException e) {
                throw new RuntimeException(new SchemaSerializationException(e));
            }
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return info;
        }

        public static <T> OldJSONSchema<T> of(Class<T> pojo) throws JsonProcessingException {
            return of(pojo, Collections.emptyMap());
        }

        public static <T> OldJSONSchema<T> of(Class<T> pojo, Map<String, String> properties) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
            JsonSchema schema = schemaGen.generateSchema(pojo);

            SchemaInfo info = SchemaInfo.builder()
                    .name("")
                    .properties(properties)
                    .type(SchemaType.JSON)
                    .schema(mapper.writeValueAsBytes(schema))
                    .build();
            return new OldJSONSchema<>(info, pojo, mapper);
        }

        @Override
        public Schema<T> clone() {
            return this;
        }
    }
}
