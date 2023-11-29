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
package org.apache.pulsar.client.impl.schema.generic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;


public class GenericJsonRecordTest {

    @Test
    public void decodeNullValue() throws Exception{
        byte[] json = "{\"somefield\":null}".getBytes(UTF_8);
        GenericJsonRecord record
                = new GenericJsonReader(Collections.singletonList(new Field("somefield", 0)))
                        .read(json, 0, json.length);
        assertTrue(record.getJsonNode().get("somefield").isNull());
        assertNull(record.getField("somefield"));
    }


    @Test
    public void decodeLongField() throws Exception{
        String jsonStr = "{\"timestamp\":1585204833128, \"count\":2, \"value\": 1.1, \"on\":true}";
        byte[] jsonStrBytes = jsonStr.getBytes();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jn = objectMapper.readTree(new String(jsonStrBytes, 0, jsonStrBytes.length, UTF_8));
        GenericJsonRecord record = new GenericJsonRecord(null, Collections.emptyList(), jn);

        Object longValue = record.getField("timestamp");
        assertTrue(longValue instanceof Long);
        assertEquals(1585204833128L, longValue);

        Object intValue = record.getField("count");
        assertTrue(intValue instanceof Integer);
        assertEquals(2, intValue);

        Object value = record.getField("value");
        assertTrue(value instanceof Double);
        assertEquals(1.1, value);

        Object boolValue = record.getField("on");
        assertTrue((boolean)boolValue);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Seller {
        public String state;
        public String street;
        public long zipCode;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class PC {
        public String brand;
        public String model;
        public int year;
        public GPU gpu;
        public Seller seller;
    }

    private enum GPU {
        AMD, NVIDIA
    }

    @Test
    public void testEncodeAndDecodeObject() throws JsonProcessingException {
        // test case from issue https://github.com/apache/pulsar/issues/9605
        JSONSchema<PC> jsonSchema = JSONSchema.of(SchemaDefinition.<PC>builder().withPojo(PC.class).build());
        GenericSchema genericJsonSchema = GenericJsonSchema.of(jsonSchema.getSchemaInfo());
        PC pc = new PC("dell", "alienware", 2021, GPU.AMD,
                new Seller("WA", "street", 98004));
        JsonNode jsonNode = ObjectMapperFactory.getMapper().getObjectMapper().valueToTree(pc);
        GenericJsonRecord genericJsonRecord =
                new GenericJsonRecord(null, null, jsonNode, genericJsonSchema.getSchemaInfo());
        byte[] encoded = genericJsonSchema.encode(genericJsonRecord);
        PC roundtrippedPc = jsonSchema.decode(encoded);
        assertEquals(roundtrippedPc, pc);
    }

    @Test
    public void testGetNativeRecord() throws Exception{
        byte[] json = "{\"somefield\":null}".getBytes(UTF_8);
        GenericJsonRecord record
                = new GenericJsonReader(Collections.singletonList(new Field("somefield", 0)))
                .read(json, 0, json.length);
        assertEquals(SchemaType.JSON, record.getSchemaType());
        assertSame(record.getNativeObject(), record.getJsonNode());
    }
}