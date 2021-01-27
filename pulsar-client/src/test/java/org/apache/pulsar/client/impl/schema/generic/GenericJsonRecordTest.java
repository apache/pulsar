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
package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.pulsar.client.api.schema.Field;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class GenericJsonRecordTest {

    @Test
    public void decodeNullValue() throws Exception{
        byte[] json = "{\"somefield\":null}".getBytes(UTF_8);
        GenericJsonRecord record
                = new GenericJsonReader(Collections.singletonList(new Field("somefield", 0, GenericSchema.STRING)))
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
}