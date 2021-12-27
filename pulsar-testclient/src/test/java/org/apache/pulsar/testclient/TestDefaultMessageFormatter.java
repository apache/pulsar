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
package org.apache.pulsar.testclient;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestDefaultMessageFormatter {

    @Test
    public void testFormatMessage() {
        String producerName = "producer-1";
        long msgId = 3;
        byte[] message = "{ \"producer\": \"%p\", \"msgId\": %i, \"nanoTime\": %t, \"float1\": %5.2f, \"float2\": %-5.2f, \"long1\": %12l, \"long2\": %l, \"int1\": %d, \"int2\": %1d , \"long3\": %5l,  \"str\": \"%5s\" }".getBytes();
        byte[] formatted = new DefaultMessageFormatter().formatMessage(producerName, msgId, message);
        String jsonString = new String(formatted, StandardCharsets.UTF_8);

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode obj = null;
        try {
            obj = objectMapper.readValue(jsonString, JsonNode.class);

        } catch(Exception jpe) {
            Assert.fail("Exception parsing json");
        }

        String prod = obj.get("producer").asText();
        int mid = obj.get("msgId").asInt();
        long nt = obj.get("nanoTime").asLong();
        float f1 = obj.get("float1").floatValue();
        float f2 = obj.get("float2").floatValue();
        long l1 = obj.get("long1").asLong();
        long l2 = obj.get("long2").asLong();
        long i1 = obj.get("int1").asInt();
        long i2 = obj.get("int2").asInt();
        String str = obj.get("str").asText();
        long l3 = obj.get("long3").asLong();
        Assert.assertEquals(producerName, prod);
        Assert.assertEquals(msgId, mid);
        Assert.assertTrue( nt > 0);
        Assert.assertNotEquals(f1, f2);
        Assert.assertNotEquals(l1, l2);
        Assert.assertNotEquals(i1, i2);
        Assert.assertTrue(l3 > 0);
        Assert.assertTrue(l3 <= 99999);
        Assert.assertTrue(i2 < 10);
        Assert.assertTrue(0 < i2, "i2 was " + i2);
        Assert.assertTrue(f2 < 100000);
        Assert.assertTrue( -100000 < f2);
    }

}
