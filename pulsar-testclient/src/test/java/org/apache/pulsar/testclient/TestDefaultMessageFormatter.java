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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class TestDefaultMessageFormatter {

    @Test
    public void testFormatMessage() {
        String producerName = "producer-1";
        long msgId = 3;
        byte[] message = "{ \"producer\": \"%p\", \"msgId\": %i, \"nanoTime\": %t, \"float1\": %5.2f, \"float2\": %-5.2f, \"long1\": %12l, \"long2\": %l, \"int1\": %d, \"int2\": %1d , \"long3\": %5l,  \"str\": \"%5s\" }".getBytes();
        byte[] formatted = new DefaultMessageFormatter().formatMessage(producerName, msgId, message);
        String jsonString = new String(formatted, StandardCharsets.UTF_8);

        JSONObject obj = new JSONObject(jsonString);
        String prod = obj.getString("producer");
        int mid = obj.getInt("msgId");
        long nt = obj.getLong("nanoTime");
        float f1 = obj.getFloat("float1");
        float f2 = obj.getFloat("float2");
        long l1 = obj.getLong("long1");
        long l2 = obj.getLong("long2");
        long i1 = obj.getLong("int1");
        long i2 = obj.getLong("int2");
        String str = obj.getString("str");
        long l3 = obj.getLong("long3");
        Assert.assertEquals(producerName, prod);
        Assert.assertEquals(msgId, mid);
        Assert.assertTrue( nt > 0);
        Assert.assertNotEquals(f1, f2);
        Assert.assertNotEquals(l1, l2);
        Assert.assertNotEquals(i1, i2);
        Assert.assertTrue(l3 > 0);
        Assert.assertTrue(l3 <= 99999);
        Assert.assertTrue(i2 < 10);
        Assert.assertTrue(0 < i2);
        Assert.assertTrue(f2 < 100000);
        Assert.assertTrue( -100000 < f2);

    }
}
