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
package org.apache.pulsar.utils;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class StatsOutputStreamTest {

    private ByteBuf buf;
    private StatsOutputStream stream;

    @BeforeMethod
    public void reset() {
        buf = Unpooled.buffer(4096);
        stream = new StatsOutputStream(buf);
    }

    @Test
    public void testPairs() {
        stream.writePair("my-count", 1);
        assertEquals(str(), "\"my-count\":1");

        stream.writePair("my-rate", 0.0);
        assertEquals(str(), "\"my-rate\":0.0");

        stream.writePair("my-flag", true);
        assertEquals(str(), "\"my-flag\":true");

        stream.writePair("my-string", "value");
        assertEquals(str(), "\"my-string\":\"value\"");
    }

    @Test
    public void testLists() {
        stream.startList();
        stream.endList();
        assertEquals(str(), "[]");

        stream.startList();
        stream.writeItem(1);
        stream.endList();
        assertEquals(str(), "[1]");

        stream.startList();
        stream.writeItem(1).writeItem(2);
        stream.endList();
        assertEquals(str(), "[1,2]");

        stream.startList();
        stream.writeItem(1).writeItem(2).writeItem(3);
        stream.endList();
        assertEquals(str(), "[1,2,3]");

        stream.startList();
        stream.writeItem(1).writeItem(2).writeItem(3).writeItem(false).writeItem(1.0).writeItem("xyz");
        stream.endList();
        assertEquals(str(), "[1,2,3,false,1.0,\"xyz\"]");
    }

    @Test
    public void testNamedLists() {
        stream.startList("abc");
        stream.endList();
        assertEquals(str(), "\"abc\":[]");

        stream.startList("abc");
        stream.writeItem(1);
        stream.endList();
        assertEquals(str(), "\"abc\":[1]");
    }

    @Test
    public void testObjects() {
        stream.startObject();
        stream.endObject();
        assertEquals(str(), "{}");

        stream.startObject();
        stream.writePair("a", 1);
        stream.endObject();
        assertEquals(str(), "{\"a\":1}");

        stream.startObject();
        stream.writePair("a", 1).writePair("b", 2);
        stream.endObject();
        assertEquals(str(), "{\"a\":1,\"b\":2}");

        stream.startObject();
        stream.writePair("a", 1).writePair("b", 2).writePair("c", 3);
        stream.endObject();
        assertEquals(str(), "{\"a\":1,\"b\":2,\"c\":3}");
    }

    @Test
    public void testNamedObjects() {
        stream.startObject("abc");
        stream.endObject();
        assertEquals(str(), "\"abc\":{}");

        stream.startObject("abc");
        stream.writePair("a", 1);
        stream.endObject();
        assertEquals(str(), "\"abc\":{\"a\":1}");
    }

    @Test
    public void testNestedObjects() {
        stream.startList();

        stream.startObject();
        stream.writePair("a", 1);
        stream.endObject();

        stream.startObject();
        stream.writePair("b", 2);
        stream.endObject();

        stream.endList();

        assertEquals(str(), "[{\"a\":1},{\"b\":2}]");
    }

    public String str() {
        String s = buf.toString(StandardCharsets.UTF_8);
        reset();
        return s;
    }
}
