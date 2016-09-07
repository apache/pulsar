/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class StatsOutputStreamTest {

    private ByteBuf buf;
    private StatsOutputStream stream;

    @BeforeMethod
    public void reset() {
        buf = Unpooled.buffer(4096);
        stream = new StatsOutputStream(buf);
    }

    @Test
    public void testBooleanFormat() {
        stream.writeBoolean(false);
        assertEquals(str(), "false");

        stream.writeBoolean(true);
        assertEquals(str(), "true");
    }

    @Test
    public void testLongFormat() {
        stream.writeLong(0);
        assertEquals(str(), "0");

        stream.writeLong(-1);
        assertEquals(str(), "-1");

        stream.writeLong(123456789);
        assertEquals(str(), "123456789");

        stream.writeLong(-123456789);
        assertEquals(str(), "-123456789");

        long i = 2 * (long) Integer.MAX_VALUE;

        stream.writeLong(i);
        assertEquals(str(), Long.toString(i));

        stream.writeLong(Long.MAX_VALUE);
        assertEquals(str(), Long.toString(Long.MAX_VALUE));

        stream.writeLong(Long.MIN_VALUE);
        assertEquals(str(), Long.toString(Long.MIN_VALUE));

        // Testing trailing 0s
        stream.writeLong(100);
        assertEquals(str(), "100");

        stream.writeLong(-1000);
        assertEquals(str(), "-1000");
    }

    @Test
    public void testDoubleFormat() {
        stream.writeDouble(0.0);
        assertEquals(str(), "0.0");

        stream.writeDouble(1.0);
        assertEquals(str(), "1.0");

        stream.writeDouble(1.123456789);
        assertEquals(str(), "1.123");

        stream.writeDouble(123456.123456789);
        assertEquals(str(), "123456.123");

        stream.writeDouble(-123456.123456789);
        assertEquals(str(), "-123456.123");

        stream.writeDouble(-123456.003456789);
        assertEquals(str(), "-123456.003");

        stream.writeDouble(-123456.100456789);
        assertEquals(str(), "-123456.100");
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
        stream.write(1);
        stream.endList();
        assertEquals(str(), "[1]");

        stream.startList();
        stream.write(1).write(2);
        stream.endList();
        assertEquals(str(), "[1,2]");

        stream.startList();
        stream.write(1).write(2).write(3);
        stream.endList();
        assertEquals(str(), "[1,2,3]");

        stream.startList();
        stream.write(1).write(2).write(3).write(false).write(1.0).write("xyz");
        stream.endList();
        assertEquals(str(), "[1,2,3,false,1.0,\"xyz\"]");
    }

    @Test
    public void testNamedLists() {
        stream.startList("abc");
        stream.endList();
        assertEquals(str(), "\"abc\":[]");

        stream.startList("abc");
        stream.write(1);
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

    @Test
    public void testString() {
        stream.writeEncodedString("�\b`~�ýý8ýH\\abcd\"");
        assertEquals(str(), "\\ufffd\\u0008`~\\ufffd\\u00fd\\u00fd8\\u00fdH\\\\abcd\\\"");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCopyOnWriteArrayList() {
        try {
            CopyOnWriteArrayList.EMPTY_LIST.add(1);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.set(0, 0);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.add(1, 1);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.remove(1);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.remove("Object");
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.addIfAbsent(1);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.removeAll(CopyOnWriteArrayList.EMPTY_LIST);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.addAllAbsent(CopyOnWriteArrayList.EMPTY_LIST);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.addAll(CopyOnWriteArrayList.EMPTY_LIST);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.removeIf(null);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }
        try {
            CopyOnWriteArrayList.EMPTY_LIST.replaceAll(null);
            fail();
        } catch (UnsupportedOperationException e) {
            // Ok
        }

    }

    public String str() {
        String s = buf.toString(Charset.forName("utf-8"));
        reset();
        return s;
    }
}
