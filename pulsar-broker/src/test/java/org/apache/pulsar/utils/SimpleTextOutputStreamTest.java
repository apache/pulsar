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

import java.nio.charset.StandardCharsets;

import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Test(groups = "utils")
public class SimpleTextOutputStreamTest {

    private ByteBuf buf;
    private SimpleTextOutputStream stream;

    @BeforeMethod
    public void reset() {
        buf = Unpooled.buffer(4096);
        stream = new StatsOutputStream(buf);
    }

    @Test
    public void testBooleanFormat() {
        stream.write(false);
        assertEquals(str(), "false");

        stream.write(true);
        assertEquals(str(), "true");
    }

    @Test
    public void testLongFormat() {
        stream.write(0);
        assertEquals(str(), "0");

        stream.write(-1);
        assertEquals(str(), "-1");

        stream.write(123456789);
        assertEquals(str(), "123456789");

        stream.write(-123456789);
        assertEquals(str(), "-123456789");

        long i = 2 * (long) Integer.MAX_VALUE;

        stream.write(i);
        assertEquals(str(), Long.toString(i));

        stream.write(Long.MAX_VALUE);
        assertEquals(str(), Long.toString(Long.MAX_VALUE));

        stream.write(Long.MIN_VALUE);
        assertEquals(str(), Long.toString(Long.MIN_VALUE));

        // Testing trailing 0s
        stream.write(100);
        assertEquals(str(), "100");

        stream.write(-1000);
        assertEquals(str(), "-1000");
    }

    @Test
    public void testDoubleFormat() {
        stream.write(0.0);
        assertEquals(str(), "0.0");

        stream.write(1.0);
        assertEquals(str(), "1.0");

        stream.write(1.123456789);
        assertEquals(str(), "1.123");

        stream.write(123456.123456789);
        assertEquals(str(), "123456.123");

        stream.write(-123456.123456789);
        assertEquals(str(), "-123456.123");

        stream.write(-123456.003456789);
        assertEquals(str(), "-123456.003");

        stream.write(-123456.100456789);
        assertEquals(str(), "-123456.100");
    }

    @Test
    public void testString() {
        stream.writeEncoded("�\b`~�ýý8ýH\\abcd\"");
        assertEquals(str(), "\\ufffd\\u0008`~\\ufffd\\u00fd\\u00fd8\\u00fdH\\\\abcd\\\"");
    }

    public String str() {
        String s = buf.toString(StandardCharsets.UTF_8);
        reset();
        return s;
    }

    @Test
    public void testWriteString() {
        String str = "persistence://test/test/test_¬¬¬¬¬¬¬aabbcc";
        stream.write(str);
        assertEquals(str, str());
    }


    @Test
    public void testWriteChar() {
        String str = "persistence://test/test/test_¬¬¬¬¬¬¬aabbcc\"\n";
        for (char c : str.toCharArray()) {
            stream.write(c);
        }
        assertEquals(str, str());

        buf.clear();

        stream.write('\n').write('"').write('A').write('Z').write('a').write('z').write(' ').write(',').write('{')
                .write('}').write('[').write(']').write('¬');
        assertEquals(str(), "\n\"AZaz ,{}[]¬");
    }
}
