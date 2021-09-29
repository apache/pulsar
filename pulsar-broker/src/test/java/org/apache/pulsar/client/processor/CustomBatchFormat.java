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
package org.apache.pulsar.client.processor;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * A batch message whose format is customized.
 *
 * 1. First 2 bytes represent the number of messages.
 * 2. Each message is a string, whose format is
 *   1. First 2 bytes represent the length `N`.
 *   2. Followed N bytes are the bytes of the string.
 */
public class CustomBatchFormat {

    public static final String KEY = "entry.format";
    public static final String VALUE = "custom";

    public static ByteBuf serialize(Iterable<String> strings) {
        final ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(1024);
        buf.writeShort(0);
        short numMessages = 0;
        for (String s : strings) {
            writeString(buf, s);
            numMessages++;
        }
        buf.setShort(0, numMessages);
        return buf;
    }

    public static abstract class StringIterable implements Iterable<String> {

        public abstract int size();
    };

    public static StringIterable deserialize(final ByteBuf buf) {
        final int numMessages = buf.readShort();
        return new StringIterable() {
            @Override
            public int size() {
                return numMessages;
            }

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < numMessages;
                    }

                    @Override
                    public String next() {
                        index++;
                        return readString(buf);
                    }
                };
            }
        };
    }

    private static void writeString(final ByteBuf buf, final String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
    }

    private static String readString(final ByteBuf buf) {
        final short length = buf.readShort();
        final byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    public void testMultipleStrings() {
        final List<List<String>> inputs = new ArrayList<>();
        inputs.add(Collections.emptyList());
        inputs.add(Collections.singletonList("java"));
        inputs.add(Arrays.asList("hello", "world", "java"));

        for (List<String> input : inputs) {
            final ByteBuf buf = serialize(input);
            final List<String> parsedTokens = new ArrayList<>();
            deserialize(buf).forEach(parsedTokens::add);

            Assert.assertEquals(parsedTokens, input);
            Assert.assertEquals(parsedTokens.size(), input.size());

            Assert.assertEquals(buf.refCnt(), 1);
            buf.release();
        }
    }
}
