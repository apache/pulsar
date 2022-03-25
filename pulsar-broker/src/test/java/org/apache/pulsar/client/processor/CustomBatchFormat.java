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
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

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

    @AllArgsConstructor
    @Getter
    public static class Metadata {
        private final int numMessages;
    }

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

    private static void writeString(final ByteBuf buf, final String s) {
        final byte[] bytes = Schema.STRING.encode(s);
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
    }

    public static Metadata readMetadata(final ByteBuf buf) {
        return new Metadata(buf.readShort());
    }

    public static byte[] readMessage(final ByteBuf buf) {
        final short length = buf.readShort();
        final byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return bytes;
    }
}
