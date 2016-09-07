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
package com.yahoo.pulsar.common.util;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class XXHashChecksum {

    private static final XXHash64 checksum = XXHashFactory.fastestInstance().hash64();

    public static long computeChecksum(ByteBuf payload) {
        if (payload.hasArray()) {
            return checksum.hash(payload.array(), payload.arrayOffset() + payload.readerIndex(),
                    payload.readableBytes(), 0L);
        } else {
            ByteBuffer payloadNio = payload.nioBuffer(payload.readerIndex(), payload.readableBytes());
            return checksum.hash(payloadNio, 0, payload.readableBytes(), 0L);
        }
    }
}
