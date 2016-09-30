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
package com.yahoo.pulsar.checksum.utils;

import static com.scurrilous.circe.params.CrcParameters.CRC32C;

import java.nio.ByteBuffer;

import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.crc.Sse42Crc32C;
import com.scurrilous.circe.crc.StandardCrcProvider;

import io.netty.buffer.ByteBuf;

public class Crc32cChecksum {

    private final static IncrementalIntHash CRC32C_HASH;

    static {
        if (Sse42Crc32C.isSupported()) {
            CRC32C_HASH = new Crc32cSse42Provider().getIncrementalInt(CRC32C);
        } else {
            CRC32C_HASH = new StandardCrcProvider().getIncrementalInt(CRC32C);
        }
    }

    /**
     * Computes crc32c checksum: if it is able to load crc32c native library then it computes using that native library
     * which is faster as it computes using hardware machine instruction else it computes using crc32c algo.
     * 
     * @param payload
     * @return
     */
    public static int computeChecksum(ByteBuf payload) {
        if (payload.hasMemoryAddress() && (CRC32C_HASH instanceof Sse42Crc32C)) {
            return CRC32C_HASH.calculate(payload.memoryAddress() + payload.readerIndex(), payload.readableBytes());
        } else if (payload.hasArray()) {
            return CRC32C_HASH.calculate(payload.array(), payload.arrayOffset() + payload.readerIndex(),
                    payload.readableBytes());
        } else {
            return CRC32C_HASH.calculate(payload.nioBuffer());
        }
    }
    
    
    public static int resumeChecksum(int previousChecksum, ByteBuf payload) {
        if (payload.hasMemoryAddress() && (CRC32C_HASH instanceof Sse42Crc32C)) {
            return CRC32C_HASH.resume(previousChecksum, payload.memoryAddress() + payload.readerIndex(),
                    payload.readableBytes());
        } else if (payload.hasArray()) {
            return CRC32C_HASH.resume(previousChecksum, payload.array(), payload.arrayOffset() + payload.readerIndex(),
                    payload.readableBytes());
        } else {
            return CRC32C_HASH.resume(previousChecksum, payload.nioBuffer());
        }
    }

}
