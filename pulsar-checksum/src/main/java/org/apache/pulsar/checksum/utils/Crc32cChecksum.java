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
package org.apache.pulsar.checksum.utils;

import static com.scurrilous.circe.params.CrcParameters.CRC32C;

import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.crc.Sse42Crc32C;
import com.scurrilous.circe.crc.StandardCrcProvider;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crc32cChecksum {

    private static final Logger log = LoggerFactory.getLogger(Crc32cChecksum.class);
    private final static IncrementalIntHash CRC32C_HASH;

    static {
        if (Sse42Crc32C.isSupported()) {
            CRC32C_HASH = new Crc32cSse42Provider().getIncrementalInt(CRC32C);
            if (log.isDebugEnabled()) {
                log.debug("SSE4.2 CRC32C provider initialized");
            }
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
    
    
    /**
     * Computes incremental checksum with input previousChecksum and input payload
     * 
     * @param previousChecksum : previously computed checksum
     * @param payload
     * @return
     */
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
