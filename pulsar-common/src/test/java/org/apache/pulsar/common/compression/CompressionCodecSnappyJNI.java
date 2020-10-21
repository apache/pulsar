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
package org.apache.pulsar.common.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

/**
 * Snappy Compression.
 */
@Slf4j
public class CompressionCodecSnappyJNI implements CompressionCodec {

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = Snappy.maxCompressedLength(uncompressedLength);

        ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());

        ByteBuf target = PooledByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);
        ByteBuffer targetNio = target.nioBuffer(0, maxLength);

        int compressedLength = 0;
        try {
            compressedLength = Snappy.compress(sourceNio, targetNio);
        } catch (IOException e) {
            log.error("Failed to compress to Snappy: {}", e.getMessage());
        }
        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = PooledByteBufAllocator.DEFAULT.buffer(uncompressedLength, uncompressedLength);
        ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);

        ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());
        Snappy.uncompress(encodedNio, uncompressedNio);

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}