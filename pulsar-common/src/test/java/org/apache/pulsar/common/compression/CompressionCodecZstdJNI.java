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

import com.github.luben.zstd.Zstd;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * Zstandard Compression.
 */
public class CompressionCodecZstdJNI implements CompressionCodec {

    private static final int ZSTD_COMPRESSION_LEVEL = 3;

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = (int) Zstd.compressBound(uncompressedLength);

        ByteBuf target = PulsarByteBufAllocator.DEFAULT.directBuffer(maxLength, maxLength);
        int compressedLength;

        if (source.hasMemoryAddress()) {
            compressedLength = (int) Zstd.compressUnsafe(target.memoryAddress(), maxLength,
                    source.memoryAddress() + source.readerIndex(),
                    uncompressedLength, ZSTD_COMPRESSION_LEVEL);
        } else {
            ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());
            ByteBuffer targetNio = target.nioBuffer(0, maxLength);

            compressedLength = Zstd.compress(targetNio, sourceNio, ZSTD_COMPRESSION_LEVEL);
        }

        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = PulsarByteBufAllocator.DEFAULT.directBuffer(uncompressedLength, uncompressedLength);

        if (encoded.hasMemoryAddress()) {
            Zstd.decompressUnsafe(uncompressed.memoryAddress(), uncompressedLength,
                    encoded.memoryAddress() + encoded.readerIndex(),
                    encoded.readableBytes());
        } else {
            ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);
            ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());

            Zstd.decompress(uncompressedNio, encodedNio);
        }

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}