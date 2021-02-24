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

import io.airlift.compress.zstd.ZStdRawCompressor;
import io.airlift.compress.zstd.ZStdRawDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * Zstandard Compression.
 */
public class CompressionCodecZstd implements CompressionCodec {

    private static final int ZSTD_COMPRESSION_LEVEL = 3;

    private static final ZstdCompressor ZSTD_COMPRESSOR = new ZstdCompressor();

    private static final FastThreadLocal<ZStdRawDecompressor> ZSTD_RAW_DECOMPRESSOR = //
            new FastThreadLocal<ZStdRawDecompressor>() {
                @Override
                protected ZStdRawDecompressor initialValue() throws Exception {
                    return new ZStdRawDecompressor();
                }
            };

    private static final FastThreadLocal<ZstdDecompressor> ZSTD_DECOMPRESSOR = //
            new FastThreadLocal<ZstdDecompressor>() {
                @Override
                protected ZstdDecompressor initialValue() throws Exception {
                    return new ZstdDecompressor();
                }
            };

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = (int) ZSTD_COMPRESSOR.maxCompressedLength(uncompressedLength);

        ByteBuf target = PulsarByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);
        int compressedLength;

        if (source.hasMemoryAddress() && target.hasMemoryAddress()) {
            compressedLength = ZStdRawCompressor.compress(
                    source.memoryAddress() + source.readerIndex(),
                    source.memoryAddress() + source.writerIndex(),
                    target.memoryAddress() + target.writerIndex(),
                    target.memoryAddress() + target.writerIndex() + maxLength,
                    ZSTD_COMPRESSION_LEVEL);
        } else {
            ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());
            ByteBuffer targetNio = target.nioBuffer(0, maxLength);

            ZSTD_COMPRESSOR.compress(sourceNio, targetNio);
            compressedLength = targetNio.position();
        }

        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = PulsarByteBufAllocator.DEFAULT.buffer(uncompressedLength, uncompressedLength);

        if (encoded.hasMemoryAddress() && uncompressed.hasMemoryAddress()) {
            ZSTD_RAW_DECOMPRESSOR.get().decompress(
                    null,
                    encoded.memoryAddress() + encoded.readerIndex(),
                    encoded.memoryAddress() + encoded.writerIndex(),
                    null,
                    uncompressed.memoryAddress() + uncompressed.writerIndex(),
                    uncompressed.memoryAddress() + uncompressed.writerIndex() + uncompressedLength);
        } else {
            ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);
            ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());
            encodedNio = AirliftUtils.ensureAirliftSupported(encodedNio);
            ZSTD_DECOMPRESSOR.get().decompress(encodedNio, uncompressedNio);
        }

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}
