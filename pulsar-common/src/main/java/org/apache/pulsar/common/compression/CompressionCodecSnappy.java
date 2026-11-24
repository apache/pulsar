/*
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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.snappy.SnappyRawCompressor;
import io.airlift.compress.snappy.SnappyRawDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * Snappy Compression.
 */
public class CompressionCodecSnappy implements CompressionCodec {

    private final ByteBufAllocator allocator;

    public CompressionCodecSnappy() {
        this(PulsarByteBufAllocator.DEFAULT);
    }

    @VisibleForTesting
    CompressionCodecSnappy(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    private static final FastThreadLocal<short[]> SNAPPY_TABLE = //
            new FastThreadLocal<short[]>() {
                @Override
                protected short[] initialValue() throws Exception {
                    return new short[SnappyRawCompressor.MAX_HASH_TABLE_SIZE];
                }
            };

    private static final FastThreadLocal<SnappyCompressor> SNAPPY_COMPRESSOR = //
            new FastThreadLocal<SnappyCompressor>() {
                @Override
                protected SnappyCompressor initialValue() throws Exception {
                    return new SnappyCompressor();
                }
            };

    private static final FastThreadLocal<SnappyDecompressor> SNAPPY_DECOMPRESSOR = //
            new FastThreadLocal<SnappyDecompressor>() {
                @Override
                protected SnappyDecompressor initialValue() throws Exception {
                    return new SnappyDecompressor();
                }
            };

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = SnappyRawCompressor.maxCompressedLength(uncompressedLength);

        ByteBuf target = allocator.buffer(maxLength, maxLength);
        int compressedLength = compress(source, target, uncompressedLength, maxLength);
        target.writerIndex(compressedLength);
        return target;
    }

    private static int compress(ByteBuf source, ByteBuf target, int uncompressedLength, int maxLength) {
        if (source.hasMemoryAddress() && target.hasMemoryAddress()) {
            return compressRaw(null, source.memoryAddress() + source.readerIndex(), uncompressedLength,
                    target, maxLength);
        }

        long arrayBaseOffset = AirliftUtils.heapArrayBaseOffset();
        if (arrayBaseOffset >= 0 && source.hasArray() && target.hasMemoryAddress()) {
            long inputOffset = arrayBaseOffset + source.arrayOffset() + source.readerIndex();
            return compressRaw(source.array(), inputOffset, uncompressedLength, target, maxLength);
        }

        ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), uncompressedLength);
        ByteBuffer targetNio = target.nioBuffer(0, maxLength);
        SNAPPY_COMPRESSOR.get().compress(sourceNio, targetNio);
        return targetNio.position();
    }

    private static int compressRaw(Object inputBase, long inputOffset, int inputLength, ByteBuf target,
                                   int maxLength) {
        long outputAddress = target.memoryAddress();
        return SnappyRawCompressor.compress(inputBase, inputOffset, inputOffset + inputLength, null,
                outputAddress, outputAddress + maxLength, SNAPPY_TABLE.get());
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = allocator.buffer(uncompressedLength, uncompressedLength);
        try {

            if (encoded.hasMemoryAddress() && uncompressed.hasMemoryAddress()) {
                SnappyRawDecompressor.decompress(
                        null,
                        encoded.memoryAddress() + encoded.readerIndex(),
                        encoded.memoryAddress() + encoded.writerIndex(),
                        null, uncompressed.memoryAddress(),
                        uncompressed.memoryAddress() + uncompressedLength);
            } else {
                ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);
                ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());

                encodedNio = AirliftUtils.ensureAirliftSupported(encodedNio);
                SNAPPY_DECOMPRESSOR.get().decompress(encodedNio, uncompressedNio);
            }

            uncompressed.writerIndex(uncompressedLength);
            return uncompressed;
        } catch (Throwable t) {
            // The output buffer is only ever handed to the caller on success: release it when the decompressor
            // rejects the input, whether as malformed or as not fitting the advertised uncompressed size.
            uncompressed.release();
            throw t;
        }
    }
}
