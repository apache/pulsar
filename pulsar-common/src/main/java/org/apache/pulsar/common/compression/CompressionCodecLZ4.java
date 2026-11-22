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
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lz4.Lz4RawCompressor;
import io.airlift.compress.lz4.Lz4RawDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * LZ4 Compression.
 */
public class CompressionCodecLZ4 implements CompressionCodec {

    private final ByteBufAllocator allocator;

    public CompressionCodecLZ4() {
        this(PulsarByteBufAllocator.DEFAULT);
    }

    @VisibleForTesting
    CompressionCodecLZ4(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    private static final FastThreadLocal<Lz4Compressor> LZ4_COMPRESSOR = new FastThreadLocal<Lz4Compressor>() {
        @Override
        protected Lz4Compressor initialValue() throws Exception {
            return new Lz4Compressor();
        }
    };

    private static final FastThreadLocal<Lz4Decompressor> LZ4_DECOMPRESSOR = new FastThreadLocal<Lz4Decompressor>() {
        @Override
        protected Lz4Decompressor initialValue() throws Exception {
            return new Lz4Decompressor();
        }
    };

    private static final FastThreadLocal<int[]> LZ4_TABLE = new FastThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() throws Exception {
            return new int[Lz4RawCompressor.MAX_TABLE_SIZE];
        }
    };

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = Lz4RawCompressor.maxCompressedLength(uncompressedLength);

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
        LZ4_COMPRESSOR.get().compress(sourceNio, targetNio);
        return targetNio.position();
    }

    private static int compressRaw(Object inputBase, long inputOffset, int inputLength, ByteBuf target,
                                   int maxLength) {
        return Lz4RawCompressor.compress(inputBase, inputOffset, inputLength, null, target.memoryAddress(),
                maxLength, LZ4_TABLE.get());
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = allocator.buffer(uncompressedLength, uncompressedLength);
        try {

            if (encoded.hasMemoryAddress() && uncompressed.hasMemoryAddress()) {
                Lz4RawDecompressor.decompress(null, encoded.memoryAddress() + encoded.readerIndex(),
                        encoded.memoryAddress() + encoded.writerIndex(), null, uncompressed.memoryAddress(),
                        uncompressed.memoryAddress() + uncompressedLength);
            } else {
                ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);
                ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());
                encodedNio = AirliftUtils.ensureAirliftSupported(encodedNio);
                LZ4_DECOMPRESSOR.get().decompress(encodedNio, uncompressedNio);
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
