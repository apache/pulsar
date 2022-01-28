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

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lz4.Lz4RawCompressor;
import io.airlift.compress.lz4.Lz4RawDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * LZ4 Compression.
 */
public class CompressionCodecLZ4 implements CompressionCodec {

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

        ByteBuf target = PulsarByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);

        int compressedLength;
        if (source.hasMemoryAddress() && target.hasMemoryAddress()) {
            compressedLength = Lz4RawCompressor.compress(
                    null,
                    source.memoryAddress() + source.readerIndex(),
                    source.readableBytes(),
                    null,
                    target.memoryAddress(),
                    maxLength,
                    LZ4_TABLE.get());
        } else {
            ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());
            ByteBuffer targetNio = target.nioBuffer(0, maxLength);

            LZ4_COMPRESSOR.get().compress(sourceNio, targetNio);
            compressedLength = targetNio.position();
        }

        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = PulsarByteBufAllocator.DEFAULT.buffer(uncompressedLength, uncompressedLength);

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
    }

}
