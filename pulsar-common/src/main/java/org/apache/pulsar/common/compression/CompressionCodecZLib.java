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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * ZLib Compression.
 */
public class CompressionCodecZLib implements CompressionCodec {

    private final ByteBufAllocator allocator;

    public CompressionCodecZLib() {
        this(PulsarByteBufAllocator.DEFAULT);
    }

    @VisibleForTesting
    CompressionCodecZLib(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    private final FastThreadLocal<Deflater> deflater = new FastThreadLocal<Deflater>() {
        @Override
        protected Deflater initialValue() throws Exception {
            return new Deflater();
        }

        @Override
        protected void onRemoval(Deflater deflater) throws Exception {
            deflater.end();
        }
    };

    private final FastThreadLocal<Inflater> inflater = new FastThreadLocal<Inflater>() {
        @Override
        protected Inflater initialValue() throws Exception {
            return new Inflater();
        }

        @Override
        protected void onRemoval(Inflater inflater) throws Exception {
            inflater.end();
        }
    };

    @Override
    public ByteBuf encode(ByteBuf source) {
        int length = source.readableBytes();
        int sizeEstimate = (int) Math.ceil(length * 1.001) + 14;
        Deflater deflater = this.deflater.get();
        ByteBuf compressed = allocator.heapBuffer(sizeEstimate);
        try {
            if (source.hasArray()) {
                deflater.setInput(source.array(), source.arrayOffset() + source.readerIndex(), length);
            } else {
                deflater.setInput(inputBuffer(source));
            }
            while (!deflater.needsInput()) {
                deflate(deflater, compressed);
            }
            return compressed;
        } catch (Throwable t) {
            compressed.release();
            throw t;
        } finally {
            // Do not retain the input's NIO view in the thread-local compressor.
            deflater.reset();
        }
    }

    private static ByteBuffer inputBuffer(ByteBuf source) {
        int readerIndex = source.readerIndex();
        int length = source.readableBytes();
        try {
            return source.nioBuffer(readerIndex, length);
        } catch (UnsupportedOperationException e) {
            // Some ByteBuf implementations support getBytes but cannot expose a NIO view.
            byte[] copy = new byte[length];
            source.getBytes(readerIndex, copy);
            return ByteBuffer.wrap(copy);
        }
    }

    private static void deflate(Deflater deflater, ByteBuf out) {
        int numBytes;
        do {
            int writerIndex = out.writerIndex();
            numBytes = deflater.deflate(out.array(), out.arrayOffset() + writerIndex, out.writableBytes(),
                    Deflater.SYNC_FLUSH);
            out.writerIndex(writerIndex + numBytes);
        } while (numBytes > 0);
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        Inflater inflater = this.inflater.get();
        ByteBuf uncompressed = allocator.heapBuffer(uncompressedLength, uncompressedLength);
        try {
            int len = encoded.readableBytes();
            if (encoded.hasArray()) {
                inflater.setInput(encoded.array(), encoded.arrayOffset() + encoded.readerIndex(), len);
            } else {
                inflater.setInput(inputBuffer(encoded));
            }

            int resultLength;
            try {
                resultLength = inflater.inflate(uncompressed.array(), uncompressed.arrayOffset(), uncompressedLength);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }

            checkArgument(resultLength == uncompressedLength);

            uncompressed.writerIndex(uncompressedLength);
            return uncompressed;
        } catch (Throwable t) {
            // The output buffer is only ever handed to the caller on success: release it on corrupt input (IOException)
            // and on a decoded size that differs from the advertised one (IllegalArgumentException) alike.
            uncompressed.release();
            throw t;
        } finally {
            // Do not retain the input's NIO view in the thread-local decompressor.
            inflater.reset();
        }
    }
}
