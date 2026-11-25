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
                int nioBufferCount = source.nioBufferCount();
                ByteBuffer[] inputs = nioBufferCount > 1 ? inputBuffers(source) : null;
                if (inputs != null && inputs.length > 1) {
                    deflate(deflater, inputs, compressed);
                    return compressed;
                }
                deflater.setInput(inputs != null && inputs.length == 1
                        ? inputs[0] : inputBuffer(source, nioBufferCount));
            }
            while (!deflater.needsInput()) {
                deflate(deflater, compressed, Deflater.SYNC_FLUSH);
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

    private static ByteBuffer[] inputBuffers(ByteBuf source) {
        try {
            return source.nioBuffers(source.readerIndex(), source.readableBytes());
        } catch (UnsupportedOperationException e) {
            // Keep the contiguous-view/copy fallback for custom buffers and non-NIO composite components.
            return null;
        }
    }

    private static ByteBuffer inputBuffer(ByteBuf source, int nioBufferCount) {
        int readerIndex = source.readerIndex();
        int length = source.readableBytes();
        if (nioBufferCount > 0) {
            try {
                return source.nioBuffer(readerIndex, length);
            } catch (UnsupportedOperationException e) {
                // A custom buffer or a composite component may still reject the requested NIO view.
            }
        }
        byte[] copy = new byte[length];
        source.getBytes(readerIndex, copy);
        return ByteBuffer.wrap(copy);
    }

    private static void deflate(Deflater deflater, ByteBuffer[] inputs, ByteBuf out) {
        int last = inputs.length - 1;
        while (last >= 0 && !inputs[last].hasRemaining()) {
            last--;
        }
        for (int i = 0; i <= last; i++) {
            if (inputs[i].hasRemaining()) {
                deflater.setInput(inputs[i]);
                // Preserve one flush boundary for the whole message, regardless of component boundaries.
                deflate(deflater, out, i == last ? Deflater.SYNC_FLUSH : Deflater.NO_FLUSH);
            }
        }
    }

    private static void deflate(Deflater deflater, ByteBuf out, int flush) {
        while (true) {
            // SYNC_FLUSH needs more than six writable bytes to avoid repeatedly emitting flush markers.
            out.ensureWritable(7);
            int writerIndex = out.writerIndex();
            int available = out.writableBytes();
            long bytesRead = deflater.getBytesRead();
            int numBytes = deflater.deflate(out.array(), out.arrayOffset() + writerIndex, available, flush);
            out.writerIndex(writerIndex + numBytes);
            if (deflater.needsInput() && (flush == Deflater.NO_FLUSH || numBytes < available)) {
                return;
            }
            if (numBytes == 0 && deflater.getBytesRead() == bytesRead) {
                throw new IllegalStateException("ZLIB compressor made no progress");
            }
        }
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        Inflater inflater = this.inflater.get();
        ByteBuf uncompressed = allocator.heapBuffer(uncompressedLength, uncompressedLength);
        try {
            int resultLength;
            try {
                if (encoded.hasArray()) {
                    inflater.setInput(encoded.array(), encoded.arrayOffset() + encoded.readerIndex(),
                            encoded.readableBytes());
                    resultLength = inflater.inflate(uncompressed.array(), uncompressed.arrayOffset(),
                            uncompressedLength);
                } else {
                    int nioBufferCount = encoded.nioBufferCount();
                    ByteBuffer[] inputs = nioBufferCount > 1 ? inputBuffers(encoded) : null;
                    if (inputs != null && inputs.length > 1) {
                        resultLength = inflate(inflater, inputs, uncompressed, uncompressedLength);
                    } else {
                        inflater.setInput(inputs != null && inputs.length == 1
                                ? inputs[0] : inputBuffer(encoded, nioBufferCount));
                        resultLength = inflater.inflate(uncompressed.array(), uncompressed.arrayOffset(),
                                uncompressedLength);
                    }
                }
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

    private static int inflate(Inflater inflater, ByteBuffer[] inputs, ByteBuf out, int length)
            throws DataFormatException {
        int written = 0;
        for (ByteBuffer input : inputs) {
            if (!input.hasRemaining()) {
                continue;
            }
            inflater.setInput(input);
            while (true) {
                int position = input.position();
                int numBytes = inflater.inflate(out.array(), out.arrayOffset() + written, length - written);
                written += numBytes;
                if (inflater.finished() || inflater.needsDictionary()) {
                    return written;
                }
                if (inflater.needsInput()) {
                    // Even with a full output, a following component can contain a checksum that must be validated.
                    break;
                }
                if (written == length || (numBytes == 0 && input.position() == position)) {
                    return written;
                }
            }
        }
        return written;
    }
}
