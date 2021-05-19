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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * ZLib Compression.
 */
public class CompressionCodecZLib implements CompressionCodec {

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
        byte[] array;
        int length = source.readableBytes();

        int sizeEstimate = (int) Math.ceil(source.readableBytes() * 1.001) + 14;
        ByteBuf compressed = PulsarByteBufAllocator.DEFAULT.heapBuffer(sizeEstimate);

        int offset = 0;
        if (source.hasArray()) {
            array = source.array();
            offset = source.arrayOffset() + source.readerIndex();
        } else {
            // If it's a direct buffer, we need to copy it
            array = new byte[length];
            source.getBytes(source.readerIndex(), array);
        }

        Deflater deflater = this.deflater.get();
        deflater.reset();
        deflater.setInput(array, offset, length);
        while (!deflater.needsInput()) {
            deflate(deflater, compressed);
        }

        return compressed;
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
        ByteBuf uncompressed = PulsarByteBufAllocator.DEFAULT.heapBuffer(uncompressedLength, uncompressedLength);

        int len = encoded.readableBytes();

        byte[] array;
        int offset;
        if (encoded.hasArray()) {
            array = encoded.array();
            offset = encoded.arrayOffset() + encoded.readerIndex();
        } else {
            array = new byte[len];
            encoded.getBytes(encoded.readerIndex(), array);
            offset = 0;
        }

        int resultLength;
        Inflater inflater = this.inflater.get();
        inflater.reset();
        inflater.setInput(array, offset, len);

        try {
            resultLength = inflater.inflate(uncompressed.array(), uncompressed.arrayOffset(), uncompressedLength);
        } catch (DataFormatException e) {
            throw new IOException(e);
        }

        checkArgument(resultLength == uncompressedLength);

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}
