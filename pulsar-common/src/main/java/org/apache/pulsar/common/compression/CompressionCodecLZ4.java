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

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * LZ4 Compression
 */
public class CompressionCodecLZ4 implements CompressionCodec {

    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor compressor = lz4Factory.fastCompressor();
    private static final LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = compressor.maxCompressedLength(uncompressedLength);

        ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());

        ByteBuf target = PooledByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);
        ByteBuffer targetNio = target.nioBuffer(0, maxLength);

        int compressedLength = compressor.compress(sourceNio, 0, uncompressedLength, targetNio, 0, maxLength);
        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
        ByteBuf uncompressed = PooledByteBufAllocator.DEFAULT.buffer(uncompressedLength, uncompressedLength);
        ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);

        ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());
        decompressor.decompress(encodedNio, encodedNio.position(), uncompressedNio, uncompressedNio.position(),
                uncompressedNio.remaining());

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}
