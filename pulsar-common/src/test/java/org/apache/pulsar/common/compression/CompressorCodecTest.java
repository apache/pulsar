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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressorCodecTest {

    private static String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras id massa odio. Duis commodo ligula sed efficitur cursus. Aliquam sollicitudin, tellus quis suscipit tincidunt, erat sem efficitur nulla, in feugiat diam ex a dolor. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Vestibulum ac volutpat nisl, vel aliquam elit. Maecenas auctor aliquet turpis, id ullamcorper metus. Ut tincidunt et magna non ultrices. Quisque lacinia metus sed egestas tincidunt. Sed congue lacinia maximus.";

    @DataProvider(name = "codec")
    public Object[][] codecProvider() {
        return new Object[][] { { CompressionType.NONE }, { CompressionType.LZ4 }, { CompressionType.ZLIB }, { CompressionType.ZSTD }, { CompressionType.SNAPPY }};
    }

    @Test(dataProvider = "codec")
    void testCompressDecompress(CompressionType type) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        byte[] data = text.getBytes();
        ByteBuf raw = PulsarByteBufAllocator.DEFAULT.directBuffer();
        raw.writeBytes(data);

        ByteBuf compressed = codec.encode(raw);
        assertEquals(raw.readableBytes(), data.length);

        int compressedSize = compressed.readableBytes();

        ByteBuf uncompressed = codec.decode(compressed, data.length);

        assertEquals(compressed.readableBytes(), compressedSize);

        assertEquals(uncompressed.readableBytes(), data.length);
        assertEquals(uncompressed, raw);

        raw.release();
        compressed.release();
        uncompressed.release();

        // Verify compression codecs have the same behavior with buffers ref counting
        assertEquals(raw.refCnt(), 0);
        assertEquals(compressed.refCnt(), 0);
        assertEquals(compressed.refCnt(), 0);
    }

    @Test(dataProvider = "codec")
    void testEmptyInput(CompressionType type) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);

        ByteBuf compressed = codec.encode(Unpooled.EMPTY_BUFFER);
        ByteBuf uncompressed = codec.decode(compressed, 0);

        assertEquals(uncompressed, Unpooled.EMPTY_BUFFER);
    }

    @Test(dataProvider = "codec")
    void testMultpileUsages(CompressionType type) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        byte[] data = text.getBytes();

        for (int i = 0; i < 5; i++) {
            ByteBuf raw = PulsarByteBufAllocator.DEFAULT.directBuffer();
            raw.writeBytes(data);
            ByteBuf compressed = codec.encode(raw);
            assertEquals(raw.readableBytes(), data.length);

            int compressedSize = compressed.readableBytes();

            ByteBuf uncompressed = codec.decode(compressed, data.length);

            assertEquals(compressed.readableBytes(), compressedSize);

            assertEquals(uncompressed.readableBytes(), data.length);
            assertEquals(uncompressed, raw);

            raw.release();
            compressed.release();
            uncompressed.release();
        }
    }

    @Test(dataProvider = "codec")
    void testCodecProvider(CompressionType type) throws IOException {
        CompressionCodec codec1 = CompressionCodecProvider.getCompressionCodec(type);
        CompressionCodec codec2 = CompressionCodecProvider.getCompressionCodec(type);

        // A single provider instance must return the same codec instance every time
        assertTrue(codec1 == codec2);
    }
}
