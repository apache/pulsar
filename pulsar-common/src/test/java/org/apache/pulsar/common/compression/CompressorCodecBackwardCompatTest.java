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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test 2-way compatibility between old JNI based codecs and the pure Java ones.
 */
public class CompressorCodecBackwardCompatTest {

    private static String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras id massa odio. Duis commodo ligula sed efficitur cursus. Aliquam sollicitudin, tellus quis suscipit tincidunt, erat sem efficitur nulla, in feugiat diam ex a dolor. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Vestibulum ac volutpat nisl, vel aliquam elit. Maecenas auctor aliquet turpis, id ullamcorper metus. Ut tincidunt et magna non ultrices. Quisque lacinia metus sed egestas tincidunt. Sed congue lacinia maximus.";

    @DataProvider(name = "codecs")
    public Object[][] codecProvider() {
        return new Object[][] {
                { new CompressionCodecLZ4(), new CompressionCodecLZ4JNI() },
                { new CompressionCodecLZ4JNI(), new CompressionCodecLZ4() },
                { new CompressionCodecZstd(), new CompressionCodecZstdJNI() },
                { new CompressionCodecZstdJNI(), new CompressionCodecZstd() },
                { new CompressionCodecSnappy(), new CompressionCodecSnappyJNI() },
                { new CompressionCodecSnappyJNI(), new CompressionCodecSnappy() },
        };
    }

    @Test(dataProvider = "codecs")
    void testCompressDecompress(CompressionCodec c1, CompressionCodec c2) throws IOException {
        byte[] data = text.getBytes();
        ByteBuf raw = PulsarByteBufAllocator.DEFAULT.directBuffer();
        raw.writeBytes(data);

        ByteBuf compressed = c1.encode(raw);
        assertEquals(raw.readableBytes(), data.length);

        int compressedSize = compressed.readableBytes();

        // Copy into a direct byte buf
        ByteBuf compressedDirect = PulsarByteBufAllocator.DEFAULT.directBuffer();
        compressedDirect.writeBytes(compressed);

        ByteBuf uncompressed = c2.decode(compressedDirect, data.length);

        assertEquals(compressedDirect.readableBytes(), compressedSize);

        assertEquals(uncompressed.readableBytes(), data.length);
        assertEquals(uncompressed, raw);

        raw.release();
        compressed.release();
        uncompressed.release();
        compressedDirect.release();

        // Verify compression codecs have the same behavior with buffers ref counting
        assertEquals(raw.refCnt(), 0);
        assertEquals(compressedDirect.refCnt(), 0);
        assertEquals(compressedDirect.refCnt(), 0);
    }
}
