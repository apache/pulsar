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
import static org.testng.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressorCodecTest {

    private static final String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras id massa odio. Duis commodo ligula sed efficitur cursus. Aliquam sollicitudin, tellus quis suscipit tincidunt, erat sem efficitur nulla, in feugiat diam ex a dolor. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Vestibulum ac volutpat nisl, vel aliquam elit. Maecenas auctor aliquet turpis, id ullamcorper metus. Ut tincidunt et magna non ultrices. Quisque lacinia metus sed egestas tincidunt. Sed congue lacinia maximus.";

    private static final String noRepeatedText = "abcde";

    private static final String zipCompressedText = "789c54914d4ec4300c85aff20e50f502acd0b0840542b03789a7b2e4249dd8461c1f7706a16157d5f1fbf9fc3c2637c86ed150878e09130735f6056574e3e2ec31415576b1227d03abf88ad32483543432238c2a63c55388e5566ba30ea86ca104e30a3e9fa5c82153625ad88a47954b50830dd5eba84a5fe0ac1a86cb2193cf4a5a3a5c7a911a3d03f1244fc17627d843951648c79963939c57495dfe06ddfaacf86073f90ccd86d49d7fcbee535ada1c8b1425e786318b40a3787eb323d4a71436ecc3822767f84f51219c62123ffcd32df81a1abea77f17d3055faca0df9237602fc4857b424b3b4fced769da648b24bb1c2c8f2ead8cb9f3445ee000f57e07e008d568eb843efa5fbc15afc92ba5a094f7c97cd7d51bf82d33a6e59fc48ab7fc9d87ddeedfd3b7b434fb010000ffff";
    private static final String lz4CompressedText = "f1aa4c6f72656d20697073756d20646f6c6f722073697420616d65742c20636f6e73656374657475722061646970697363696e6720656c69742e2043726173206964206d61737361206f64696f2e204475697320636f6d6d6f646f206c6967756c612073656420656666696369747572206375727375732e20416c697175616d20736f6c6c696369747564696e2c2074656c6c757320717569732073757363697069742074696e636964756e742c20657261742073656d206566663600f20e72206e756c6c612c20696e2066657567696174206469616d2065782061d000f3022e20566573746962756c756d20616e7465ed00617072696d69733900f92761756369627573206f726369206c756374757320657420756c74726963657320706f737565726520637562696c69612043757261653b5800f3076320766f6c7574706174206e69736c2c2076656c2061e500023101f2004d616563656e617320617563746f721e00a06574207475727069732c4c0100cc00f7016d636f72706572206d657475732e20550101009500956d61676e61206e6f6e9f00f2022e2051756973717565206c6163696e69613b00027901666765737461734401502e20536564d90118673000706178696d75732e";
    private static final String zstdCompressedText= "28b52ffd600c017d0900c6573c1b9027314099d2a79d3ee4d712d9a201c61c60008415bcfbc24c1802330033003300b7c8cf78602958ceaf800dfc81a64c2a3eeea453b0a13f33625ef9e407f9ac9a96a5c172a156c8faf4c1c3c36533b98b1d1c08857e58e9c8dac1e62ee86e4db1b4d231176af67899abf5c92b3711cebd3375617edc61bad0227e013672f36cbd7e38980f4b5337564a740f132e00a3b305d3ef862ddbb14d4c9c563f7a6b35e76aedf975d0b986d752befc24a6865cf6d6704d43b0414e3007466e7902218719d848d234846e5e74b645e4b7c0864163320895031bb96ac5c79283558a7c7256ad76506bc949c6ca88807c967071191500535970afd547bb8b6f0dd0eb5822abc3580008d6f669019e5640828ea2d9b1a02d13b685db5c2f0cb70c9edac05061b912e2f3395e150b9b01";
    private static final String snappyCompressedText = "8c04f0b44c6f72656d20697073756d20646f6c6f722073697420616d65742c20636f6e73656374657475722061646970697363696e6720656c69742e2043726173206964206d61737361206f64696f2e204475697320636f6d6d6f646f206c6967756c612073656420656666696369747572206375727375732e20416c697175616d20736f6c6c696369747564696e2c2074656c6c757320717569732073757363697069742074696e636964756e742c20657261742073656d1d516c6e756c6c612c20696e2066657567696174206469616d20657820612005d0442e20566573746962756c756d20616e74652009ed147072696d69730539d861756369627573206f726369206c756374757320657420756c74726963657320706f737565726520637562696c69612043757261653b202e5800586320766f6c7574706174206e69736c2c2076656c20616c09e52931384d616563656e617320617563746f72091e246574207475727069732c214c01cc286d636f72706572206d6574212900553d010195206d61676e61206e6f6e159f402e2051756973717565206c6163696e6961093b002025791c67657374617320743144102e2053656421d900672e3000186178696d75732e";

    @DataProvider(name = "codecAndText")
    public Object[][] codecAndTextProvider() {
        return new Object[][] {
                { CompressionType.NONE, noRepeatedText},
                { CompressionType.NONE, text },
                { CompressionType.LZ4, noRepeatedText},
                { CompressionType.LZ4, text },
                { CompressionType.ZLIB, noRepeatedText},
                { CompressionType.ZLIB, text },
                { CompressionType.ZSTD, noRepeatedText},
                { CompressionType.ZSTD, text },
                { CompressionType.SNAPPY, noRepeatedText},
                { CompressionType.SNAPPY, text }
        };
    }

    @DataProvider(name = "codec")
    public Object[][] codecProvider() {
        return new Object[][] {
                { CompressionType.NONE, ByteBufUtil.hexDump(text.getBytes()) },
                { CompressionType.LZ4, lz4CompressedText },
                { CompressionType.ZLIB, zipCompressedText },
                { CompressionType.ZSTD, zstdCompressedText },
                { CompressionType.SNAPPY, snappyCompressedText }
        };
    }

    @Test(dataProvider = "codecAndText")
    void testCompressDecompress(CompressionType type, String sourceText) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        byte[] data = sourceText.getBytes();
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

    @Test(dataProvider = "codecAndText")
    void testDecompressReadonlyByteBuf(CompressionType type, String sourceText) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        byte[] data = sourceText.getBytes();
        ByteBuf raw = PulsarByteBufAllocator.DEFAULT.directBuffer();
        raw.writeBytes(data);

        ByteBuf compressed = codec.encode(raw);
        assertEquals(raw.readableBytes(), data.length);

        int compressedSize = compressed.readableBytes();
        // Readonly ByteBuffers are not supported by AirLift
        // https://github.com/apache/pulsar/issues/8974
        ByteBuf compressedComplexByteBuf = compressed.asReadOnly();
        ByteBuf uncompressed = codec.decode(compressedComplexByteBuf, data.length);

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
    void testEmptyInput(CompressionType type, String compressedText) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);

        ByteBuf compressed = codec.encode(Unpooled.EMPTY_BUFFER);
        ByteBuf uncompressed = codec.decode(compressed, 0);

        assertEquals(uncompressed, Unpooled.EMPTY_BUFFER);
    }

    @Test(dataProvider = "codecAndText")
    void testMultpileUsages(CompressionType type, String sourceText) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);
        byte[] data = sourceText.getBytes();

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
    void testCodecProvider(CompressionType type, String compressedText) throws IOException {
        CompressionCodec codec1 = CompressionCodecProvider.getCompressionCodec(type);
        CompressionCodec codec2 = CompressionCodecProvider.getCompressionCodec(type);

        // A single provider instance must return the same codec instance every time
        assertSame(codec1, codec2);
    }

    @Test(dataProvider = "codec")
    void testDecompressFromSampleBuffer(CompressionType type, String compressedText) throws IOException {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(type);

        ByteBuf compressed = PulsarByteBufAllocator.DEFAULT.directBuffer();
        compressed.writeBytes(ByteBufUtil.decodeHexDump(compressedText));

        ByteBuf uncompressed = codec.decode(compressed, text.length());
        assertEquals(uncompressed.toString(StandardCharsets.UTF_8), text);

        compressed.release();
        uncompressed.release();
    }
}
