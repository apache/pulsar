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
#include <gtest/gtest.h>
#include <lib/CompressionCodecZLib.h>

using namespace pulsar;

TEST(ZLibCompressionTest, compressDecompress) {
    CompressionCodecZLib codec;

    std::string payload = "Payload to compress";
    SharedBuffer compressed = codec.encode(SharedBuffer::copy(payload.c_str(), payload.size()));

    SharedBuffer uncompressed;
    bool res = codec.decode(compressed, payload.size(), uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(payload, std::string(uncompressed.data(), uncompressed.readableBytes()));
}

// Java and C++ are using different ZLib settings when compressing, so the resulting
// compressed blobs are slightly different. Both should lead to the same result when
// decompressing
TEST(ZLibCompressionTest, decodeCppCompressed) {
    CompressionCodecZLib codec;

    const uint8_t compressed[] = {0x78, 0x9c, 0x63, 0x60, 0x80, 0x01, 0x00, 0x00, 0x0a, 0x00, 0x01};

    SharedBuffer uncompressed;
    uint32_t uncompressedSize = 10;

    bool res = codec.decode(SharedBuffer::copy((const char*)compressed, sizeof(compressed)), uncompressedSize,
                            uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(uncompressedSize, uncompressed.readableBytes());
}

TEST(ZLibCompressionTest, decodeJavaCompressed) {
    CompressionCodecZLib codec;

    const uint8_t compressed[] = {0x78, 0x9c, 0x62, 0x60, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff};

    SharedBuffer uncompressed;
    uint32_t uncompressedSize = 10;

    bool res = codec.decode(SharedBuffer::copy((const char*)compressed, sizeof(compressed)), uncompressedSize,
                            uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(uncompressedSize, uncompressed.readableBytes());
}
