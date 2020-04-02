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

#include "../lib/CompressionCodecSnappy.h"

using namespace pulsar;

TEST(CompressionCodecSnappyTest, testEncodeAndDecode) {
    CompressionCodecSnappy compressionCodecSnappy;
    char data[] = "snappy compression compresses snappy";
    size_t sz = sizeof(data);
    SharedBuffer source = SharedBuffer::wrap(data, sz);
    SharedBuffer compressed = compressionCodecSnappy.encode(source);
    ASSERT_GT(compressed.readableBytes(), 0);

    SharedBuffer uncompressed;
    bool res = compressionCodecSnappy.decode(compressed, static_cast<uint32_t>(sz), uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(uncompressed.readableBytes(), sz);
    ASSERT_STREQ(data, uncompressed.data());
}
