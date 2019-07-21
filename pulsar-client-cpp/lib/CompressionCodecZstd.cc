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
#include "CompressionCodecZstd.h"

#if HAS_ZSTD
#include <zstd.h>

namespace pulsar {

static const int COMPRESSION_LEVEL = 3;

SharedBuffer CompressionCodecZstd::encode(const SharedBuffer& raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    size_t maxCompressedSize = ZSTD_compressBound(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(maxCompressedSize);

    int compressedSize = ZSTD_compress(compressed.mutableData(), maxCompressedSize, raw.data(),
                                       raw.readableBytes(), COMPRESSION_LEVEL);
    compressed.bytesWritten(compressedSize);

    return compressed;
}

bool CompressionCodecZstd::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                  SharedBuffer& decoded) {
    SharedBuffer decompressed = SharedBuffer::allocate(uncompressedSize);

    size_t result = ZSTD_decompress(decompressed.mutableData(), uncompressedSize, encoded.data(),
                                    encoded.readableBytes());
    if (result == uncompressedSize) {
        decompressed.bytesWritten(uncompressedSize);
        decoded = decompressed;
        return true;
    } else {
        // Decompression failed
        return false;
    }
}
}  // namespace pulsar

#else  // No ZSTD

namespace pulsar {

SharedBuffer CompressionCodecZstd::encode(const SharedBuffer& raw) { throw "ZStd compression not supported"; }

bool CompressionCodecZstd::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                  SharedBuffer& decoded) {
    throw "ZStd compression not supported";
}
}  // namespace pulsar

#endif  // HAS_ZSTD
