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
#include "CompressionCodecSnappy.h"

#if HAS_SNAPPY
#include <snappy.h>
#include "snappy-c.h"

namespace pulsar {

SharedBuffer CompressionCodecSnappy::encode(const SharedBuffer& raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    int maxCompressedSize = snappy_max_compressed_length(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(maxCompressedSize);

    unsigned long bytesWritten = maxCompressedSize;

    snappy_status status =
        snappy_compress(raw.data(), raw.readableBytes(), compressed.mutableData(), &bytesWritten);

    if (status != SNAPPY_OK) {
        LOG_ERROR("Failed to compress to snappy. res=" << res);
        abort();
    }

    compressed.bytesWritten(bytesWritten);

    return compressed;
}

bool CompressionCodecSnappy::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                    SharedBuffer& decoded) {
    SharedBuffer decompressed = SharedBuffer::allocate(uncompressedSize);

    snappy_status status = snappy_uncompress(encoded.data(), encoded.readableBytes(),
                                             decompressed.mutableData(), uncompressedSize);

    if (status == SNAPPY_OK) {
        decoded = decompressed;
        decoded.setWriterIndex(uncompressedSize);
        return true;
    } else {
        // Decompression failed
        return false;
    }
}
}  // namespace pulsar

#else  // No SNAPPY

namespace pulsar {

SharedBuffer CompressionCodecSnappy::encode(const SharedBuffer& raw) {
    throw "Snappy compression not supported";
}

bool CompressionCodecSnappy::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                    SharedBuffer& decoded) {
    throw "Snappy compression not supported";
}
}  // namespace pulsar

#endif  // HAS_SNAPPY
