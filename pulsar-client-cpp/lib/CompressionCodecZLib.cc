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
#include "CompressionCodecZLib.h"

#include <zlib.h>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

SharedBuffer CompressionCodecZLib::encode(const SharedBuffer &raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    int maxCompressedSize = compressBound(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(maxCompressedSize);

    unsigned long bytesWritten = maxCompressedSize;
    int res = compress((Bytef *)compressed.mutableData(), &bytesWritten, (const Bytef *)raw.data(),
                       raw.readableBytes());
    if (res != Z_OK) {
        LOG_ERROR("Failed to compress buffer. res=" << res);
        abort();
    }

    compressed.bytesWritten(bytesWritten);
    return compressed;
}

static bool buffer_uncompress(const char *compressedBuffer, unsigned long compressedSize, char *resultBuffer,
                              uint32_t uncompressedSize) {
    z_stream stream;
    stream.next_in = (Bytef *)compressedBuffer;
    stream.avail_in = compressedSize;
    stream.zalloc = NULL;
    stream.zfree = NULL;
    stream.opaque = NULL;

    int res = inflateInit2(&stream, MAX_WBITS);
    if (res != Z_OK) {
        LOG_ERROR("Failed to initialize inflate stream: " << res);
        return false;
    }

    stream.next_out = (Bytef *)resultBuffer;
    stream.avail_out = uncompressedSize;

    res = inflate(&stream, Z_PARTIAL_FLUSH);
    inflateEnd(&stream);

    if (res == Z_OK || res == Z_STREAM_END) {
        return true;
    } else {
        LOG_ERROR("Failed to decompress zlib buffer: " << res << " -- compressed size: " << compressedSize
                                                       << " -- uncompressed size: " << uncompressedSize);
        return false;
    }
}

bool CompressionCodecZLib::decode(const SharedBuffer &encoded, uint32_t uncompressedSize,
                                  SharedBuffer &decoded) {
    SharedBuffer decompressed = SharedBuffer::allocate(uncompressedSize);

    if (buffer_uncompress(encoded.data(), encoded.readableBytes(), decompressed.mutableData(),
                          uncompressedSize)) {
        decoded = decompressed;
        decoded.setWriterIndex(uncompressedSize);
        return true;
    } else {
        return false;
    }
}
}  // namespace pulsar
