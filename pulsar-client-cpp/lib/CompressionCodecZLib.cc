/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "CompressionCodecZLib.h"

#include <zlib.h>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

SharedBuffer CompressionCodecZLib::encode(const SharedBuffer& raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    int maxCompressedSize = compressBound(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(maxCompressedSize);

    unsigned long bytesWritten = maxCompressedSize;
    int res = compress((Bytef*) compressed.mutableData(), &bytesWritten, (const Bytef*) raw.data(),
             raw.readableBytes());
    if (res != Z_OK) {
        LOG_ERROR("Failed to compress buffer. res=" << res);
        abort();
    }

    compressed.bytesWritten(bytesWritten);
    return compressed;
}

bool CompressionCodecZLib::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                  SharedBuffer& decoded) {
    SharedBuffer decompressed = SharedBuffer::allocate(uncompressedSize);

    unsigned long bytesUncompressed = uncompressedSize;
    int res = uncompress((Bytef*) decompressed.mutableData(), &bytesUncompressed, (Bytef*) encoded.data(), encoded.readableBytes());

    decompressed.bytesWritten(bytesUncompressed);
    if (res == Z_OK) {
        decoded = decompressed;
        return true;
    } else {
        return false;
    }
}

}
