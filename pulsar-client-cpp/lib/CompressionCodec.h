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
#ifndef LIB_COMPRESSIONCODEC_H_
#define LIB_COMPRESSIONCODEC_H_

#include <pulsar/defines.h>
#include <pulsar/Producer.h>

#include "SharedBuffer.h"
#include "PulsarApi.pb.h"

#include <map>

using namespace pulsar;
namespace pulsar {

class CompressionCodec;
class CompressionCodecNone;
class CompressionCodecLZ4;
class CompressionCodecZLib;
class CompressionCodecZstd;
class CompressionCodecSnappy;

class PULSAR_PUBLIC CompressionCodecProvider {
   public:
    static CompressionType convertType(proto::CompressionType type);
    static proto::CompressionType convertType(CompressionType type);

    static CompressionCodec& getCodec(CompressionType compressionType);

   private:
    static CompressionCodecNone compressionCodecNone_;
    static CompressionCodecLZ4 compressionCodecLZ4_;
    static CompressionCodecZLib compressionCodecZLib_;
    static CompressionCodecZstd compressionCodecZstd_;
    static CompressionCodecSnappy compressionCodecSnappy_;
};

class PULSAR_PUBLIC CompressionCodec {
   public:
    virtual ~CompressionCodec() {}

    /**
     * Compress a buffer
     *
     * @param raw
     *            a buffer with the uncompressed content. The reader/writer indexes will not be modified
     * @return a buffer with the compressed content.
     */
    virtual SharedBuffer encode(const SharedBuffer& raw) = 0;

    /**
     * Decompress a buffer.
     *
     * The buffer needs to have been compressed with the matching Encoder.
     *
     * @param encoded
     *            the compressed content
     * @param uncompressedSize
     *            the size of the original content
     * @param decoded
     *             were the result will be passed
     * @return true if the buffer was decompressed, false otherwise
     */
    virtual bool decode(const SharedBuffer& encoded, uint32_t uncompressedSize, SharedBuffer& decoded) = 0;
};

class PULSAR_PUBLIC CompressionCodecNone : public CompressionCodec {
   public:
    SharedBuffer encode(const SharedBuffer& raw);

    bool decode(const SharedBuffer& encoded, uint32_t uncompressedSize, SharedBuffer& decoded);
};
}  // namespace pulsar

#endif /* LIB_COMPRESSIONCODEC_H_ */
