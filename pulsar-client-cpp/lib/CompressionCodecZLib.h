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
#ifndef LIB_COMPRESSIONCODECZLIB_H_
#define LIB_COMPRESSIONCODECZLIB_H_

#include <pulsar/defines.h>
#include "CompressionCodec.h"
#include <zlib.h>

// Make symbol visible to unit tests

namespace pulsar {

class PULSAR_PUBLIC CompressionCodecZLib : public CompressionCodec {
   public:
    SharedBuffer encode(const SharedBuffer& raw);

    bool decode(const SharedBuffer& encoded, uint32_t uncompressedSize, SharedBuffer& decoded);
};

}  // namespace pulsar

#endif /* LIB_COMPRESSIONCODECZLIB_H_ */
