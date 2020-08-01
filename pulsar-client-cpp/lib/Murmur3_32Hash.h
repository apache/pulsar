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

//-----------------------------------------------------------------------------
// The original MurmurHash3 was written by Austin Appleby, and is placed in the
// public domain. This source code, implemented by Licht Takeuchi, is based on
// the orignal MurmurHash3 source code.
#ifndef MURMUR3_32_HASH_HPP_
#define MURMUR3_32_HASH_HPP_

#include <pulsar/defines.h>
#include "Hash.h"

#include <cstdint>
#include <string>

namespace pulsar {

class PULSAR_PUBLIC Murmur3_32Hash : public Hash {
   public:
    Murmur3_32Hash();

    int32_t makeHash(const std::string& key);

   private:
    uint32_t seed;

    static uint32_t fmix(uint32_t h);
    static uint32_t mixK1(uint32_t k1);
    static uint32_t mixH1(uint32_t h1, uint32_t k1);
    static uint32_t rotate_left(uint32_t x, uint8_t r);
    uint32_t makeHash(const void* key, const int64_t len);
};
}  // namespace pulsar

#endif /* MURMUR3_32_HASH_HPP_ */
