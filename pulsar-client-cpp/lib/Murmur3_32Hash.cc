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
#include "Murmur3_32Hash.h"

#include <boost/predef.h>
#include <limits>

#if BOOST_COMP_MSVC
#include <stdlib.h>
#define ROTATE_LEFT(x, y) _rotl(x, y)
#else
#define ROTATE_LEFT(x, y) rotate_left(x, y)
#endif

#if BOOST_ENDIAN_LITTLE_BYTE
#define BYTESPWAP(x) (x)
#elif BOOST_ENDIAN_BIG_BYTE
#if BOOST_COMP_CLANG || BOOST_COMP_GNUC
#define BYTESPWAP(x) __builtin_bswap32(x)
#elif BOOST_COMP_MSVC
#define BYTESPWAP(x) _byteswap_uint32(x)
#else
#endif
#else
#endif

#define MACRO_CHUNK_SIZE 4
#define MACRO_C1 0xcc9e2d51U
#define MACRO_C2 0x1b873593U

namespace pulsar {

Murmur3_32Hash::Murmur3_32Hash() : seed(0) {}

int32_t Murmur3_32Hash::makeHash(const std::string &key) {
    return static_cast<int32_t>(makeHash(&key.front(), key.length()) & std::numeric_limits<int32_t>::max());
}

uint32_t Murmur3_32Hash::makeHash(const void *key, const int64_t len) {
    const uint8_t *data = reinterpret_cast<const uint8_t *>(key);
    const int nblocks = len / MACRO_CHUNK_SIZE;
    uint32_t h1 = seed;
    const uint32_t *blocks = reinterpret_cast<const uint32_t *>(data + nblocks * MACRO_CHUNK_SIZE);

    for (int i = -nblocks; i != 0; i++) {
        uint32_t k1 = BYTESPWAP(blocks[i]);

        k1 = mixK1(k1);
        h1 = mixH1(h1, k1);
    }

    const uint8_t *tail = reinterpret_cast<const uint8_t *>(data + nblocks * MACRO_CHUNK_SIZE);
    uint32_t k1 = 0;
    switch (len - nblocks * MACRO_CHUNK_SIZE) {
        case 3:
            k1 ^= static_cast<uint32_t>(tail[2]) << 16;
        case 2:
            k1 ^= static_cast<uint32_t>(tail[1]) << 8;
        case 1:
            k1 ^= static_cast<uint32_t>(tail[0]);
    };

    h1 ^= mixK1(k1);
    h1 ^= len;
    h1 = fmix(h1);

    return h1;
}

uint32_t Murmur3_32Hash::fmix(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

uint32_t Murmur3_32Hash::mixK1(uint32_t k1) {
    k1 *= MACRO_C1;
    k1 = ROTATE_LEFT(k1, 15);
    k1 *= MACRO_C2;
    return k1;
}

uint32_t Murmur3_32Hash::mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = ROTATE_LEFT(h1, 13);
    return h1 * 5 + 0xe6546b64;
}

uint32_t Murmur3_32Hash::rotate_left(uint32_t x, uint8_t r) { return (x << r) | (x >> ((32 - r))); }
}  // namespace pulsar
