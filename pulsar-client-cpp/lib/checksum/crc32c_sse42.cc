/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
#include "crc32c_sse42.h"

#include <boost/version.hpp>
#if BOOST_VERSION >= 105500
#include <boost/predef.h>
#else
#if _MSC_VER
#pragma message("Boost version is < 1.55, disable CRC32C")
#else
#warning "Boost version is < 1.55, disable CRC32C"
#endif
#endif

#include <assert.h>
#include <stdlib.h>
#include "lib/checksum/crc32c_sw.h"
#include "gf2.hpp"

#if BOOST_ARCH_X86_64
#include <nmmintrin.h>  // SSE4.2
#include <wmmintrin.h>  // PCLMUL
#else
#ifdef _MSC_VER
#pragma message("BOOST_ARCH_X86_64 is not defined, CRC32C will be disabled")
#else
#warning "BOOST_ARCH_X86_64 is not defined, CRC32C SSE4.2 will be disabled"
#endif
#endif

#ifdef _MSC_VER
#include <intrin.h>
#elif BOOST_ARCH_X86_64
#include <cpuid.h>
#endif

//#define CRC32C_DEBUG
#define CRC32C_PCLMULQDQ

#ifdef CRC32C_DEBUG
#include <stdio.h>
#define DEBUG_PRINTF1(fmt, v1) printf(fmt, v1)
#define DEBUG_PRINTF2(fmt, v1, v2) printf(fmt, v1, v2)
#define DEBUG_PRINTF3(fmt, v1, v2, v3) printf(fmt, v1, v2, v3)
#define DEBUG_PRINTF4(fmt, v1, v2, v3, v4) printf(fmt, v1, v2, v3, v4)
#else
#define DEBUG_PRINTF1(fmt, v1)
#define DEBUG_PRINTF2(fmt, v1, v2)
#define DEBUG_PRINTF3(fmt, v1, v2, v3)
#define DEBUG_PRINTF4(fmt, v1, v2, v3, v4)
#endif

namespace pulsar {

static bool initialized = false;
static bool has_sse42 = false;
static bool has_pclmulqdq = false;

bool crc32c_initialize() {
    if (!initialized) {
#ifdef _MSC_VER
        const uint32_t cpuid_ecx_sse42 = (1 << 20);
        const uint32_t cpuid_ecx_pclmulqdq = (1 << 1);
        int CPUInfo[4] = {};
        __cpuid(CPUInfo, 1);
        has_sse42 = (CPUInfo[2] & cpuid_ecx_sse42) != 0;
        has_pclmulqdq = (CPUInfo[2] & cpuid_ecx_pclmulqdq) != 0;
#elif BOOST_ARCH_X86_64
        const uint32_t cpuid_ecx_sse42 = (1 << 20);
        const uint32_t cpuid_ecx_pclmulqdq = (1 << 1);
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
            has_sse42 = (ecx & cpuid_ecx_sse42) != 0;
            has_pclmulqdq = (ecx & cpuid_ecx_pclmulqdq) != 0;
        }
#else
        has_sse42 = false;
        has_pclmulqdq = false;
#endif
        DEBUG_PRINTF1("has_sse42 = %d\n", has_sse42);
        DEBUG_PRINTF1("has_pclmulqdq = %d\n", has_pclmulqdq);
        initialized = true;
    }

    return has_sse42;
}

chunk_config::chunk_config(size_t words, const chunk_config *next) : words(words), next(next) {
    assert(words > 0);
    assert(!next || next->words < words);
    const size_t loop_bytes = loops() * 8;
    make_shift_table(loop_bytes, shift1);
    make_shift_table(loop_bytes * 2, shift2);
}

void chunk_config::make_shift_table(size_t bytes, uint32_t table[256]) {
    bitmatrix<32, 32> op;
    op.lower_shift();
    op[0] = 0x82f63b78;  // reversed CRC-32C polynomial
    bitmatrix<32, 32> m;
    pow(m, op, bytes * 8);
    for (unsigned int i = 0; i < 256; ++i) table[i] = (const bitvector<32>)mul(m, bitvector<32>(i));
}

#if BOOST_ARCH_X86_64

static uint32_t crc32c_chunk(uint32_t crc, const void *buf, const chunk_config &config) {
    DEBUG_PRINTF3("  crc32c_chunk(crc = 0x%08x, buf = %p, config.words = " SIZE_T_FORMAT ")", crc, buf,
                  config.words);

    const uint64_t *pq = (const uint64_t *)buf;
    uint64_t crc0 = config.extra() > 1 ? _mm_crc32_u64(crc, *pq++) : crc;
    uint64_t crc1 = 0;
    uint64_t crc2 = 0;
    const size_t loops = config.loops();
    for (unsigned int i = 0; i < loops; ++i, ++pq) {
        crc1 = _mm_crc32_u64(crc1, pq[1 * loops]);
        crc2 = _mm_crc32_u64(crc2, pq[2 * loops]);
        crc0 = _mm_crc32_u64(crc0, pq[0 * loops]);
    }
    pq += 2 * loops;
    uint64_t tmp = *pq++;
#ifdef CRC32C_PCLMULQDQ
    if (has_pclmulqdq) {
        __m128i k = _mm_set_epi64x(config.shift1[1], config.shift2[1]);
        __m128i mul1 = _mm_clmulepi64_si128(_mm_cvtsi64_si128((int64_t)crc1), k, 0x10);
        __m128i mul0 = _mm_clmulepi64_si128(_mm_cvtsi64_si128((int64_t)crc0), k, 0x00);
        tmp ^= (uint64_t)_mm_cvtsi128_si64(mul1);
        tmp ^= (uint64_t)_mm_cvtsi128_si64(mul0);
    } else
#endif
    {
        tmp ^= config.shift1[crc1 & 0xff];
        tmp ^= ((uint64_t)config.shift1[(crc1 >> 8) & 0xff]) << 8;
        tmp ^= ((uint64_t)config.shift1[(crc1 >> 16) & 0xff]) << 16;
        tmp ^= ((uint64_t)config.shift1[(crc1 >> 24) & 0xff]) << 24;

        tmp ^= config.shift2[crc0 & 0xff];
        tmp ^= ((uint64_t)config.shift2[(crc0 >> 8) & 0xff]) << 8;
        tmp ^= ((uint64_t)config.shift2[(crc0 >> 16) & 0xff]) << 16;
        tmp ^= ((uint64_t)config.shift2[(crc0 >> 24) & 0xff]) << 24;
    }
    crc2 = _mm_crc32_u64(crc2, tmp);
    if (config.extra() > 2)  // only if words is divisible by 3
        crc2 = _mm_crc32_u64(crc2, *pq);
    crc = (uint32_t)crc2;

    DEBUG_PRINTF1(" = 0x%08x\n", crc);
    return crc;
}

static uint32_t crc32c_words(uint32_t crc, const void *buf, size_t count) {
    DEBUG_PRINTF3("  crc32c_words(crc = 0x%08x, buf = %p, count = " SIZE_T_FORMAT ")", crc, buf, count);

    const uint64_t *pq = (const uint64_t *)buf;
    size_t loops = (count + 7) / 8;
    assert(loops > 0);
    switch (count & 7) {
        case 0:
            do {
                crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 7:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 6:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 5:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 4:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 3:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 2:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
                case 1:
                    crc = (uint32_t)_mm_crc32_u64(crc, *pq++);
            } while (--loops > 0);
    }

    DEBUG_PRINTF1(" = 0x%08x\n", crc);
    return crc;
}

static uint32_t crc32c_bytes(uint32_t crc, const void *buf, size_t count) {
    DEBUG_PRINTF3("  crc32c_bytes(crc = 0x%08x, buf = %p, count = " SIZE_T_FORMAT ")", crc, buf, count);

    const uint8_t *pc = (const uint8_t *)buf;
    size_t loops = (count + 7) / 8;
    assert(loops > 0);
    switch (count & 7) {
        case 0:
            do {
                crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 7:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 6:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 5:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 4:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 3:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 2:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
                case 1:
                    crc = (uint32_t)_mm_crc32_u8(crc, *pc++);
            } while (--loops > 0);
    }

    DEBUG_PRINTF1(" = 0x%08x\n", crc);
    return crc;
}

uint32_t crc32c(uint32_t init, const void *buf, size_t len, const chunk_config *config) {
    DEBUG_PRINTF3("crc32c(init = 0x%08x, buf = %p, len = " SIZE_T_FORMAT ")\n", init, buf, len);

    uint32_t crc = ~init;
    const char *pc = (const char *)buf;
    if (len >= 24) {
        if ((uintptr_t)pc & 7) {
            size_t unaligned = 8 - ((uintptr_t)pc & 7);
            crc = crc32c_bytes(crc, pc, unaligned);
            pc += unaligned;
            len -= unaligned;
        }
        size_t words = len / 8;
        while (config) {
            while (words >= config->words) {
                crc = crc32c_chunk(crc, pc, *config);
                pc += config->words * 8;
                words -= config->words;
            }
            config = config->next;
        }
        if (words > 0) {
            crc = crc32c_words(crc, pc, words);
            pc += words * 8;
        }
        len &= 7;
    }
    if (len) crc = crc32c_bytes(crc, pc, len);
    crc = ~crc;

    DEBUG_PRINTF1("crc = 0x%08x\n", crc);
    return crc;
}

#else  // ! BOOST_ARCH_X86_64

uint32_t crc32c(uint32_t init, const void *buf, size_t len, const chunk_config *config) {
    // SSE 4.2 extension for hw implementation are not present
    return crc32c_sw(init, buf, len);  // fallback to the software implementation
}

#endif

}  // namespace pulsar
