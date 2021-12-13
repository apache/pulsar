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

//  Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "crc32c_arm.h"
#include "lib/checksum/crc32c_sw.h"

#if defined(HAVE_ARM64_CRC)

#if defined(__linux__)
#include <asm/hwcap.h>
#endif
#ifdef PULSAR_AUXV_GETAUXVAL_PRESENT
#include <sys/auxv.h>
#endif
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
#ifndef HWCAP_PMULL
#define HWCAP_PMULL (1 << 4)
#endif
#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif

#ifdef HAVE_ARM64_CRYPTO
/* unfolding to compute 8 * 3 = 24 bytes parallelly */
#define CRC32C24BYTES(ITR)                                      \
    crc1 = crc32c_u64(crc1, *(buf64 + BLK_LENGTH + (ITR)));     \
    crc2 = crc32c_u64(crc2, *(buf64 + BLK_LENGTH * 2 + (ITR))); \
    crc0 = crc32c_u64(crc0, *(buf64 + (ITR)));

/* unfolding to compute 24 * 7 = 168 bytes parallelly */
#define CRC32C7X24BYTES(ITR)       \
    do {                           \
        CRC32C24BYTES((ITR)*7 + 0) \
        CRC32C24BYTES((ITR)*7 + 1) \
        CRC32C24BYTES((ITR)*7 + 2) \
        CRC32C24BYTES((ITR)*7 + 3) \
        CRC32C24BYTES((ITR)*7 + 4) \
        CRC32C24BYTES((ITR)*7 + 5) \
        CRC32C24BYTES((ITR)*7 + 6) \
    } while (0)
#endif
namespace pulsar {
static bool initialized = false;
static bool pmull_runtime_flag = false;

bool crc32c_arm64_initialize() {
    bool has_crc32c_arm_runtime = false;
    if (!initialized) {
        has_crc32c_arm_runtime = crc32c_runtime_check();
        if (has_crc32c_arm_runtime) {
            pmull_runtime_flag = crc32c_pmull_runtime_check();
        }
    }
    initialized = true;
    return has_crc32c_arm_runtime;
}

uint32_t crc32c_runtime_check() {
#if !defined(__APPLE__)
    uint64_t auxv = 0;
#if defined(PULSAR_AUXV_GETAUXVAL_PRESENT)
    auxv = getauxval(AT_HWCAP);
#elif defined(__FreeBSD__)
    elf_aux_info(AT_HWCAP, &auxv, sizeof(auxv));
#endif
    return (auxv & HWCAP_CRC32) != 0;
#else
    int r;
    size_t l = sizeof(r);
    if (sysctlbyname("hw.optional.armv8_crc32", &r, &l, NULL, 0) == -1) return 0;
    return r == 1;
#endif
}

bool crc32c_pmull_runtime_check() {
#if !defined(__APPLE__)
    uint64_t auxv = 0;
#if defined(PULSAR_AUXV_GETAUXVAL_PRESENT)
    auxv = getauxval(AT_HWCAP);
#elif defined(__FreeBSD__)
    elf_aux_info(AT_HWCAP, &auxv, sizeof(auxv));
#endif
    return (auxv & HWCAP_PMULL) != 0;
#else
    return true;
#endif
}

uint32_t crc32c_arm64(uint32_t crc, const void *data, size_t len) {
    const uint8_t *buf8;
    const uint64_t *buf64 = (uint64_t *)data;
    int length = (int)len;
    crc ^= 0xffffffff;

    /*
     * Pmull runtime check here.
     * Raspberry Pi supports crc32 but doesn't support pmull.
     * Skip Crc32c Parallel computation if no crypto extension available.
     */
    if (pmull_runtime_flag) {
/* Macro (HAVE_ARM64_CRYPTO) is used for compiling check  */
#ifdef HAVE_ARM64_CRYPTO
/* Crc32c Parallel computation
 *   Algorithm comes from Intel whitepaper:
 *   crc-iscsi-polynomial-crc32-instruction-paper
 *
 * Input data is divided into three equal-sized blocks
 *   Three parallel blocks (crc0, crc1, crc2) for 1024 Bytes
 *   One Block: 42(BLK_LENGTH) * 8(step length: crc32c_u64) bytes
 */
#define BLK_LENGTH 42
        while (length >= 1024) {
            uint64_t t0, t1;
            uint32_t crc0 = 0, crc1 = 0, crc2 = 0;

            /* Parallel Param:
             *   k0 = CRC32(x ^ (42 * 8 * 8 * 2 - 1));
             *   k1 = CRC32(x ^ (42 * 8 * 8 - 1));
             */
            uint32_t k0 = 0xe417f38a, k1 = 0x8f158014;

            /* Prefetch data for following block to avoid cache miss */
            PREF1KL1((uint8_t *)buf64, 1024);

            /* First 8 byte for better pipelining */
            crc0 = crc32c_u64(crc, *buf64++);

            /* 3 blocks crc32c parallel computation
             * Macro unfolding to compute parallelly
             * 168 * 6 = 1008 (bytes)
             */
            CRC32C7X24BYTES(0);
            CRC32C7X24BYTES(1);
            CRC32C7X24BYTES(2);
            CRC32C7X24BYTES(3);
            CRC32C7X24BYTES(4);
            CRC32C7X24BYTES(5);
            buf64 += (BLK_LENGTH * 3);

            /* Last 8 bytes */
            crc = crc32c_u64(crc2, *buf64++);

            t0 = (uint64_t)vmull_p64(crc0, k0);
            t1 = (uint64_t)vmull_p64(crc1, k1);

            /* Merge (crc0, crc1, crc2) -> crc */
            crc1 = crc32c_u64(0, t1);
            crc ^= crc1;
            crc0 = crc32c_u64(0, t0);
            crc ^= crc0;

            length -= 1024;
        }

        if (length == 0) return crc ^ (0xffffffffU);
#endif
    }  // if Pmull runtime check here

    buf8 = (const uint8_t *)buf64;
    while (length >= 8) {
        crc = crc32c_u64(crc, *(const uint64_t *)buf8);
        buf8 += 8;
        length -= 8;
    }

    /* The following is more efficient than the straight loop */
    if (length >= 4) {
        crc = crc32c_u32(crc, *(const uint32_t *)buf8);
        buf8 += 4;
        length -= 4;
    }

    if (length >= 2) {
        crc = crc32c_u16(crc, *(const uint16_t *)buf8);
        buf8 += 2;
        length -= 2;
    }

    if (length >= 1) crc = crc32c_u8(crc, *buf8);

    crc ^= 0xffffffff;
    return crc;
}

}  // namespace pulsar
#endif