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

#ifndef UTIL_CRC32C_ARM64_H
#define UTIL_CRC32C_ARM64_H

#include <cinttypes>
#include <cstddef>

#if defined(__aarch64__) || defined(__AARCH64__)

#ifdef __ARM_FEATURE_CRC32
#define HAVE_ARM64_CRC
#include <arm_acle.h>
#define crc32c_u8(crc, v) __crc32cb(crc, v)
#define crc32c_u16(crc, v) __crc32ch(crc, v)
#define crc32c_u32(crc, v) __crc32cw(crc, v)
#define crc32c_u64(crc, v) __crc32cd(crc, v)
#define PREF4X64L1(buffer, PREF_OFFSET, ITR)                                                              \
    __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]" ::[v] "r"(buffer), [c] "I"((PREF_OFFSET) + ((ITR) + 0) * 64)); \
    __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]" ::[v] "r"(buffer), [c] "I"((PREF_OFFSET) + ((ITR) + 1) * 64)); \
    __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]" ::[v] "r"(buffer), [c] "I"((PREF_OFFSET) + ((ITR) + 2) * 64)); \
    __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]" ::[v] "r"(buffer), [c] "I"((PREF_OFFSET) + ((ITR) + 3) * 64));

#define PREF1KL1(buffer, PREF_OFFSET)    \
    PREF4X64L1(buffer, (PREF_OFFSET), 0) \
    PREF4X64L1(buffer, (PREF_OFFSET), 4) \
    PREF4X64L1(buffer, (PREF_OFFSET), 8) \
    PREF4X64L1(buffer, (PREF_OFFSET), 12)
namespace pulsar {
bool crc32c_arm64_initialize();
uint32_t crc32c_arm64(uint32_t crc, const void* data, size_t len);
uint32_t crc32c_runtime_check();
bool crc32c_pmull_runtime_check();
}  // namespace pulsar
#ifdef __ARM_FEATURE_CRYPTO
#define HAVE_ARM64_CRYPTO
#include <arm_neon.h>
#endif  // __ARM_FEATURE_CRYPTO
#endif  // __ARM_FEATURE_CRC32

#endif  // defined(__aarch64__) || defined(__AARCH64__)

#endif
