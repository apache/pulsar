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
#include "ChecksumProvider.h"

#include <assert.h>
#include "crc32c_sse42.h"
#include "crc32c_sw.h"

namespace pulsar {
bool isCrc32cSupported = crc32cSupported();

bool crc32cSupported() { return crc32c_initialize(); }

/**
 *  computes crc32c checksum: uses sse4.2 hardware-instruction to compute crc32c if machine supports else it
 * computes using sw algo
 *  @param
 *  previousChecksum = in case of incremental-checksum-computation pass previous computed else pass 0 in other
 * case.
 *  data = for which checksum will be computed
 *  length = length of data from offset
 */
uint32_t computeChecksum(uint32_t previousChecksum, const void* data, int length) {
    if (isCrc32cSupported) {
        return crc32cHw(previousChecksum, data, length);
    } else {
        return crc32cSw(previousChecksum, data, length);
    }
}

/**
 * Computes crc32c using hardware sse4.2 instruction
 */
uint32_t crc32cHw(uint32_t previousChecksum, const void* data, int length) {
    assert(isCrc32cSupported);
    return crc32c(previousChecksum, data, length, 0);
}

/**
 * Computes crc32c using sw crc-table algo
 */
uint32_t crc32cSw(uint32_t previousChecksum, const void* data, int length) {
    return crc32c_sw(previousChecksum, data, length);
}
}  // namespace pulsar
