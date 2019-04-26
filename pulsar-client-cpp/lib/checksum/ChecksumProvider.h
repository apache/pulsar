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
#ifndef _CHECKSUM_PROVIDER_H_
#define _CHECKSUM_PROVIDER_H_

#include <stdint.h>
#include <pulsar/defines.h>

namespace pulsar {

PULSAR_PUBLIC bool crc32cSupported();
PULSAR_PUBLIC uint32_t computeChecksum(uint32_t previousChecksum, const void *data, int length);
PULSAR_PUBLIC uint32_t crc32cHw(uint32_t previousChecksum, const void *data, int length);
PULSAR_PUBLIC uint32_t crc32cSw(uint32_t previousChecksum, const void *data, int length);
}  // namespace pulsar

#endif  // _CHECKSUM_PROVIDER_H_
