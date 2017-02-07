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

#ifndef _CHECKSUM_PROVIDER_H_
#define _CHECKSUM_PROVIDER_H_

#include <stdint.h>

#pragma GCC visibility push(default)

namespace pulsar {

bool crc32cSupported();
uint32_t computeChecksum(uint32_t previousChecksum, const void *data, int length);
uint32_t crc32cHw(uint32_t previousChecksum, const void * data, int length);
uint32_t crc32cSw(uint32_t previousChecksum, const void * data, int length);
}

#pragma GCC visibility pop

#endif // _CHECKSUM_PROVIDER_H_
