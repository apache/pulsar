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
#include "JavaStringHash.h"

namespace pulsar {

JavaStringHash::JavaStringHash() {}

int32_t JavaStringHash::makeHash(const std::string& key) {
    uint64_t len = key.length();
    const char* val = key.c_str();
    uint32_t hash = 0;

    for (int i = 0; i < len; i++) {
        hash = 31 * hash + val[i];
    }

    hash &= std::numeric_limits<int32_t>::max();

    return hash;
}

}  // namespace pulsar
