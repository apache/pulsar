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
#pragma once

#include <chrono>
#include <functional>
#include <thread>

namespace pulsar {

template <typename Rep, typename Period>
inline void waitUntil(std::chrono::duration<Rep, Period> timeout, std::function<bool()> condition) {
    auto timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();
    while (timeoutMs > 0) {
        auto now = std::chrono::high_resolution_clock::now();
        if (condition()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::high_resolution_clock::now() - now)
                           .count();
        timeoutMs -= elapsed;
    }
}

}  // namespace pulsar
