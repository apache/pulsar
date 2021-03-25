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

#include <atomic>

namespace pulsar {

// `AtomicType` must be `std::atomic<T>` or `std::atomic_xxx,` which could be the typedef or base class
// of `std::atomic<T>` in C++11.
// See http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3690.pdf chapter 29.5.7 for reference.
template <typename AtomicType, typename T>
inline T addAndGet(AtomicType& value, T delta) {
    while (true) {
        T oldValue = value.load();
        T newValue = oldValue + delta;
        if (value.compare_exchange_weak(oldValue, newValue)) {
            return newValue;
        }
    }
}

}  // namespace pulsar
