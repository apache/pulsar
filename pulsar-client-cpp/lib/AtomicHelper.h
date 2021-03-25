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

template <typename T>
inline T addAndGet(std::atomic<T>& value, T delta) {
    while (true) {
        T oldValue = value.load();
        T newValue = oldValue + delta;
        if (value.compare_exchange_weak(oldValue, newValue)) {
            return newValue;
        }
    }
}

// GCC (>= 4.8) supports C++11, but it doesn't follow the C++ standard completely before GCC 5.
// For example, in C++11 standard, `std::atomic_int` is the typedef of `std::atomic<int>`.
// However, before GCC 5, `std::atomic<int>` derives from `std::atomic_int`, so here we add the template
// specification to make the API compatible with older GCC compiler (< 5).
#if __GNUC__ < 5
template <typename AtomicType, typename T>
inline T addAndGet(AtomicType& value, T delta) {
    return addAndGet(static_cast<std::atomic<T>&>(value), delta);
}
#endif

}  // namespace pulsar
