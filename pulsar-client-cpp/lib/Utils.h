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
#ifndef UTILS_HPP_
#define UTILS_HPP_

#include <pulsar/Result.h>

#include "Future.h"

#include <map>
#include <iostream>

namespace pulsar {

struct WaitForCallback {
    Promise<bool, Result> m_promise;

    WaitForCallback(Promise<bool, Result> promise) : m_promise(promise) {}

    void operator()(Result result) { m_promise.setValue(result); }
};

template <typename T>
struct WaitForCallbackValue {
    Promise<Result, T> m_promise;

    WaitForCallbackValue(Promise<Result, T> promise) : m_promise(promise) {}

    void operator()(Result result, const T& value) {
        if (result == ResultOk) {
            m_promise.setValue(value);
        } else {
            m_promise.setFailed(result);
        }
    }
};

template <typename T>
struct WaitForCallbackType {
    Promise<Result, T> m_promise;

    WaitForCallbackType(Promise<Result, T> promise) : m_promise(promise) {}

    void operator()(T result) { m_promise.setValue(result); }
};

static std::ostream& operator<<(std::ostream& os, const std::map<Result, unsigned long>& m) {
    os << "{";
    for (std::map<Result, unsigned long>::const_iterator it = m.begin(); it != m.end(); it++) {
        os << "[Key: " << strResult(it->first) << ", Value: " << it->second << "], ";
    }
    os << "}";
    return os;
}

/**
 * Utility class that encloses an optional value
 */
template <typename T>
class Optional {
   public:
    const T& value() const { return value_; }

    bool is_present() const { return present_; }

    bool is_empty() const { return !present_; }

    /**
     * Create an Optional with the bound value
     */
    static Optional<T> of(const T& value) { return Optional<T>(value); }

    /**
     * Create an empty optional
     */
    static Optional<T> empty() { return Optional<T>(); }

    Optional() : value_(), present_(false) {}

   private:
    Optional(const T& value) : value_(value), present_(true) {}

    T value_;
    bool present_;
};
}  // namespace pulsar

#endif /* UTILS_HPP_ */
