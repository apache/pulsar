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

#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Utils.h"

namespace pulsar {

// V must be default constructible and copyable
template <typename K, typename V>
class SynchronizedHashMap {
    using MutexType = std::recursive_mutex;
    using Lock = std::lock_guard<MutexType>;

   public:
    using OptValue = Optional<V>;
    using PairVector = std::vector<std::pair<K, V>>;

    SynchronizedHashMap() = default;

    SynchronizedHashMap(const PairVector& pairs) {
        for (auto&& kv : pairs) {
            data_.emplace(kv.first, kv.second);
        }
    }

    template <typename... Args>
    void emplace(Args&&... args) {
        Lock lock(mutex_);
        data_.emplace(std::forward<Args>(args)...);
    }

    void forEach(std::function<void(const K&, const V&)> f) const {
        Lock lock(mutex_);
        for (const auto& kv : data_) {
            f(kv.first, kv.second);
        }
    }

    void forEachValue(std::function<void(const V&)> f) const {
        Lock lock(mutex_);
        for (const auto& kv : data_) {
            f(kv.second);
        }
    }

    void clear() {
        Lock lock(mutex_);
        data_.clear();
    }

    // clear the map and apply `f` on each removed value
    void clear(std::function<void(const K&, const V&)> f) {
        Lock lock(mutex_);
        auto it = data_.begin();
        while (it != data_.end()) {
            f(it->first, it->second);
            auto next = data_.erase(it);
            it = next;
        }
    }

    OptValue find(const K& key) const {
        Lock lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return OptValue::of(it->second);
        } else {
            return OptValue::empty();
        }
    }

    OptValue findFirstValueIf(std::function<bool(const V&)> f) const {
        Lock lock(mutex_);
        for (const auto& kv : data_) {
            if (f(kv.second)) {
                return OptValue::of(kv.second);
            }
        }
        return OptValue::empty();
    }

    OptValue remove(const K& key) {
        Lock lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            auto result = OptValue::of(it->second);
            data_.erase(it);
            return result;
        } else {
            return OptValue::empty();
        }
    }

    // This method is only used for test
    PairVector toPairVector() const {
        Lock lock(mutex_);
        PairVector pairs;
        for (auto&& kv : data_) {
            pairs.emplace_back(kv);
        }
        return pairs;
    }

    // This method is only used for test
    size_t size() const noexcept {
        Lock lock(mutex_);
        return data_.size();
    }

   private:
    std::unordered_map<K, V> data_;
    // Use recursive_mutex to allow methods being called in `forEach`
    mutable MutexType mutex_;
};

}  // namespace pulsar
