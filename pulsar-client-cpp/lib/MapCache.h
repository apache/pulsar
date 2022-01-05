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

#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace pulsar {

// A thread safe map cache that supports removing the first N oldest entries from the map.
// Value must be moveable and have the default constructor.
template <typename Key, typename Value>
class MapCache {
    using Lock = std::lock_guard<std::mutex>;

    mutable std::mutex mutex_;
    std::unordered_map<Key, Value> map_;
    std::deque<Key> keys_;

   public:
    using const_iterator = typename decltype(map_)::const_iterator;

    MapCache() = default;
    MapCache(MapCache&&) noexcept = default;

    const_iterator find(const Key& key) const {
        Lock lock(mutex_);
        return map_.find(key);
    }

    const_iterator end() const {
        Lock lock(mutex_);
        return map_.end();
    }

    bool putIfAbsent(const Key& key, Value&& value) {
        Lock lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            map_.emplace(key, std::move(value));
            keys_.push_back(key);
            return true;
        } else {
            return false;
        }
    }

    std::vector<Value> removeOldestValues(size_t numToRemove) {
        std::vector<Value> values;
        values.reserve(numToRemove);

        Lock lock(mutex_);
        for (size_t i = 0; !keys_.empty() && i < numToRemove; i++) {
            const auto key = keys_.front();
            auto it = map_.find(key);
            if (it != map_.end()) {
                values.emplace_back(std::move(it->second));
                map_.erase(it);
            }
            keys_.pop_front();
        }
        return values;
    }

    void remove(const Key& key) {
        Lock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            removeKeyFromKeys(key);
            map_.erase(it);
        }
    }

    // Following methods are only used for tests
    std::vector<Key> getKeys() const {
        Lock lock(mutex_);
        std::vector<Key> keys;
        for (auto key : keys_) {
            keys.emplace_back(key);
        }
        return keys;
    }

    size_t size() const {
        Lock lock(mutex_);
        return map_.size();
    }

   private:
    void removeKeyFromKeys(const Key& key) {
        for (auto it = keys_.cbegin(); it != keys_.end(); ++it) {
            if (*it == key) {
                keys_.erase(it);
            }
        }
    }
};

}  // namespace pulsar
