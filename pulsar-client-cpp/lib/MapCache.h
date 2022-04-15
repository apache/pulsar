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
#include <functional>
#include <unordered_map>
#include <vector>

namespace pulsar {

// A map cache that supports removing the first N oldest entries from the map.
// Value must be moveable and have the default constructor.
template <typename Key, typename Value>
class MapCache {
    std::unordered_map<Key, Value> map_;
    std::deque<Key> keys_;

   public:
    using const_iterator = typename decltype(map_)::const_iterator;
    using iterator = typename decltype(map_)::iterator;

    MapCache() = default;
    // Here we don't use =default to be compatible with GCC 4.8
    MapCache(MapCache&& rhs) noexcept : map_(std::move(rhs.map_)), keys_(std::move(rhs.keys_)) {}

    size_t size() const noexcept { return map_.size(); }

    const_iterator find(const Key& key) const { return map_.find(key); }
    iterator find(const Key& key) { return map_.find(key); }

    const_iterator end() const noexcept { return map_.end(); }
    iterator end() noexcept { return map_.end(); }

    iterator putIfAbsent(const Key& key, Value&& value) {
        auto it = map_.find(key);
        if (it == map_.end()) {
            keys_.push_back(key);
            return map_.emplace(key, std::move(value)).first;
        } else {
            return end();
        }
    }

    void removeOldestValues(size_t numToRemove,
                            const std::function<void(const Key&, const Value&)>& callback) {
        for (size_t i = 0; !keys_.empty() && i < numToRemove; i++) {
            const auto key = keys_.front();
            auto it = map_.find(key);
            if (it != map_.end()) {
                if (callback) {
                    callback(it->first, it->second);
                }
                map_.erase(it);
            }
            keys_.pop_front();
        }
    }

    void remove(const Key& key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            removeKeyFromKeys(key);
            map_.erase(it);
        }
    }

    // Following methods are only used for tests
    std::vector<Key> getKeys() const {
        std::vector<Key> keys;
        for (auto key : keys_) {
            keys.emplace_back(key);
        }
        return keys;
    }

   private:
    void removeKeyFromKeys(const Key& key) {
        for (auto it = keys_.begin(); it != keys_.end(); ++it) {
            if (*it == key) {
                keys_.erase(it);
                break;
            }
        }
    }
};

}  // namespace pulsar
