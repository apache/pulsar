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
#include <gtest/gtest.h>
#include <lib/MapCache.h>

using namespace pulsar;

struct MoveOnlyInt {
    int x = 0;

    MoveOnlyInt() = default;
    MoveOnlyInt(int xx) : x(xx) {}
    MoveOnlyInt(const MoveOnlyInt&) = delete;
    MoveOnlyInt(MoveOnlyInt&& rhs) noexcept : x(rhs.x) {}

    bool operator=(const MoveOnlyInt& rhs) const { return x == rhs.x; }
};

using VecInt = std::vector<int>;

inline VecInt toIntVec(const std::vector<MoveOnlyInt>& v) {
    VecInt result;
    for (const auto& i : v) {
        result.emplace_back(i.x);
    }
    return result;
}

TEST(MapCacheTest, testPutIfAbsent) {
    MapCache<int, MoveOnlyInt> cache;

    ASSERT_TRUE(cache.putIfAbsent(1, {100}));
    ASSERT_FALSE(cache.putIfAbsent(1, {200}));
    auto it = cache.find(1);
    ASSERT_NE(it, cache.end());
    ASSERT_EQ(it->second.x, 100);

    cache.remove(1);
    ASSERT_EQ(cache.find(1), cache.end());
}

TEST(MapCacheTest, testRemoveOldestValues) {
    MapCache<int, MoveOnlyInt> cache;
    ASSERT_TRUE(cache.putIfAbsent(1, {200}));
    ASSERT_TRUE(cache.putIfAbsent(2, {210}));
    ASSERT_TRUE(cache.putIfAbsent(3, {220}));
    ASSERT_EQ(cache.getKeys(), (VecInt{1, 2, 3}));

    ASSERT_EQ(toIntVec(cache.removeOldestValues(2)), (VecInt{200, 210}));

    ASSERT_EQ(cache.getKeys(), (VecInt{3}));
    ASSERT_EQ(cache.size(), 1);
    auto it = cache.find(3);
    ASSERT_NE(it, cache.end());
    ASSERT_EQ(it->second.x, 220);
}

TEST(MapCacheTest, testRemoveAllValues) {
    MapCache<int, MoveOnlyInt> cache;
    ASSERT_TRUE(cache.putIfAbsent(1, {300}));
    ASSERT_TRUE(cache.putIfAbsent(2, {310}));
    ASSERT_TRUE(cache.putIfAbsent(3, {320}));

    // removeOldestValues works well even if the argument is greater than the size of keys
    ASSERT_EQ(toIntVec(cache.removeOldestValues(10000)), (VecInt{300, 310, 320}));
    ASSERT_TRUE(cache.getKeys().empty());
    ASSERT_EQ(cache.size(), 0);
}
