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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include "lib/Latch.h"
#include "lib/SynchronizedHashMap.h"

using namespace pulsar;
using SyncMapType = SynchronizedHashMap<int, int>;
using OptValue = typename SyncMapType::OptValue;
using PairVector = typename SyncMapType::PairVector;

inline void sleepMs(long millis) { std::this_thread::sleep_for(std::chrono::milliseconds(millis)); }

inline PairVector sort(PairVector pairs) {
    std::sort(pairs.begin(), pairs.end(), [](const std::pair<int, int>& lhs, const std::pair<int, int>& rhs) {
        return lhs.first < rhs.first;
    });
    return pairs;
}

TEST(SynchronizedHashMap, testClear) {
    SyncMapType m({{1, 100}, {2, 200}});
    m.clear();
    ASSERT_EQ(m.toPairVector(), PairVector{});

    PairVector expectedPairs({{3, 300}, {4, 400}});
    SyncMapType m2(expectedPairs);
    PairVector pairs;
    m2.clear([&pairs](const int& key, const int& value) { pairs.emplace_back(key, value); });
    ASSERT_EQ(m2.toPairVector(), PairVector{});
    ASSERT_EQ(sort(pairs), expectedPairs);
}

TEST(SynchronizedHashMap, testRemoveAndFind) {
    SyncMapType m({{1, 100}, {2, 200}, {3, 300}});

    OptValue optValue;
    optValue = m.findFirstValueIf([](const int& x) { return x == 200; });
    ASSERT_TRUE(optValue.is_present());
    ASSERT_EQ(optValue.value(), 200);

    optValue = m.findFirstValueIf([](const int& x) { return x >= 301; });
    ASSERT_FALSE(optValue.is_present());

    optValue = m.find(1);
    ASSERT_TRUE(optValue.is_present());
    ASSERT_EQ(optValue.value(), 100);

    ASSERT_FALSE(m.find(0).is_present());
    ASSERT_FALSE(m.remove(0).is_present());

    optValue = m.remove(1);
    ASSERT_TRUE(optValue.is_present());
    ASSERT_EQ(optValue.value(), 100);

    ASSERT_FALSE(m.remove(1).is_present());
    ASSERT_FALSE(m.find(1).is_present());
}

TEST(SynchronizedHashMapTest, testForEach) {
    SyncMapType m({{1, 100}, {2, 200}, {3, 300}});
    std::vector<int> values;
    m.forEachValue([&values](const int& value) { values.emplace_back(value); });
    std::sort(values.begin(), values.end());
    ASSERT_EQ(values, std::vector<int>({100, 200, 300}));

    PairVector pairs;
    m.forEach([&pairs](const int& key, const int& value) { pairs.emplace_back(key, value); });
    PairVector expectedPairs({{1, 100}, {2, 200}, {3, 300}});
    ASSERT_EQ(sort(pairs), expectedPairs);
}

TEST(SynchronizedHashMap, testRecursiveMutex) {
    SyncMapType m({{1, 100}});
    OptValue optValue;
    m.forEach([&m, &optValue](const int& key, const int& value) {
        optValue = m.find(key);  // the internal mutex was locked again
    });
    ASSERT_TRUE(optValue.is_present());
    ASSERT_EQ(optValue.value(), 100);
}

TEST(SynchronizedHashMapTest, testThreadSafeForEach) {
    SyncMapType m({{1, 100}, {2, 200}, {3, 300}});

    Latch latch(1);
    std::thread t{[&m, &latch] {
        latch.wait();  // this thread must start after `m.forEach` started
        m.remove(2);
    }};

    std::atomic_bool firstElementDone{false};
    PairVector pairs;
    m.forEach([&latch, &firstElementDone, &pairs](const int& key, const int& value) {
        pairs.emplace_back(key, value);
        if (!firstElementDone) {
            latch.countdown();
            firstElementDone = true;
        }
        sleepMs(200);
    });
    {
        PairVector expectedPairs({{1, 100}, {2, 200}, {3, 300}});
        ASSERT_EQ(sort(pairs), expectedPairs);
    }
    t.join();
    {
        PairVector expectedPairs({{1, 100}, {3, 300}});
        ASSERT_EQ(sort(m.toPairVector()), expectedPairs);
    }
}
