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
#include <thread>

#include "../lib/Semaphore.h"
#include "../lib/Latch.h"

using namespace pulsar;

TEST(SemaphoreTest, testLimit) {
    Semaphore s(100);

    for (int i = 0; i < 100; i++) {
        s.acquire();
    }

    ASSERT_EQ(s.currentUsage(), 100);
    ASSERT_FALSE(s.tryAcquire());
    s.release();
    ASSERT_EQ(s.currentUsage(), 99);

    ASSERT_TRUE(s.tryAcquire());
    ASSERT_EQ(s.currentUsage(), 100);
}

TEST(SemaphoreTest, testStepRelease) {
    Semaphore s(100);

    for (int i = 0; i < 100; i++) {
        s.acquire();
    }

    Latch l1(1);
    std::thread t1([&]() {
        s.acquire();
        l1.countdown();
    });

    Latch l2(1);
    std::thread t2([&]() {
        s.acquire();
        l2.countdown();
    });

    Latch l3(1);
    std::thread t3([&]() {
        s.acquire();
        l3.countdown();
    });

    // The threads are blocked since the quota is full
    ASSERT_FALSE(l1.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l2.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l3.wait(std::chrono::milliseconds(100)));

    ASSERT_EQ(s.currentUsage(), 100);
    s.release();
    s.release();
    s.release();

    ASSERT_TRUE(l1.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l2.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l3.wait(std::chrono::seconds(1)));
    ASSERT_EQ(s.currentUsage(), 100);

    t1.join();
    t2.join();
    t3.join();
}

TEST(SemaphoreTest, testSingleRelease) {
    Semaphore s(100);

    s.acquire(100);
    ASSERT_EQ(s.currentUsage(), 100);

    Latch l1(1);
    std::thread t1([&]() {
        s.acquire();
        l1.countdown();
    });

    Latch l2(1);
    std::thread t2([&]() {
        s.acquire();
        l2.countdown();
    });

    Latch l3(1);
    std::thread t3([&]() {
        s.acquire();
        l3.countdown();
    });

    // The threads are blocked since the quota is full
    ASSERT_FALSE(l1.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l2.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l3.wait(std::chrono::milliseconds(100)));

    ASSERT_EQ(s.currentUsage(), 100);
    s.release(3);

    ASSERT_TRUE(l1.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l2.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l3.wait(std::chrono::seconds(1)));
    ASSERT_EQ(s.currentUsage(), 100);

    t1.join();
    t2.join();
    t3.join();
}