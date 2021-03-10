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

#include "../lib/MemoryLimitController.h"
#include "../lib/Latch.h"

using namespace pulsar;

TEST(MemoryLimitControllerTest, testLimit) {
    MemoryLimitController mlc(100);

    for (int i = 0; i < 101; i++) {
        mlc.reserveMemory(1);
    }

    ASSERT_EQ(mlc.currentUsage(), 101);
    ASSERT_FALSE(mlc.tryReserveMemory(1));
    mlc.releaseMemory(1);
    ASSERT_EQ(mlc.currentUsage(), 100);

    ASSERT_TRUE(mlc.tryReserveMemory(1));
    ASSERT_EQ(mlc.currentUsage(), 101);
}

TEST(MemoryLimitControllerTest, testBlocking) {
    MemoryLimitController mlc(100);

    for (int i = 0; i < 101; i++) {
        mlc.reserveMemory(1);
    }

    Latch l1(1);
    std::thread t1([&]() {
        mlc.reserveMemory(1);
        l1.countdown();
    });

    Latch l2(1);
    std::thread t2([&]() {
        mlc.reserveMemory(1);
        l2.countdown();
    });

    Latch l3(1);
    std::thread t3([&]() {
        mlc.reserveMemory(1);
        l3.countdown();
    });

    // The threads are blocked since the quota is full
    ASSERT_FALSE(l1.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l2.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l3.wait(std::chrono::milliseconds(100)));

    ASSERT_EQ(mlc.currentUsage(), 101);
    mlc.releaseMemory(3);

    ASSERT_TRUE(l1.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l2.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l3.wait(std::chrono::seconds(1)));
    ASSERT_EQ(mlc.currentUsage(), 101);

    t1.join();
    t2.join();
    t3.join();
}

TEST(MemoryLimitControllerTest, testStepRelease) {
    MemoryLimitController mlc(100);

    for (int i = 0; i < 101; i++) {
        mlc.reserveMemory(1);
    }

    Latch l1(1);
    std::thread t1([&]() {
        mlc.reserveMemory(1);
        l1.countdown();
    });

    Latch l2(1);
    std::thread t2([&]() {
        mlc.reserveMemory(1);
        l2.countdown();
    });

    Latch l3(1);
    std::thread t3([&]() {
        mlc.reserveMemory(1);
        l3.countdown();
    });

    // The threads are blocked since the quota is full
    ASSERT_FALSE(l1.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l2.wait(std::chrono::milliseconds(100)));
    ASSERT_FALSE(l3.wait(std::chrono::milliseconds(100)));

    ASSERT_EQ(mlc.currentUsage(), 101);
    mlc.releaseMemory(1);
    mlc.releaseMemory(1);
    mlc.releaseMemory(1);

    ASSERT_TRUE(l1.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l2.wait(std::chrono::seconds(1)));
    ASSERT_TRUE(l3.wait(std::chrono::seconds(1)));
    ASSERT_EQ(mlc.currentUsage(), 101);

    t1.join();
    t2.join();
    t3.join();
}