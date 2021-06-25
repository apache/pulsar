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
#include <atomic>
#include <chrono>
#include <thread>
#include "lib/ExecutorService.h"
#include "lib/LogUtils.h"
#include "lib/PeriodicTask.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

TEST(PeriodicTaskTest, testCountdownTask) {
    ExecutorService executor;

    std::atomic_int count{5};

    auto task = std::make_shared<PeriodicTask>(executor.getIOService(), 200);
    task->setCallback([task, &count](const PeriodicTask::ErrorCode& ec) {
        if (--count <= 0) {
            task->stop();
        }
        LOG_INFO("Now count is " << count << ", error code: " << ec.message());
    });

    // Wait for 2 seconds to verify callback won't be triggered after 1 second (200 ms * 5)
    task->start();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    LOG_INFO("Now count is " << count);
    ASSERT_EQ(count.load(), 0);
    task->stop();  // it's redundant, just to verify multiple stop() is idempotent

    // Test start again
    count = 1;
    task->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    LOG_INFO("Now count is " << count);
    ASSERT_EQ(count.load(), 0);
    task->stop();

    executor.close();
}

TEST(PeriodicTaskTest, testNegativePeriod) {
    ExecutorService executor;

    auto task = std::make_shared<PeriodicTask>(executor.getIOService(), -1);
    std::atomic_bool callbackTriggered{false};
    task->setCallback([&callbackTriggered](const PeriodicTask::ErrorCode& ec) { callbackTriggered = true; });

    task->start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(callbackTriggered.load(), false);
    task->stop();

    executor.close();
}
