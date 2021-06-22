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

class CountdownTask : public PeriodicTask {
   public:
    CountdownTask(boost::asio::io_service& ioService, int periodMs, int initialCount)
        : PeriodicTask(ioService, periodMs), count_(initialCount) {}

    void callback(const ErrorCode& ec) override {
        if (--count_ <= 0) {
            stop();
        }
        LOG_INFO("Now count is " << count_ << ", error code: " << ec.message());
    }

    int getCount() const noexcept { return count_; }

   private:
    std::atomic_int count_;
};

TEST(PeriodicTaskTest, testCountdownTask) {
    ExecutorService executor;

    auto task = std::make_shared<CountdownTask>(executor.getIOService(), 200, 5);

    // Wait for 3 seconds to verify callback won't be triggered after 1 second (200 ms * 5)
    task->start();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    LOG_INFO("Now count is " << task->getCount());
    ASSERT_EQ(task->getCount(), 0);

    task->stop();  // it's redundant, just to verify multiple stop() is idempotent

    executor.close();
}
