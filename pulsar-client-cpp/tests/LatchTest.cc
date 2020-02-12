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
#include <lib/Latch.h>
#include <thread>
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

class Service {
   private:
    std::string serviceName_;
    std::chrono::milliseconds sleepDuration_;
    Latch latch_;
    std::thread thread_;

   public:
    Service(const std::string& serviceName, std::chrono::milliseconds sleepDuration, const Latch& latch)
        : serviceName_(serviceName), sleepDuration_(sleepDuration), latch_(latch) {
        thread_ = std::thread(&Service::run, this);
    }

    void run() {
        std::this_thread::sleep_for(sleepDuration_);
        LOG_INFO("Service " << serviceName_ << " is up");
        latch_.countdown();
    }

    ~Service() { thread_.join(); }
};

TEST(LatchTest, testCountDown) {
    Latch latch(3);
    Service service1("service1", std::chrono::milliseconds(50), latch);
    Service service2("service2", std::chrono::milliseconds(30), latch);
    Service service3("service3", std::chrono::milliseconds(20), latch);
    latch.wait();
}

TEST(LatchTest, testLatchCount) {
    Latch latch(3);
    Service service1("service1", std::chrono::milliseconds(50), latch);
    Service service2("service2", std::chrono::milliseconds(30), latch);
    Service service3("service3", std::chrono::milliseconds(20), latch);
    ASSERT_EQ(3, latch.getCount());
    latch.wait();
    ASSERT_EQ(0, latch.getCount());
}

TEST(LatchTest, testTimedWait) {
    // Wait for at most 3 seconds which is more than the maximum sleep time (50 millis)
    Latch latch1(3);
    Service service1("service1", std::chrono::milliseconds(50), latch1);
    Service service2("service2", std::chrono::milliseconds(30), latch1);
    Service service3("service3", std::chrono::milliseconds(50), latch1);
    ASSERT_TRUE(latch1.wait(std::chrono::seconds(3)));

    // Wait for up to 300 millis which is less than the maximum sleep time (500 millis)
    Latch latch2(3);
    Service service4("service4", std::chrono::milliseconds(500), latch2);
    Service service5("service5", std::chrono::milliseconds(300), latch2);
    Service service6("service6", std::chrono::milliseconds(500), latch2);
    ASSERT_FALSE(latch2.wait(std::chrono::milliseconds(300)));

    // After the assert is passed and Service is destroyed because of join, the
    // main thread would not exit until service4 thread is returned.
}
