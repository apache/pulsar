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
#include "Backoff.h"
#include "PulsarFriend.h"

using namespace pulsar;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;

static bool checkExactAndDecrementTimer(Backoff& backoff, const unsigned int& t2) {
    const unsigned int& t1 = backoff.next().total_milliseconds();
    boost::posix_time::ptime& firstBackOffTime = PulsarFriend::getFirstBackoffTime(backoff);
    firstBackOffTime -= milliseconds(t2);
    return t1 == t2;
}

static bool withinTenPercentAndDecrementTimer(Backoff& backoff, const unsigned int& t2) {
    const unsigned int& t1 = backoff.next().total_milliseconds();
    boost::posix_time::ptime& firstBackOffTime = PulsarFriend::getFirstBackoffTime(backoff);
    firstBackOffTime -= milliseconds(t2);
    return (t1 >= t2 * 0.9 && t1 <= t2);
}

TEST(BackoffTest, mandatoryStopTestNegativeTest) {
    Backoff backoff(milliseconds(100), seconds(60), milliseconds(1900));
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    backoff.next().total_milliseconds();  // 200
    backoff.next().total_milliseconds();  // 400
    backoff.next().total_milliseconds();  // 800
    ASSERT_FALSE(withinTenPercentAndDecrementTimer(backoff, 400));
}

TEST(BackoffTest, firstBackoffTimerTest) {
    Backoff backoff(milliseconds(100), seconds(60), milliseconds(1900));
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    boost::posix_time::ptime firstBackOffTime = PulsarFriend::getFirstBackoffTime(backoff);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    TimeDuration diffBackOffTime = PulsarFriend::getFirstBackoffTime(backoff) - firstBackOffTime;
    ASSERT_EQ(diffBackOffTime, milliseconds(0));  // no change since reset not called

    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    diffBackOffTime = PulsarFriend::getFirstBackoffTime(backoff) - firstBackOffTime;
    ASSERT_TRUE(diffBackOffTime >= milliseconds(300) && diffBackOffTime < seconds(1));
}

TEST(BackoffTest, basicTest) {
    Backoff backoff(milliseconds(5), seconds(60), seconds(60));
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 5));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 10));

    backoff.reset();
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 5));
}

TEST(BackoffTest, maxTest) {
    Backoff backoff(milliseconds(5), milliseconds(20), milliseconds(20));
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 5));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 10));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 5));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 20));
}

TEST(BackoffTest, mandatoryStopTest) {
    Backoff backoff(milliseconds(100), seconds(60), milliseconds(1900));
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));
    // would have been 1600 w/o the mandatory stop
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 3200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 6400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 12800));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 25600));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 51200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));

    backoff.reset();
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));
    // would have been 1600 w/o the mandatory stop
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));

    backoff.reset();
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));

    backoff.reset();
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));
}

TEST(BackoffTest, ignoringMandatoryStopTest) {
    Backoff backoff(milliseconds(100), seconds(60), milliseconds(0));
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 1600));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 3200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 6400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 12800));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 25600));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 51200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));

    backoff.reset();
    ASSERT_TRUE(checkExactAndDecrementTimer(backoff, 100));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 800));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 1600));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 3200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 6400));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 12800));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 25600));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 51200));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));
    ASSERT_TRUE(withinTenPercentAndDecrementTimer(backoff, 60000));
}
