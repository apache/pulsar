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
#include "Backoff.h"

using namespace pulsar;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;
TEST(BackoffTest, basicTest) {
    Backoff backoff(milliseconds(5), seconds(60), seconds(60));
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
    ASSERT_EQ(backoff.next().total_milliseconds(), 10);
    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
}

TEST(BackoffTest, maxTest) {
    Backoff backoff(milliseconds(5), milliseconds(20), milliseconds(20));
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
    ASSERT_EQ(backoff.next().total_milliseconds(), 10);
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
    ASSERT_EQ(backoff.next().total_milliseconds(), 20);
}

TEST(BackoffTest, mandatoryStopTest) {
    Backoff backoff(milliseconds(100), seconds(60), milliseconds(1900));
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    ASSERT_EQ(backoff.next().total_milliseconds(), 200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 800);
    // would have been 1600 w/o the mandatory stop
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 3200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 6400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 12800);
    ASSERT_EQ(backoff.next().total_milliseconds(), 25600);
    ASSERT_EQ(backoff.next().total_milliseconds(), 51200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 60000);
    ASSERT_EQ(backoff.next().total_milliseconds(), 60000);

    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    ASSERT_EQ(backoff.next().total_milliseconds(), 200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 800);
    // would have been 1600 w/o the mandatory stop
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);

    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    ASSERT_EQ(backoff.next().total_milliseconds(), 200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 800);

    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 100);
    ASSERT_EQ(backoff.next().total_milliseconds(), 200);
    ASSERT_EQ(backoff.next().total_milliseconds(), 400);
    ASSERT_EQ(backoff.next().total_milliseconds(), 800);
}
