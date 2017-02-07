/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "Backoff.h"

using namespace pulsar;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;
TEST(BackoffTest, basicTest) {
    Backoff backoff(milliseconds(5), seconds(60));
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
    ASSERT_EQ(backoff.next().total_milliseconds(), 10);
    backoff.reset();
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
}

TEST(BackoffTest, maxTest) {
    Backoff backoff(milliseconds(5), milliseconds(20));
    ASSERT_EQ(backoff.next().total_milliseconds(), 5);
    ASSERT_EQ(backoff.next().total_milliseconds(), 10);
    ASSERT_EQ(backoff.next().total_milliseconds(), 20);
    ASSERT_EQ(backoff.next().total_milliseconds(), 20);
}
