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
#include <pulsar/Messages.h>
#include <MessagesImpl.h>
#include "pulsar/MessageBuilder.h"

using namespace pulsar;

TEST(MessagesTest, testMessage) {
    // 0. test not limits
    {
        MessagesImpl messages(-1, -1);
        ASSERT_TRUE(messages.canAdd(Message()));
    }

    // 1. test max number of messages.
    {
        Message msg = MessageBuilder().setContent("c").build();
        MessagesImpl messages(10, -1);
        for (int i = 0; i < 10; i++) {
            messages.add(msg);
        }
        ASSERT_FALSE(messages.canAdd(msg));
        ASSERT_EQ(messages.size(), 10);
        try {
            messages.add(msg);
            FAIL() << "Should be failed.";
        } catch (std::invalid_argument& e) {
        }

        messages.clear();
        ASSERT_TRUE(messages.canAdd(msg));
        ASSERT_EQ(messages.size(), 0);
    }

    // 2. test max size of messages.
    {
        Message msg = MessageBuilder().setContent("c").build();
        MessagesImpl messages(-1, 10);
        for (int i = 0; i < 10; i++) {
            messages.add(msg);
        }
        ASSERT_FALSE(messages.canAdd(msg));
        ASSERT_EQ(messages.size(), 10);
        try {
            messages.add(msg);
            FAIL() << "Should be failed.";
        } catch (std::invalid_argument& e) {
        }

        messages.clear();
        ASSERT_TRUE(messages.canAdd(msg));
        ASSERT_EQ(messages.size(), 0);
    }
}
