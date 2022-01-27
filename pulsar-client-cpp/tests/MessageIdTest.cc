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
#include <pulsar/MessageId.h>
#include "lib/MessageIdUtil.h"
#include "PulsarFriend.h"

#include <gtest/gtest.h>

#include <string>

using namespace pulsar;

TEST(MessageIdTest, testSerialization) {
    MessageId msgId = PulsarFriend::getMessageId(-1, 1, 2, 3);

    std::string serialized;
    msgId.serialize(serialized);

    MessageId deserialized = MessageId::deserialize(serialized);

    ASSERT_EQ(msgId, deserialized);
}

TEST(MessageIdTest, testCompareLedgerAndEntryId) {
    MessageId id1(-1, 2L, 1L, 0);
    MessageId id2(-1, 2L, 1L, 1);
    MessageId id3(-1, 2L, 2L, 0);
    MessageId id4(-1, 3L, 0L, 0);
    ASSERT_EQ(compareLedgerAndEntryId(id1, id2), 0);
    ASSERT_EQ(compareLedgerAndEntryId(id1, id2), 0);

    ASSERT_EQ(compareLedgerAndEntryId(id1, id3), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id3, id1), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id1, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id1), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id2, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id2), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id3, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id3), 1);
}
