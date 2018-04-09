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
#include <pulsar/Client.h>
#include <pulsar/ProducerConfiguration.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <boost/functional/hash.hpp>

#include "tests/mocks/GMockMessage.h"

#include "../lib/RoundRobinMessageRouter.h"
#include "../lib/TopicMetadataImpl.h"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::ReturnRef;

using namespace pulsar;

// TODO: Edit Message class to suit Google Mock and enable these tests when 2.0.0 release.

TEST(RoundRobinMessageRouterTest, DISABLED_getPartitionWithoutPartitionKey) {
    const int numPartitions1 = 5;
    const int numPartitions2 = 3;

    RoundRobinMessageRouter router1(ProducerConfiguration::BoostHash);
    RoundRobinMessageRouter router2(ProducerConfiguration::BoostHash);

    GMockMessage message;
    EXPECT_CALL(message, hasPartitionKey()).Times(20).WillRepeatedly(Return(false));
    EXPECT_CALL(message, getPartitionKey()).Times(0);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(i % numPartitions1, router1.getPartition(message, TopicMetadataImpl(numPartitions1)));
        ASSERT_EQ(i % numPartitions2, router2.getPartition(message, TopicMetadataImpl(numPartitions2)));
    }
}

TEST(RoundRobinMessageRouterTest, DISABLED_getPartitionWithPartitionKey) {
    const int numPartitons = 1234;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash);

    std::string partitionKey1 = "key1";
    std::string partitionKey2 = "key2";

    GMockMessage message1;
    EXPECT_CALL(message1, hasPartitionKey()).Times(1).WillOnce(Return(true));
    EXPECT_CALL(message1, getPartitionKey()).Times(1).WillOnce(ReturnRef(partitionKey1));

    GMockMessage message2;
    EXPECT_CALL(message2, hasPartitionKey()).Times(1).WillOnce(Return(true));
    EXPECT_CALL(message2, getPartitionKey()).Times(1).WillOnce(ReturnRef(partitionKey2));

    auto expectedParrtition1 =
        static_cast<const int>(boost::hash<std::string>()(partitionKey1) % numPartitons);
    auto expectedParrtition2 =
        static_cast<const int>(boost::hash<std::string>()(partitionKey2) % numPartitons);

    ASSERT_EQ(expectedParrtition1, router.getPartition(message1, TopicMetadataImpl(numPartitons)));
    ASSERT_EQ(expectedParrtition2, router.getPartition(message2, TopicMetadataImpl(numPartitons)));
}