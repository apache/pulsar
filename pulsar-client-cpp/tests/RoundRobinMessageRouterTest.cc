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
#include <thread>

#include "../lib/RoundRobinMessageRouter.h"
#include "../lib/TopicMetadataImpl.h"

using namespace pulsar;

TEST(RoundRobinMessageRouterTest, onePartition) {
    const int numPartitions = 1;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, false, 1, 1,
                                   boost::posix_time::milliseconds(0));

    Message msg1 = MessageBuilder().setPartitionKey("my-key-1").setContent("one").build();
    Message msg2 = MessageBuilder().setPartitionKey("my-key-2").setContent("two").build();
    Message msg3 = MessageBuilder().setContent("three").build();

    int p1 = router.getPartition(msg1, TopicMetadataImpl(numPartitions));
    int p2 = router.getPartition(msg2, TopicMetadataImpl(numPartitions));
    int p3 = router.getPartition(msg3, TopicMetadataImpl(numPartitions));
    ASSERT_EQ(p1, 0);
    ASSERT_EQ(p2, 0);
    ASSERT_EQ(p3, 0);
}

TEST(RoundRobinMessageRouterTest, sameKey) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, false, 1, 1,
                                   boost::posix_time::milliseconds(0));

    Message msg1 = MessageBuilder().setPartitionKey("my-key").setContent("one").build();
    Message msg2 = MessageBuilder().setPartitionKey("my-key").setContent("two").build();

    int p1 = router.getPartition(msg1, TopicMetadataImpl(numPartitions));
    int p2 = router.getPartition(msg2, TopicMetadataImpl(numPartitions));
    ASSERT_EQ(p2, p1);
}

TEST(RoundRobinMessageRouterTest, batchingDisabled) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, false, 1, 1,
                                   boost::posix_time::milliseconds(0));

    Message msg1 = MessageBuilder().setContent("one").build();
    Message msg2 = MessageBuilder().setContent("two").build();

    int p1 = router.getPartition(msg1, TopicMetadataImpl(numPartitions));
    int p2 = router.getPartition(msg2, TopicMetadataImpl(numPartitions));
    ASSERT_EQ(p2, (p1 + 1) % numPartitions);
}

TEST(RoundRobinMessageRouterTest, batchingEnabled) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, true, 1000, 100000,
                                   boost::posix_time::seconds(1));

    int p = -1;
    for (int i = 0; i < 100; i++) {
        Message msg = MessageBuilder().setContent("0123456789").build();

        int p1 = router.getPartition(msg, TopicMetadataImpl(numPartitions));
        if (p != -1) {
            ASSERT_EQ(p1, p);
        }

        p = p1;
    }
}

TEST(RoundRobinMessageRouterTest, maxDelay) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, true, 1000, 100000,
                                   boost::posix_time::seconds(1));

    int p1 = -1;
    for (int i = 0; i < 100; i++) {
        Message msg = MessageBuilder().setContent("0123456789").build();

        int p = router.getPartition(msg, TopicMetadataImpl(numPartitions));
        if (p1 != -1) {
            ASSERT_EQ(p1, p);
        }

        p1 = p;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Second set of messages will go in separate partition

    int p2 = -1;
    for (int i = 0; i < 100; i++) {
        Message msg = MessageBuilder().setContent("0123456789").build();

        int p = router.getPartition(msg, TopicMetadataImpl(numPartitions));
        if (p2 != -1) {
            ASSERT_EQ(p2, p);
        }

        p2 = p;
    }

    ASSERT_EQ(p2, (p1 + 1) % numPartitions);
}

TEST(RoundRobinMessageRouterTest, maxNumberOfMessages) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, true, 2, 1000,
                                   boost::posix_time::seconds(1));

    Message msg1 = MessageBuilder().setContent("one").build();
    Message msg2 = MessageBuilder().setContent("two").build();
    Message msg3 = MessageBuilder().setContent("tree").build();

    TopicMetadataImpl tm = TopicMetadataImpl(numPartitions);
    int p1 = router.getPartition(msg1, tm);
    int p2 = router.getPartition(msg2, tm);
    int p3 = router.getPartition(msg3, tm);
    ASSERT_EQ(p1, p2);
    ASSERT_EQ(p3, (p2 + 1) % numPartitions);
}

TEST(RoundRobinMessageRouterTest, maxBatchSize) {
    const int numPartitions = 13;

    RoundRobinMessageRouter router(ProducerConfiguration::BoostHash, true, 10, 8,
                                   boost::posix_time::seconds(1));

    Message msg1 = MessageBuilder().setContent("one").build();
    Message msg2 = MessageBuilder().setContent("two").build();
    Message msg3 = MessageBuilder().setContent("tree").build();

    TopicMetadataImpl tm = TopicMetadataImpl(numPartitions);
    int p1 = router.getPartition(msg1, tm);
    int p2 = router.getPartition(msg2, tm);
    int p3 = router.getPartition(msg3, tm);
    ASSERT_EQ(p1, p2);
    ASSERT_EQ(p3, (p2 + 1) % numPartitions);
}
