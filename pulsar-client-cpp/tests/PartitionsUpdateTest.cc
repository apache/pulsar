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
#include <pulsar/Client.h>

#include <set>
#include <chrono>
#include <thread>

#include "HttpHelper.h"

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";
static const std::string topicOperateUrl = adminUrl + "admin/v2/persistent/";

static const std::string topicNameSuffix = "public/default/partitions-update-test-topic";
static const std::string topicName = "persistent://" + topicNameSuffix;

static constexpr int initialNumPartitions = 2;
static constexpr int latestNumPartitions = 5;

static const ProducerConfiguration producerConfig =
    ProducerConfiguration().setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
static const std::string subName = "SubscriptionName";

static bool updateNumPartitions() {
    int res = makePostRequest(topicOperateUrl + topicNameSuffix + "/partitions",
                              std::to_string(latestNumPartitions));
    return (res == 204 || res == 409);
}

static void waitForPartitionsUpdated() {
    // Assume producer and consumer have updated partitions in 3 seconds if enabled
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

class PartitionsUpdateTest : public ::testing::Test {
   protected:
    PartitionsUpdateTest() : clientForProducer_(serviceUrl), clientForConsumer_(serviceUrl) {
        // Ensure `topicName` doesn't exist before created
        makeDeleteRequest(topicOperateUrl + topicNameSuffix);
        makeDeleteRequest(topicOperateUrl + topicNameSuffix + "/partitions");
    }

    void SetUp() override {
        // Create a partitioned topic
        int res = makePutRequest(topicOperateUrl + topicNameSuffix + "/partitions",
                                 std::to_string(initialNumPartitions));
        ASSERT_TRUE(res == 204 || res == 409);
    }

    void TearDown() override {
        // Close producer and consumer first, otherwise delete will fail
        consumer_.close();
        clientForConsumer_.close();

        producer_.close();
        clientForProducer_.close();

        int res = makeDeleteRequest(topicOperateUrl + topicNameSuffix + "/partitions");
        ASSERT_TRUE(res == 204 || res == 409);
    }

    bool initProducer(bool enablePartitionsUpdateForProducer) {
        if (enablePartitionsUpdateForProducer) {
            clientForProducer_ = Client(serviceUrl, ClientConfiguration().setPartititionsUpdateInterval(1));
        } else {
            clientForProducer_ = Client(serviceUrl, ClientConfiguration().setPartititionsUpdateInterval(0));
        }
        return clientForProducer_.createProducer(topicName, producerConfig, producer_) == ResultOk;
    }

    bool initConsumer(bool enablePartitionsUpdateForConsumer) {
        if (enablePartitionsUpdateForConsumer) {
            clientForConsumer_ = Client(serviceUrl, ClientConfiguration().setPartititionsUpdateInterval(1));
        } else {
            clientForConsumer_ = Client(serviceUrl, ClientConfiguration().setPartititionsUpdateInterval(0));
        }
        return clientForConsumer_.subscribe(topicName, subName, consumer_) == ResultOk;
    }

    void sendMessages(int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            producer_.send(MessageBuilder().setContent("a").build());
        }
    }

    void receiveMessages(int numMessages) {
        partitionNames_.clear();
        while (numMessages > 0) {
            Message msg;
            if (consumer_.receive(msg, 100) == ResultOk) {
                partitionNames_.emplace(msg.getTopicName());
                numMessages--;
            }
        }
    }

    Client clientForProducer_;
    Producer producer_;

    Client clientForConsumer_;
    Consumer consumer_;

    std::set<std::string> partitionNames_;
};

TEST_F(PartitionsUpdateTest, testConfigPartitionsUpdateInterval) {
    ClientConfiguration clientConfig;
    ASSERT_EQ(60, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(0);
    ASSERT_EQ(0, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(1);
    ASSERT_EQ(1, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(-1);
    ASSERT_EQ(static_cast<unsigned int>(-1), clientConfig.getPartitionsUpdateInterval());
}

TEST_F(PartitionsUpdateTest, testOnlyProducerEnabled) {
    ASSERT_TRUE(initProducer(true));
    ASSERT_TRUE(initConsumer(false));

    updateNumPartitions();
    waitForPartitionsUpdated();

    // Only `initialNumPartitions / latestNumPartitions` of produced messages could be consumed
    sendMessages(latestNumPartitions * latestNumPartitions);
    receiveMessages(initialNumPartitions * latestNumPartitions);

    ASSERT_EQ(initialNumPartitions, partitionNames_.size());
}

TEST_F(PartitionsUpdateTest, testOnlyConsumerEnabled) {
    ASSERT_TRUE(initProducer(false));
    ASSERT_TRUE(initConsumer(true));

    updateNumPartitions();
    waitForPartitionsUpdated();

    sendMessages(latestNumPartitions);
    receiveMessages(latestNumPartitions);

    ASSERT_EQ(initialNumPartitions, partitionNames_.size());
}

TEST_F(PartitionsUpdateTest, testBothEnabled) {
    ASSERT_TRUE(initProducer(true));
    ASSERT_TRUE(initConsumer(true));

    updateNumPartitions();
    waitForPartitionsUpdated();

    sendMessages(latestNumPartitions);
    receiveMessages(latestNumPartitions);

    ASSERT_EQ(latestNumPartitions, partitionNames_.size());
}

TEST_F(PartitionsUpdateTest, testBothDisabled) {
    ASSERT_TRUE(initProducer(false));
    ASSERT_TRUE(initConsumer(false));

    updateNumPartitions();
    waitForPartitionsUpdated();

    sendMessages(latestNumPartitions);
    receiveMessages(latestNumPartitions);

    ASSERT_EQ(initialNumPartitions, partitionNames_.size());
}
