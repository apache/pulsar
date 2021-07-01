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
#include <cmath>
#include <ctime>
#include <vector>
#include <map>

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <pulsar/KeySharedPolicy.h>
#include "lib/LogUtils.h"

#include "HttpHelper.h"
#include "LogHelper.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

class KeySharedPolicyTest : public ::testing::Test {
   protected:
    KeySharedPolicyTest() : client(lookupUrl, ClientConfiguration().setPartititionsUpdateInterval(1)) {}

    void TearDown() override { client.close(); }

    void addProducer(const std::string& topicName) {
        producers.emplace_back();
        auto conf = ProducerConfiguration().setBatchingEnabled(false).setPartitionsRoutingMode(
            ProducerConfiguration::RoundRobinDistribution);
        ASSERT_EQ(ResultOk, client.createProducer(topicName, conf, producers.back()));
    }

    ConsumerConfiguration getConsumerConfiguration() {
        ConsumerConfiguration conf;
        conf.setConsumerType(ConsumerKeyShared);
        return conf;
    }

    static constexpr int NUMBER_OF_KEYS = 300;

    static Message newIntMessage(int i, const std::string& key, const char* orderingKey = nullptr) {
        MessageBuilder builder;
        if (orderingKey) {
            builder.setOrderingKey(orderingKey);
        }
        return builder.setPartitionKey(key).setContent(std::to_string(i)).build();
    }

    static void sendCallback(Result result, const MessageId&) { ASSERT_EQ(result, ResultOk); }

    void receiveAndCheckDistribution(int expectedNumTotalMessages) {
        keyToConsumer.clear();
        messagesPerConsumer.clear();
        int totalMessages = 0;

        for (size_t i = 0; i < consumers.size(); i++) {
            auto& consumer = consumers[i];
            while (true) {
                Message msg;
                Result result = consumer.receive(msg, 3000);
                if (result == ResultTimeout) {
                    break;
                }

                ASSERT_EQ(result, ResultOk);
                totalMessages++;
                messagesPerConsumer[i]++;
                ASSERT_EQ(ResultOk, consumer.acknowledge(msg));

                if (msg.hasPartitionKey() || msg.hasOrderingKey()) {
                    std::string key = msg.hasOrderingKey() ? msg.getOrderingKey() : msg.getPartitionKey();
                    auto iter = keyToConsumer.find(key);
                    if (iter == keyToConsumer.end()) {
                        keyToConsumer[key] = i;
                    } else {
                        // check messages with the same key will be consumed by the same consumer
                        ASSERT_EQ(iter->second, i);
                    }
                }
            }
        }

        LOG_INFO("messagesPerConsumer: " << messagesPerConsumer);
        int numTotalMessages = 0;
        for (const auto& kv : messagesPerConsumer) {
            numTotalMessages += kv.second;
        }
        ASSERT_EQ(numTotalMessages, expectedNumTotalMessages);

        const double expectedMessagesPerConsumer = static_cast<double>(totalMessages) / consumers.size();
        constexpr double PERCENT_ERROR = 0.50;
        for (const auto& kv : messagesPerConsumer) {
            int count = kv.second;
            ASSERT_LT(fabs(count - expectedMessagesPerConsumer), expectedMessagesPerConsumer * PERCENT_ERROR);
        }
    }

    Client client;
    std::vector<Producer> producers;
    std::vector<Consumer> consumers;
    const std::string subName = "SubscriptionName";

    // key is message's ordering key or partitioned key, value is consumer index
    std::map<std::string, size_t> keyToConsumer;
    // key is consumer index, value is the number of message received by
    std::map<size_t, int> messagesPerConsumer;
};

TEST_F(KeySharedPolicyTest, testStickyConsumer) {
    const std::string topicName = "KeySharedPolicyTest-sticky-consumer" + std::to_string(time(nullptr));

    consumers.emplace_back();
    KeySharedPolicy ksp1;
    ksp1.setKeySharedMode(STICKY);
    ksp1.setStickyRanges({StickyRange(0, 20000)});
    ConsumerConfiguration consumerConfig1 = getConsumerConfiguration().setKeySharedPolicy(ksp1);
    Result result = client.subscribe(topicName, subName, consumerConfig1, consumers.back());
    ASSERT_EQ(ResultOk, result);

    consumers.emplace_back();
    KeySharedPolicy ksp2;
    ksp2.setKeySharedMode(STICKY);
    ksp2.setStickyRanges({StickyRange(20001, 40000)});
    ConsumerConfiguration consumerConfig2 = getConsumerConfiguration().setKeySharedPolicy(ksp2);
    result = client.subscribe(topicName, subName, consumerConfig2, consumers.back());
    ASSERT_EQ(ResultOk, result);

    consumers.emplace_back();
    KeySharedPolicy ksp3;
    ksp3.setKeySharedMode(STICKY);
    ksp3.setStickyRanges({StickyRange(40001, 65535)});
    ConsumerConfiguration consumerConfig3 = getConsumerConfiguration().setKeySharedPolicy(ksp3);
    result = client.subscribe(topicName, subName, consumerConfig3, consumers.back());
    ASSERT_EQ(ResultOk, result);

    addProducer(topicName);

    srand(time(nullptr));
    constexpr int numMessagesPerProducer = 1000;
    for (int i = 0; i < numMessagesPerProducer; i++) {
        std::string key = std::to_string(rand() % NUMBER_OF_KEYS);
        producers[0].sendAsync(newIntMessage(i, key), sendCallback);
    }
    ASSERT_EQ(ResultOk, producers[0].flush());

    receiveAndCheckDistribution(numMessagesPerProducer);
}

TEST_F(KeySharedPolicyTest, ResultConsumerAssignError) {
    const std::string topicName =
        "KeySharedPolicyTest-result-consumer-assign-error" + std::to_string(time(nullptr));

    // empty range
    KeySharedPolicy ksp;
    ksp.setKeySharedMode(STICKY);
    ConsumerConfiguration consumerConfig = getConsumerConfiguration().setKeySharedPolicy(ksp);
    Consumer consumer;
    ASSERT_EQ(ResultConsumerAssignError, client.subscribe(topicName, subName, consumerConfig, consumer));

    // intersect range
    KeySharedPolicy ksp1;
    ksp1.setKeySharedMode(STICKY);
    ksp1.setStickyRanges({StickyRange(0, 65535)});
    ConsumerConfiguration consumerConfig1 = getConsumerConfiguration().setKeySharedPolicy(ksp1);
    Consumer consumer1;
    Result result = client.subscribe(topicName, subName, consumerConfig1, consumer1);
    ASSERT_EQ(ResultOk, result);

    KeySharedPolicy ksp2;
    ksp2.setKeySharedMode(STICKY);
    ksp2.setStickyRanges({StickyRange(0, 65535)});
    ConsumerConfiguration consumerConfig2 = getConsumerConfiguration().setKeySharedPolicy(ksp2);
    Consumer consumer2;
    ASSERT_EQ(ResultConsumerAssignError, client.subscribe(topicName, subName, consumerConfig2, consumer2));

    ASSERT_EQ(ResultOk, consumer1.close());
}

TEST_F(KeySharedPolicyTest, InvalidStickyRanges) {
    KeySharedPolicy ksp;
    ASSERT_THROW(ksp.setStickyRanges({}), std::invalid_argument);
    ASSERT_THROW(ksp.setStickyRanges({StickyRange(-1, 10)}), std::invalid_argument);
    ASSERT_THROW(ksp.setStickyRanges({StickyRange(0, 65536)}), std::invalid_argument);
    ASSERT_THROW(ksp.setStickyRanges({StickyRange(0, 10), StickyRange(9, 20)}), std::invalid_argument);
}
