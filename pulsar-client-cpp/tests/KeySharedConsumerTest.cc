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
#include "lib/LogUtils.h"

#include "HttpHelper.h"
#include "LogHelper.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

class KeySharedConsumerTest : public ::testing::Test {
   protected:
    KeySharedConsumerTest() : client(lookupUrl, ClientConfiguration().setPartititionsUpdateInterval(1)) {}

    void TearDown() override { client.close(); }

    void addProducer(const std::string& topicName) {
        producers.emplace_back();
        // NOTE: Current C++ producer doesn't support key based batch, so we need to disable batching
        auto conf = ProducerConfiguration().setBatchingEnabled(false).setPartitionsRoutingMode(
            ProducerConfiguration::RoundRobinDistribution);
        ASSERT_EQ(ResultOk, client.createProducer(topicName, conf, producers.back()));
    }

    ConsumerConfiguration getConsumerConfiguration() {
        ConsumerConfiguration conf;
        conf.setConsumerType(ConsumerKeyShared);
        conf.setPatternAutoDiscoveryPeriod(1);  // find new topics quickly
        return conf;
    }

    void addConsumer(const std::string& topicName) {
        consumers.emplace_back();
        ASSERT_EQ(ResultOk,
                  client.subscribe(topicName, subName, getConsumerConfiguration(), consumers.back()));
    }

    void addRegexConsumer(const std::string& pattern) {
        consumers.emplace_back();
        ASSERT_EQ(ResultOk,
                  client.subscribeWithRegex(pattern, subName, getConsumerConfiguration(), consumers.back()));
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

    void receiveAndCheckDistribution() {
        // key is message's ordering key or partitioned key, value is consumer index
        std::map<std::string, size_t> keyToConsumer;
        // key is consumer index, value is the number of message received by consumers[key]
        std::map<size_t, int> messagesPerConsumer;

        int totalMessages = 0;

        for (size_t i = 0; i < consumers.size(); i++) {
            auto& consumer = consumers[i];
            int messagesForThisConsumer = 0;
            while (true) {
                Message msg;
                Result result = consumer.receive(msg, 1000);
                if (result == ResultTimeout) {
                    messagesPerConsumer[i] = messagesForThisConsumer;
                    break;
                }

                ASSERT_EQ(result, ResultOk);
                totalMessages++;
                messagesForThisConsumer++;
                ASSERT_EQ(ResultOk, consumer.acknowledge(msg));

                if (msg.hasPartitionKey() || msg.hasOrderingKey()) {
                    std::string key = msg.hasOrderingKey() ? msg.getOrderingKey() : msg.getPartitionKey();
                    auto iter = keyToConsumer.find(key);
                    if (iter == keyToConsumer.end()) {
                        keyToConsumer[key] = i;
                    } else {
                        ASSERT_EQ(iter->second, i);
                    }
                }
            }
        }

        const double expectedMessagesPerConsumer = static_cast<double>(totalMessages) / consumers.size();
        constexpr double PERCENT_ERROR = 0.50;
        LOG_INFO("messagesPerConsumer: " << messagesPerConsumer);
        for (const auto& kv : messagesPerConsumer) {
            int count = kv.second;
            ASSERT_LT(fabs(count - expectedMessagesPerConsumer), expectedMessagesPerConsumer * PERCENT_ERROR);
        }
    }

    Client client;
    std::vector<Producer> producers;
    std::vector<Consumer> consumers;
    const std::string subName = "SubscriptionName";
};

TEST_F(KeySharedConsumerTest, testNonPartitionedTopic) {
    const std::string topicName = "KeySharedConsumerTest-non-par-topic" + std::to_string(time(nullptr));

    addProducer(topicName);
    for (int i = 0; i < 3; i++) {
        addConsumer(topicName);
    }

    srand(time(nullptr));
    for (int i = 0; i < 1000; i++) {
        std::string key = std::to_string(rand() % NUMBER_OF_KEYS);
        producers[0].sendAsync(newIntMessage(i, key), sendCallback);
    }
    ASSERT_EQ(ResultOk, producers[0].flush());

    receiveAndCheckDistribution();
}

TEST_F(KeySharedConsumerTest, testMultiTopics) {
    const std::string topicNamePrefix = "KeySharedConsumerTest-multi-topics" + std::to_string(time(nullptr));

    for (int i = 0; i < 3; i++) {
        addProducer(topicNamePrefix + std::to_string(i));
    }
    for (int i = 0; i < 3; i++) {
        addRegexConsumer(".*" + topicNamePrefix + ".*");
    }

    srand(time(nullptr));
    for (auto& producer : producers) {
        for (int i = 0; i < 1000; i++) {
            std::string key = std::to_string(rand() % NUMBER_OF_KEYS);
            producer.sendAsync(newIntMessage(i, key), sendCallback);
        }
        ASSERT_EQ(ResultOk, producer.flush());
    }

    receiveAndCheckDistribution();
}

TEST_F(KeySharedConsumerTest, testOrderingKeyPriority) {
    const std::string topicName =
        "KeySharedConsumerTest-ordering-key-priority" + std::to_string(time(nullptr));

    addProducer(topicName);
    for (int i = 0; i < 3; i++) {
        addConsumer(topicName);
    }

    srand(time(nullptr));
    for (int i = 0; i < 1000; i++) {
        int randomInt = rand();
        std::string key = std::to_string(randomInt % NUMBER_OF_KEYS);
        std::string orderingKey = std::to_string((randomInt + 1) % NUMBER_OF_KEYS);
        producers[0].sendAsync(newIntMessage(i, key, orderingKey.c_str()), sendCallback);
    }
    ASSERT_EQ(ResultOk, producers[0].flush());

    receiveAndCheckDistribution();
}
