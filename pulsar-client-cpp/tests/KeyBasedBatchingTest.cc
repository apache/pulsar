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
#include <time.h>
#include <atomic>
#include <map>
#include <utility>

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <lib/Latch.h>

using namespace pulsar;

static ProducerConfiguration createDefaultProducerConfig() {
    // The default producer configuration only use number of messages to limit batching
    return ProducerConfiguration()
        .setBatchingType(ProducerConfiguration::KeyBasedBatching)
        .setBatchingMaxAllowedSizeInBytes(static_cast<unsigned long>(-1))
        .setBatchingMaxPublishDelayMs(3600 * 1000);
}

class KeyBasedBatchingTest : public ::testing::Test {
   protected:
    KeyBasedBatchingTest() : client_("pulsar://localhost:6650") {}

    void TearDown() override { client_.close(); }

    void initTopicName(const std::string& testName) {
        topicName_ = "KeyBasedBatchingTest-" + testName + "-" + std::to_string(time(nullptr));
    }

    void initProducer(const ProducerConfiguration& producerConfig) {
        ASSERT_EQ(ResultOk, client_.createProducer(topicName_, producerConfig, producer_));
    }

    void initConsumer() { ASSERT_EQ(ResultOk, client_.subscribe(topicName_, "SubscriptionName", consumer_)); }

    void receiveAndAck(Message& msg) {
        ASSERT_EQ(ResultOk, consumer_.receive(msg, 3000));
        ASSERT_EQ(ResultOk, consumer_.acknowledge(msg));
    }

    Client client_;
    Producer producer_;
    Consumer consumer_;
    std::string topicName_;
};

TEST_F(KeyBasedBatchingTest, testFlush) {
    initTopicName("Flush");
    // no limits for batching
    initProducer(createDefaultProducerConfig().setBatchingMaxMessages(
        static_cast<unsigned int>(-1))  // no limits for batching
    );

    constexpr int numMessages = 100;
    const std::string keys[] = {"A", "B"};
    std::atomic_int numMessageSent{0};
    for (int i = 0; i < numMessages; i++) {
        producer_.sendAsync(MessageBuilder().setOrderingKey(keys[i % 2]).setContent("x").build(),
                            [&numMessageSent](Result result, const MessageId&) {
                                numMessageSent++;
                                ASSERT_EQ(result, ResultOk);
                            });
    }

    ASSERT_EQ(ResultOk, producer_.flush());
    ASSERT_EQ(numMessageSent.load(), numMessages);
}

TEST_F(KeyBasedBatchingTest, testOrderingKeyPriority) {
    initTopicName("OrderingKeyPriority");
    initProducer(createDefaultProducerConfig().setBatchingMaxMessages(3));
    initConsumer();

    Latch latch(3);
    auto sendCallback = [&latch](Result result, const MessageId& id) {
        ASSERT_EQ(result, ResultOk);
        latch.countdown();
    };
    // "0" is send to batch of "A" because ordering key has higher priority
    producer_.sendAsync(MessageBuilder().setContent("0").setOrderingKey("A").setPartitionKey("B").build(),
                        sendCallback);
    producer_.sendAsync(MessageBuilder().setContent("1").setOrderingKey("A").build(), sendCallback);
    producer_.sendAsync(MessageBuilder().setContent("2").setOrderingKey("B").build(), sendCallback);
    latch.countdown();

    Message msg;
    receiveAndAck(msg);
    ASSERT_EQ("0", msg.getDataAsString());
    ASSERT_EQ("A", msg.getOrderingKey());
    ASSERT_EQ("B", msg.getPartitionKey());
    receiveAndAck(msg);
    ASSERT_EQ("1", msg.getDataAsString());
    ASSERT_EQ("A", msg.getOrderingKey());
    receiveAndAck(msg);
    ASSERT_EQ("2", msg.getDataAsString());
    ASSERT_EQ("B", msg.getOrderingKey());
}

TEST_F(KeyBasedBatchingTest, testSequenceId) {
    initTopicName("SequenceId");
    initProducer(createDefaultProducerConfig().setBatchingMaxMessages(6));
    initConsumer();

    Latch latch(6);
    auto sendAsync = [this, &latch](const std::string& key, const std::string& value) {
        producer_.sendAsync(MessageBuilder().setOrderingKey(key).setContent(value).build(),
                            [&latch](Result result, const MessageId& id) {
                                ASSERT_EQ(result, ResultOk);
                                latch.countdown();
                            });
    };
    sendAsync("A", "0");
    sendAsync("B", "1");
    sendAsync("C", "2");
    sendAsync("B", "3");
    sendAsync("C", "4");
    sendAsync("A", "5");
    // sequence id: B < C < A, so there are 3 batches in order as following:
    //   B: 1, 3
    //   C: 2, 4
    //   A: 0, 5
    latch.wait();

    std::vector<std::string> receivedKeys;
    std::vector<std::string> receivedValues;
    for (int i = 0; i < 6; i++) {
        Message msg;
        receiveAndAck(msg);
        receivedKeys.emplace_back(msg.getOrderingKey());
        receivedValues.emplace_back(msg.getDataAsString());
    }

    decltype(receivedKeys) expectedKeys{"B", "B", "C", "C", "A", "A"};
    decltype(receivedValues) expectedValues{"1", "3", "2", "4", "0", "5"};
    EXPECT_EQ(receivedKeys, expectedKeys);
    EXPECT_EQ(receivedValues, expectedValues);
}

TEST_F(KeyBasedBatchingTest, testSingleBatch) {
    initTopicName("SingleBatch");
    initProducer(createDefaultProducerConfig().setBatchingMaxMessages(5));
    initConsumer();

    constexpr int numMessages = 5 * 100;
    std::atomic_int numMessageSent{0};
    // messages with no key are packed to the same batch and this batch has no key
    // the broker uses `NON_KEY` as the key when dispatching messages from this batch
    for (int i = 0; i < numMessages; i++) {
        producer_.sendAsync(MessageBuilder().setContent("x").build(),
                            [&numMessageSent](Result result, const MessageId&) {
                                ASSERT_EQ(result, ResultOk);
                                ++numMessageSent;
                            });
    }

    Message msg;
    for (int i = 0; i < numMessages; i++) {
        receiveAndAck(msg);
    }
    ASSERT_EQ(ResultTimeout, consumer_.receive(msg, 3000));
    ASSERT_EQ(numMessageSent.load(), numMessages);
}

TEST_F(KeyBasedBatchingTest, testCloseBeforeSend) {
    initTopicName("CloseBeforeSend");
    // Any asynchronous send won't be completed unless `close()` or `flush()` is triggered
    initProducer(createDefaultProducerConfig().setBatchingMaxMessages(static_cast<unsigned>(-1)));

    std::mutex mtx;
    std::vector<Result> results;
    auto saveResult = [&mtx, &results](Result result) {
        std::lock_guard<std::mutex> lock(mtx);
        results.emplace_back(result);
    };
    auto sendAsync = [saveResult, this](const std::string& key, const std::string& value) {
        producer_.sendAsync(MessageBuilder().setOrderingKey(key).setContent(value).build(),
                            [saveResult](Result result, const MessageId& id) { saveResult(result); });
    };

    constexpr int numKeys = 10;
    for (int i = 0; i < numKeys; i++) {
        sendAsync("key-" + std::to_string(i), "value");
    }

    ASSERT_EQ(ResultOk, producer_.close());

    // After close() completed, all callbacks should have failed with ResultAlreadyClosed
    std::lock_guard<std::mutex> lock(mtx);
    ASSERT_EQ(results.size(), numKeys);
    for (int i = 0; i < numKeys; i++) {
        ASSERT_EQ(results[i], ResultAlreadyClosed) << " results[" << i << "] is " << results[i];
    }
}
