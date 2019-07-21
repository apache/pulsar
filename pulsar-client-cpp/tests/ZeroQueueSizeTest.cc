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
#include <lib/Latch.h>
#include "ConsumerTest.h"
#include <functional>

DECLARE_LOG_OBJECT()

using namespace pulsar;

static int totalMessages = 10;
static int globalCount = 0;
static std::string lookupUrl = "pulsar://localhost:6650";
static std::string contentBase = "msg-";

static void messageListenerFunction(Consumer consumer, const Message& msg, Latch& latch) {
    ASSERT_EQ(0, ConsumerTest::getNumOfMessagesInQueue(consumer));
    std::ostringstream ss;
    ss << contentBase << globalCount;
    ASSERT_EQ(ss.str(), msg.getDataAsString());
    globalCount++;
    latch.countdown();
    ASSERT_EQ(0, ConsumerTest::getNumOfMessagesInQueue(consumer));
}

TEST(ZeroQueueSizeTest, testProduceConsume) {
    Client client(lookupUrl);
    std::string topicName = "zero-queue-size";
    std::string subName = "my-sub-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setReceiverQueueSize(0);
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);

    for (int i = 0; i < totalMessages; i++) {
        std::ostringstream ss;
        ss << contentBase << i;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        result = producer.send(msg);
        ASSERT_EQ(ResultOk, result);
    }

    for (int i = 0; i < totalMessages; i++) {
        ASSERT_EQ(0, ConsumerTest::getNumOfMessagesInQueue(consumer));
        std::ostringstream ss;
        ss << contentBase << i;
        Message receivedMsg;
        consumer.receive(receivedMsg);
        ASSERT_EQ(ss.str(), receivedMsg.getDataAsString());
        ASSERT_EQ(0, ConsumerTest::getNumOfMessagesInQueue(consumer));
    }

    consumer.unsubscribe();
    consumer.close();
    producer.close();
    client.close();
}

TEST(ZeroQueueSizeTest, testMessageListener) {
    Client client(lookupUrl);
    std::string topicName = "zero-queue-size-listener";
    std::string subName = "my-sub-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setReceiverQueueSize(0);
    Latch latch(totalMessages);
    consConfig.setMessageListener(
        std::bind(messageListenerFunction, std::placeholders::_1, std::placeholders::_2, latch));
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);

    globalCount = 0;

    for (int i = 0; i < totalMessages; i++) {
        std::ostringstream ss;
        ss << contentBase << i;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        result = producer.send(msg);
        ASSERT_EQ(ResultOk, result);
    }

    ASSERT_TRUE(latch.wait(std::chrono::seconds(30)));
    ASSERT_EQ(globalCount, totalMessages);

    consumer.unsubscribe();
    consumer.close();
    producer.close();
    client.close();
}
