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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

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

static ConsumerConfiguration zeroQueueSharedConsumerConf(
    const std::string& name, std::function<void(Consumer, const Message&)> callback) {
    ConsumerConfiguration conf;
    conf.setConsumerType(ConsumerShared);
    conf.setReceiverQueueSize(0);
    conf.setSubscriptionInitialPosition(InitialPositionEarliest);
    conf.setMessageListener([name, callback](Consumer consumer, const Message& msg) {
        LOG_INFO(name << " received " << msg.getDataAsString() << " from " << msg.getMessageId());
        callback(consumer, msg);
    });
    return conf;
}

class IntVector {
   public:
    size_t add(int i) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.emplace_back(i);
        return data_.size();
    }

    std::vector<int> data() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_;
    }

   private:
    std::vector<int> data_;
    mutable std::mutex mutex_;
};

TEST(ZeroQueueSizeTest, testPauseResume) {
    Client client(lookupUrl);
    const auto topic = "ZeroQueueSizeTestPauseListener-" + std::to_string(time(nullptr));
    const auto subscription = "my-sub";

    auto intToMessage = [](int i) { return MessageBuilder().setContent(std::to_string(i)).build(); };
    auto messageToInt = [](const Message& msg) { return std::stoi(msg.getDataAsString()); };

    // 1. Produce 10 messages
    Producer producer;
    const auto producerConf = ProducerConfiguration().setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf, producer));
    for (int i = 0; i < 10; i++) {
        MessageId id;
        ASSERT_EQ(ResultOk, producer.send(intToMessage(i), id));
        LOG_INFO("Send " << i << " to " << id);
    }

    // 2. consumer-1 receives 1 message and pause
    std::mutex mtx;
    std::condition_variable condConsumer1FirstMessage;
    std::condition_variable condConsumer1Completed;
    IntVector messages1;
    const auto conf1 = zeroQueueSharedConsumerConf("consumer-1", [&](Consumer consumer, const Message& msg) {
        const auto numReceived = messages1.add(messageToInt(msg));
        if (numReceived == 1) {
            ASSERT_EQ(ResultOk, consumer.pauseMessageListener());
            condConsumer1FirstMessage.notify_all();
        } else if (numReceived == 5) {
            ASSERT_EQ(ResultOk, consumer.pauseMessageListener());
            condConsumer1Completed.notify_all();
        }
    });
    Consumer consumer1;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subscription, conf1, consumer1));
    {
        std::unique_lock<std::mutex> lock(mtx);
        ASSERT_EQ(condConsumer1FirstMessage.wait_for(lock, std::chrono::seconds(3)),
                  std::cv_status::no_timeout);
        ASSERT_EQ(messages1.data(), (std::vector<int>{0}));
    }

    // 3. consumer-2 receives 5 messages and pause
    std::condition_variable condConsumer2Completed;
    IntVector messages2;
    const auto conf2 = zeroQueueSharedConsumerConf("consumer-2", [&](Consumer consumer, const Message& msg) {
        const int numReceived = messages2.add(messageToInt(msg));
        if (numReceived == 5) {
            ASSERT_EQ(ResultOk, consumer.pauseMessageListener());
            condConsumer2Completed.notify_all();
        }
    });
    Consumer consumer2;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subscription, conf2, consumer2));
    {
        std::unique_lock<std::mutex> lock(mtx);
        ASSERT_EQ(condConsumer2Completed.wait_for(lock, std::chrono::seconds(3)), std::cv_status::no_timeout);
        ASSERT_EQ(messages2.data(), (std::vector<int>{1, 2, 3, 4, 5}));
    }

    // 4. consumer-1 resumes listening, and receives last 4 messages
    ASSERT_EQ(ResultOk, consumer1.resumeMessageListener());
    {
        std::unique_lock<std::mutex> lock(mtx);
        ASSERT_EQ(condConsumer1Completed.wait_for(lock, std::chrono::seconds(3)), std::cv_status::no_timeout);
        ASSERT_EQ(messages1.data(), (std::vector<int>{0, 6, 7, 8, 9}));
    }

    client.close();
}

TEST(ZeroQueueSizeTest, testPauseResumeNoReconnection) {
    Client client(lookupUrl);
    const auto topic = "ZeroQueueSizeTestPauseResumeNoReconnection-" + std::to_string(time(nullptr));

    std::mutex mtx;
    std::condition_variable cond;
    bool running = true;

    auto notify = [&mtx, &cond, &running] {
        std::unique_lock<std::mutex> lock(mtx);
        running = false;
        cond.notify_all();
    };
    auto wait = [&mtx, &cond, &running] {
        std::unique_lock<std::mutex> lock(mtx);
        running = true;
        while (running) {
            cond.wait(lock);
        }
    };

    std::mutex mtxForMessages;
    std::vector<std::string> receivedMessages;

    ConsumerConfiguration consumerConf;
    consumerConf.setReceiverQueueSize(0);
    consumerConf.setMessageListener(
        [&mtxForMessages, &receivedMessages, &notify](Consumer consumer, const Message& msg) {
            std::unique_lock<std::mutex> lock(mtxForMessages);
            receivedMessages.emplace_back(msg.getDataAsString());
            lock.unlock();
            consumer.acknowledge(msg);
            notify();  // notify the consumer that a new message arrived
        });

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "my-sub", consumerConf, consumer));

    Producer producer;
    ASSERT_EQ(ResultOk,
              client.createProducer(topic, ProducerConfiguration().setBatchingEnabled(false), producer));

    constexpr int numMessages = 300;
    for (int i = 0; i < numMessages; i++) {
        const auto message = MessageBuilder().setContent(std::to_string(i)).build();
        consumer.resumeMessageListener();
        producer.sendAsync(message, {});
        wait();  // wait until a new message is received
        consumer.pauseMessageListener();
    }

    std::unique_lock<std::mutex> lock(mtxForMessages);
    ASSERT_EQ(receivedMessages.size(), numMessages);
    for (int i = 0; i < numMessages; i++) {
        ASSERT_EQ(i, std::stoi(receivedMessages[i]));
    }
    lock.unlock();

    client.close();
}
