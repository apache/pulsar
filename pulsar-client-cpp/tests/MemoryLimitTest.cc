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
#include <array>
#include <thread>

#include "../lib/MemoryLimitController.h"
#include "../lib/Latch.h"
#include "../lib/Future.h"
#include "../lib/Utils.h"

#include <pulsar/Client.h>

using namespace pulsar;

extern std::string lookupUrl;
extern std::string unique_str();

TEST(MemoryLimitTest, testRejectMessages) {
    std::string topic = "topic-" + unique_str();

    ClientConfiguration config;
    config.setMemoryLimit(100 * 1024);
    Client client(lookupUrl, config);

    ProducerConfiguration producerConf;
    producerConf.setBlockIfQueueFull(false);
    Producer producer;
    Result res = client.createProducer(topic, producerConf, producer);
    ASSERT_EQ(res, ResultOk);

    const int n = 101;
    Latch latch(n);

    std::array<char, 1024> buffer;

    for (int i = 0; i < n; i++) {
        producer.sendAsync(MessageBuilder().setContent(buffer.data(), buffer.size()).build(),
                           [&](Result res, const MessageId& msgId) {
                               ASSERT_EQ(res, ResultOk);
                               latch.countdown();
                           });
    }

    res = producer.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultMemoryBufferIsFull);

    latch.wait();

    // We should now be able to send again
    res = producer.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultOk);
}

TEST(MemoryLimitTest, testRejectMessagesOnMultipleTopics) {
    std::string t1 = "topic-1-" + unique_str();
    std::string t2 = "topic-2-" + unique_str();

    ClientConfiguration config;
    config.setMemoryLimit(100 * 1024);
    Client client(lookupUrl, config);

    ProducerConfiguration producerConf;
    producerConf.setBlockIfQueueFull(false);
    producerConf.setBatchingMaxPublishDelayMs(10000);

    Producer p1;
    Result res = client.createProducer(t1, producerConf, p1);
    ASSERT_EQ(res, ResultOk);

    Producer p2;
    res = client.createProducer(t2, producerConf, p2);
    ASSERT_EQ(res, ResultOk);

    const int n = 101;
    Latch latch(n);

    std::array<char, 1024> buffer;

    for (int i = 0; i < n / 2; i++) {
        p1.sendAsync(MessageBuilder().setContent(buffer.data(), buffer.size()).build(),
                     [&](Result res, const MessageId& msgId) {
                         ASSERT_EQ(res, ResultOk);
                         latch.countdown();
                     });

        p2.sendAsync(MessageBuilder().setContent(buffer.data(), buffer.size()).build(),
                     [&](Result res, const MessageId& msgId) {
                         ASSERT_EQ(res, ResultOk);
                         latch.countdown();
                     });
    }

    // Last message in order to reach the limit
    p1.sendAsync(MessageBuilder().setContent(buffer.data(), buffer.size()).build(),
                 [&](Result res, const MessageId& msgId) {
                     ASSERT_EQ(res, ResultOk);
                     latch.countdown();
                 });

    res = p1.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultMemoryBufferIsFull);

    res = p2.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultMemoryBufferIsFull);

    latch.wait();

    // We should now be able to send again
    res = p1.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultOk);

    res = p2.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
    ASSERT_EQ(res, ResultOk);
}

TEST(MemoryLimitTest, testNoProducerQueueSize) {
    std::string topic = "topic-" + unique_str();

    ClientConfiguration config;
    config.setMemoryLimit(10 * 1024);
    Client client(lookupUrl, config);

    ProducerConfiguration producerConf;
    producerConf.setBlockIfQueueFull(true);
    producerConf.setMaxPendingMessages(0);
    producerConf.setMaxPendingMessagesAcrossPartitions(0);
    Producer producer;
    Result res = client.createProducer(topic, producerConf, producer);
    ASSERT_EQ(res, ResultOk);

    std::array<Promise<Result, MessageId>, 100> promises;

    for (int i = 0; i < 100; i++) {
        producer.sendAsync(MessageBuilder().setContent("hello").build(),
                           WaitForCallbackValue<MessageId>(promises[i]));
    }

    producer.flush();

    for (auto& p : promises) {
        MessageId id;
        Result res = p.getFuture().get(id);
        ASSERT_EQ(res, ResultOk);
    }
}