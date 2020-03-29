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

#include <gtest/gtest.h>

#include "HttpHelper.h"

#include <string>
#include <thread>

using namespace pulsar;

static std::string serviceUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

TEST(ClientDeduplicationTest, testProducerSequenceAfterReconnect) {
    Client client(serviceUrl);

    std::string topicName =
        "persistent://public/dedup-1/testProducerSequenceAfterReconnect-" + std::to_string(time(NULL));

    // call admin api to create namespace and enable deduplication
    std::string url = adminUrl + "admin/v2/namespaces/public/dedup-1";
    int res = makePutRequest(url, R"({"replication_clusters": ["standalone"]})");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/v2/namespaces/public/dedup-1/permissions/anonymous";
    res = makePostRequest(url, R"(["produce","consume"])");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/v2/namespaces/public/dedup-1/deduplication";
    res = makePostRequest(url, "true");
    ASSERT_TRUE(res == 204 || res == 409);

    // Ensure dedup status was refreshed
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(client.createReader(topicName, MessageId::earliest(), readerConf, reader), ResultOk);

    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setProducerName("my-producer-name");
    ASSERT_EQ(client.createProducer(topicName, producerConf, producer), ResultOk);

    ASSERT_EQ(producer.getLastSequenceId(), -1L);

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(producer.send(msg), ResultOk);
        ASSERT_EQ(producer.getLastSequenceId(), i);
    }

    producer.close();

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));
    ASSERT_EQ(producer.getLastSequenceId(), 9);

    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(producer.send(msg), ResultOk);
        ASSERT_EQ(producer.getLastSequenceId(), i);
    }

    client.close();
}

TEST(ClientDeduplicationTest, testProducerDeduplication) {
    Client client(adminUrl);

    std::string topicName =
        "persistent://public/dedup-2/testProducerDeduplication-" + std::to_string(time(NULL));

    std::string url = adminUrl + "admin/v2/namespaces/public/dedup-2";
    int res = makePutRequest(url, R"({"replication_clusters": ["standalone"]})");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/v2/namespaces/public/dedup-2/permissions/anonymous";
    res = makePostRequest(url, R"(["produce","consume"])");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/v2/namespaces/public/dedup-2/deduplication";
    res = makePostRequest(url, "true");
    ASSERT_TRUE(res == 204 || res == 409);

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(client.createReader(topicName, MessageId::earliest(), readerConf, reader), ResultOk);

    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setProducerName("my-producer-name");
    ASSERT_EQ(client.createProducer(topicName, producerConf, producer), ResultOk);

    ASSERT_EQ(producer.getLastSequenceId(), -1L);

    Consumer consumer;
    ASSERT_EQ(client.subscribe(topicName, "my-subscription", consumer), ResultOk);

    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-0").setSequenceId(0).build()), ResultOk);
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-1").setSequenceId(1).build()), ResultOk);
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-2").setSequenceId(2).build()), ResultOk);

    // Repeat the messages and verify they're not received by consumer
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-1").setSequenceId(1).build()), ResultOk);
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-2").setSequenceId(2).build()), ResultOk);

    producer.close();

    Message msg;
    for (int i = 0; i < 3; i++) {
        consumer.receive(msg);

        ASSERT_EQ(msg.getDataAsString(), "my-message-" + std::to_string(i));
        consumer.acknowledge(msg);
    }

    // No other messages should be received
    ASSERT_EQ(consumer.receive(msg, 1000), ResultTimeout);

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));
    ASSERT_EQ(producer.getLastSequenceId(), 2);

    // Repeat the messages and verify they're not received by consumer
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-1").setSequenceId(1).build()), ResultOk);
    ASSERT_EQ(producer.send(MessageBuilder().setContent("my-message-2").setSequenceId(2).build()), ResultOk);

    // No other messages should be received
    ASSERT_EQ(consumer.receive(msg, 1000), ResultTimeout);

    client.close();
}
