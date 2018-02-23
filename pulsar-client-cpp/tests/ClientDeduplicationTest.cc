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
#include <boost/lexical_cast.hpp>

#include "HttpHelper.h"

#include <string>

using namespace pulsar;

static std::string serviceUrl = "pulsar://localhost:8885";
static std::string adminUrl = "http://localhost:8765/";

TEST(ClientDeduplicationTest, testProducerSequenceAfterReconnect) {
    Client client(serviceUrl);

    std::string topicName = "persistent://sample/standalone/ns-dedup-1/testProducerSequenceAfterReconnect-" +
                            boost::lexical_cast<std::string>(time(NULL));

    // call admin api to create namespace and enable deduplication
    std::string url = adminUrl + "admin/namespaces/sample/standalone/ns-dedup-1";
    int res = makePutRequest(url, "");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/namespaces/sample/standalone/ns-dedup-1/deduplication";
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

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(producer.send(msg), ResultOk);
        ASSERT_EQ(producer.getLastSequenceId(), i);
    }

    producer.close();

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));
    ASSERT_EQ(producer.getLastSequenceId(), 9);

    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(producer.send(msg), ResultOk);
        ASSERT_EQ(producer.getLastSequenceId(), i);
    }

    client.close();
}

TEST(ClientDeduplicationTest, testProducerDeduplication) {
    Client client(serviceUrl);

    std::string topicName = "persistent://sample/standalone/ns-dedup-2/testProducerDeduplication-" +
                            boost::lexical_cast<std::string>(time(NULL));

    // call admin api to create namespace and enable deduplication
    std::string url = adminUrl + "admin/namespaces/sample/standalone/ns-dedup-2";
    int res = makePutRequest(url, "");
    ASSERT_TRUE(res == 204 || res == 409);

    url = adminUrl + "admin/namespaces/sample/standalone/ns-dedup-2/deduplication";
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

        ASSERT_EQ(msg.getDataAsString(), "my-message-" + boost::lexical_cast<std::string>(i));
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
