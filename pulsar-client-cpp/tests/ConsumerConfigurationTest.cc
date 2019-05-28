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
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/bind.hpp>
#include <lib/LogUtils.h>

DECLARE_LOG_OBJECT()

#include "../lib/Future.h"
#include "../lib/Utils.h"

using namespace pulsar;

TEST(ConsumerConfigurationTest, testReadCompactPersistentExclusive) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "persist-topic";
    std::string subName = "test-persist-exclusive";

    Result result;

    ConsumerConfiguration config;
    config.setReadCompacted(true);
    config.setConsumerType(ConsumerExclusive);

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    Consumer consumer;
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultOk, result);
    consumer.close();
}

TEST(ConsumerConfigurationTest, testReadCompactPersistentFailover) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "persist-topic";
    std::string subName = "test-persist-fail-over";

    Result result;

    ConsumerConfiguration config;
    config.setReadCompacted(true);
    config.setConsumerType(ConsumerFailover);

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    Consumer consumer;
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultOk, result);
    consumer.close();
}

TEST(ConsumerConfigurationTest, testSubscribePersistentKeyShared) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "persist-key-shared-topic";
    std::string subName = "test-persist-key-shared";

    Result result;

    ConsumerConfiguration config;
    // now, key-shared not support read compact
    config.setReadCompacted(false);
    config.setConsumerType(ConsumerKeyShared);

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    Consumer consumer;
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultOk, result);
    consumer.close();
}

TEST(ConsumerConfigurationTest, testReadCompactPersistentShared) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "persist-topic";
    std::string subName = "test-persist-shared";

    Result result;

    ConsumerConfiguration config;
    config.setReadCompacted(true);
    config.setConsumerType(ConsumerShared);

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    Consumer consumer;
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultInvalidConfiguration, result);
    consumer.close();
}

TEST(ConsumerConfigurationTest, testReadCompactNonPersistentExclusive) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "non-persistent://public/default/testNonPersistentTopic";
    std::string subName = "test-non-persist-exclusive";

    Result result;

    ConsumerConfiguration config;
    config.setReadCompacted(true);
    config.setConsumerType(ConsumerExclusive);

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    Consumer consumer;
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultInvalidConfiguration, result);
    consumer.close();
}

TEST(ConsumerConfigurationTest, testSubscriptionInitialPosition) {
    std::string lookupUrl = "pulsar://localhost:6650";
    std::string topicName = "persist-topic-test-position";
    std::string subName = "test-subscription-initial-earliest-position";

    ClientConfiguration clientConfig;
    Client client(lookupUrl, clientConfig);

    LOG_INFO("create 1 producer...");
    Producer producer;
    Result result;
    ProducerConfiguration conf;
    result = client.createProducer(topicName, conf, producer);
    ASSERT_EQ(ResultOk, result);

    // Send synchronously
    std::string content1 = "msg-1-content-1";
    Message msg = MessageBuilder().setContent(content1).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    std::string content2 = "msg-2-content-2";
    msg = MessageBuilder().setContent(content2).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration config;
    config.setSubscriptionInitialPosition(InitialPosition::InitialPositionEarliest);
    result = client.subscribe(topicName, subName, config, consumer);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg;

    result = consumer.receive(receivedMsg, 2000);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(content1, receivedMsg.getDataAsString());

    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}
