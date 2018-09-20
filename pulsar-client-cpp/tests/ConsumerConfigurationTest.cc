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

#include "../lib/Future.h"
#include "../lib/Utils.h"

using namespace pulsar;

TEST(ConsumerConfigurationTest, testReadCompactPersistentExclusive) {
    std::string lookupUrl = "pulsar://localhost:8885";
    std::string topicName = "persistent://prop/unit/ns1/persist-topic";
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
    std::string lookupUrl = "pulsar://localhost:8885";
    std::string topicName = "persistent://prop/unit/ns1/persist-topic";
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

TEST(ConsumerConfigurationTest, testReadCompactPersistentShared) {
    std::string lookupUrl = "pulsar://localhost:8885";
    std::string topicName = "persistent://prop/unit/ns1/persist-topic";
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
    std::string lookupUrl = "pulsar://localhost:8885";
    std::string topicName = "non-persistent://prop/unit/ns1/testNonPersistentTopic";
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