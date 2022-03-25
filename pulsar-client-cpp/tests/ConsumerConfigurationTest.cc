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
#include <lib/LogUtils.h>
#include "NoOpsCryptoKeyReader.h"

DECLARE_LOG_OBJECT()

#include "../lib/Future.h"
#include "../lib/Utils.h"

using namespace pulsar;

class DummyEventListener : public ConsumerEventListener {
   public:
    virtual void becameActive(Consumer consumer, int partitionId) override {}

    virtual void becameInactive(Consumer consumer, int partitionId) override {}
};

TEST(ConsumerConfigurationTest, testDefaultConfig) {
    ConsumerConfiguration conf;
    ASSERT_EQ(conf.getSchema().getSchemaType(), SchemaType::BYTES);
    ASSERT_EQ(conf.getConsumerType(), ConsumerExclusive);
    ASSERT_EQ(conf.hasMessageListener(), false);
    ASSERT_EQ(conf.hasConsumerEventListener(), false);
    ASSERT_EQ(conf.getReceiverQueueSize(), 1000);
    ASSERT_EQ(conf.getMaxTotalReceiverQueueSizeAcrossPartitions(), 50000);
    ASSERT_EQ(conf.getConsumerName(), "");
    ASSERT_EQ(conf.getUnAckedMessagesTimeoutMs(), 0);
    ASSERT_EQ(conf.getTickDurationInMs(), 1000);
    ASSERT_EQ(conf.getNegativeAckRedeliveryDelayMs(), 60000);
    ASSERT_EQ(conf.getAckGroupingTimeMs(), 100);
    ASSERT_EQ(conf.getAckGroupingMaxSize(), 1000);
    ASSERT_EQ(conf.getBrokerConsumerStatsCacheTimeInMs(), 30000);
    ASSERT_EQ(conf.isReadCompacted(), false);
    ASSERT_EQ(conf.getPatternAutoDiscoveryPeriod(), 60);
    ASSERT_EQ(conf.getSubscriptionInitialPosition(), InitialPositionLatest);
    ASSERT_EQ(conf.getCryptoKeyReader(), CryptoKeyReaderPtr{});
    ASSERT_EQ(conf.getCryptoFailureAction(), ConsumerCryptoFailureAction::FAIL);
    ASSERT_EQ(conf.isEncryptionEnabled(), false);
    ASSERT_EQ(conf.isReplicateSubscriptionStateEnabled(), false);
    ASSERT_EQ(conf.getProperties().empty(), true);
    ASSERT_EQ(conf.getPriorityLevel(), 0);
    ASSERT_EQ(conf.getMaxPendingChunkedMessage(), 10);
    ASSERT_EQ(conf.isAutoAckOldestChunkedMessageOnQueueFull(), false);
}

TEST(ConsumerConfigurationTest, testCustomConfig) {
    ConsumerConfiguration conf;

    const std::string exampleSchema =
        "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
        "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
    const SchemaInfo schema(AVRO, "Avro", exampleSchema, StringMap{{"schema-key", "schema-value"}});

    conf.setSchema(schema);
    ASSERT_EQ(conf.getSchema().getName(), schema.getName());
    ASSERT_EQ(conf.getSchema().getSchemaType(), schema.getSchemaType());
    ASSERT_EQ(conf.getSchema().getSchema(), schema.getSchema());
    ASSERT_EQ(conf.getSchema().getProperties(), schema.getProperties());

    conf.setConsumerType(ConsumerKeyShared);
    ASSERT_EQ(conf.getConsumerType(), ConsumerKeyShared);

    conf.setMessageListener([](Consumer consumer, const Message& msg) {});
    ASSERT_EQ(conf.hasMessageListener(), true);

    conf.setConsumerEventListener(std::make_shared<DummyEventListener>());
    ASSERT_EQ(conf.hasConsumerEventListener(), true);

    conf.setReceiverQueueSize(2000);
    ASSERT_EQ(conf.getReceiverQueueSize(), 2000);

    conf.setMaxTotalReceiverQueueSizeAcrossPartitions(100000);
    ASSERT_EQ(conf.getMaxTotalReceiverQueueSizeAcrossPartitions(), 100000);

    conf.setConsumerName("consumer");
    ASSERT_EQ(conf.getConsumerName(), "consumer");

    conf.setUnAckedMessagesTimeoutMs(20000);
    ASSERT_EQ(conf.getUnAckedMessagesTimeoutMs(), 20000);

    conf.setTickDurationInMs(2000);
    ASSERT_EQ(conf.getTickDurationInMs(), 2000);

    conf.setNegativeAckRedeliveryDelayMs(10000);
    ASSERT_EQ(conf.getNegativeAckRedeliveryDelayMs(), 10000);

    conf.setAckGroupingTimeMs(200);
    ASSERT_EQ(conf.getAckGroupingTimeMs(), 200);

    conf.setAckGroupingMaxSize(2000);
    ASSERT_EQ(conf.getAckGroupingMaxSize(), 2000);

    conf.setBrokerConsumerStatsCacheTimeInMs(60000);
    ASSERT_EQ(conf.getBrokerConsumerStatsCacheTimeInMs(), 60000);

    conf.setReadCompacted(true);
    ASSERT_EQ(conf.isReadCompacted(), true);

    conf.setPatternAutoDiscoveryPeriod(120);
    ASSERT_EQ(conf.getPatternAutoDiscoveryPeriod(), 120);

    conf.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(conf.getSubscriptionInitialPosition(), InitialPositionEarliest);

    const auto cryptoKeyReader = std::make_shared<NoOpsCryptoKeyReader>();
    conf.setCryptoKeyReader(cryptoKeyReader);
    ASSERT_EQ(conf.getCryptoKeyReader(), cryptoKeyReader);
    // NOTE: once CryptoKeyReader was set, the isEncryptionEnabled() would return true, it's different from
    // ProducerConfiguration
    ASSERT_EQ(conf.isEncryptionEnabled(), true);

    conf.setCryptoFailureAction(ConsumerCryptoFailureAction::CONSUME);
    ASSERT_EQ(conf.getCryptoFailureAction(), ConsumerCryptoFailureAction::CONSUME);

    conf.setReplicateSubscriptionStateEnabled(true);
    ASSERT_EQ(conf.isReplicateSubscriptionStateEnabled(), true);

    conf.setProperty("k1", "v1");
    ASSERT_EQ(conf.getProperties()["k1"], "v1");
    ASSERT_EQ(conf.hasProperty("k1"), true);

    conf.setPriorityLevel(1);
    ASSERT_EQ(conf.getPriorityLevel(), 1);

    conf.setMaxPendingChunkedMessage(500);
    ASSERT_EQ(conf.getMaxPendingChunkedMessage(), 500);

    conf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    ASSERT_TRUE(conf.isAutoAckOldestChunkedMessageOnQueueFull());
}

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

TEST(ConsumerConfigurationTest, testResetAckTimeOut) {
    const uint64_t milliSeconds = 50000;
    ConsumerConfiguration config;
    config.setUnAckedMessagesTimeoutMs(milliSeconds);
    ASSERT_EQ(milliSeconds, config.getUnAckedMessagesTimeoutMs());

    // should be able to set it back to 0.
    config.setUnAckedMessagesTimeoutMs(0);
    ASSERT_EQ(0, config.getUnAckedMessagesTimeoutMs());
}
