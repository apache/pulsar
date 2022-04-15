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
#include <pulsar/ProducerConfiguration.h>
#include "NoOpsCryptoKeyReader.h"

using namespace pulsar;

TEST(ProducerConfigurationTest, testDefaultConfig) {
    ProducerConfiguration conf;
    ASSERT_EQ(conf.getProducerName(), "");
    ASSERT_EQ(conf.getSchema().getName(), "BYTES");
    ASSERT_EQ(conf.getSchema().getSchemaType(), SchemaType::BYTES);
    ASSERT_EQ(conf.getSendTimeout(), 30000);
    ASSERT_EQ(conf.getInitialSequenceId(), -1ll);
    ASSERT_EQ(conf.getCompressionType(), CompressionType::CompressionNone);
    ASSERT_EQ(conf.getMaxPendingMessages(), 1000);
    ASSERT_EQ(conf.getMaxPendingMessagesAcrossPartitions(), 50000);
    ASSERT_EQ(conf.getPartitionsRoutingMode(), ProducerConfiguration::UseSinglePartition);
    ASSERT_EQ(conf.getMessageRouterPtr(), MessageRoutingPolicyPtr{});
    ASSERT_EQ(conf.getHashingScheme(), ProducerConfiguration::BoostHash);
    ASSERT_EQ(conf.getBlockIfQueueFull(), false);
    ASSERT_EQ(conf.getBatchingEnabled(), true);
    ASSERT_EQ(conf.getBatchingMaxMessages(), 1000);
    ASSERT_EQ(conf.getBatchingMaxAllowedSizeInBytes(), 128 * 1024);
    ASSERT_EQ(conf.getBatchingMaxPublishDelayMs(), 10);
    ASSERT_EQ(conf.getBatchingType(), ProducerConfiguration::DefaultBatching);
    ASSERT_EQ(conf.getCryptoKeyReader(), CryptoKeyReaderPtr{});
    ASSERT_EQ(conf.getCryptoFailureAction(), ProducerCryptoFailureAction::FAIL);
    ASSERT_EQ(conf.isEncryptionEnabled(), false);
    ASSERT_EQ(conf.getEncryptionKeys(), std::set<std::string>{});
    ASSERT_EQ(conf.getProperties().empty(), true);
    ASSERT_EQ(conf.isChunkingEnabled(), false);
}

class MockMessageRoutingPolicy : public MessageRoutingPolicy {
   public:
    int getPartition(const Message& msg) override { return 0; }
    int getPartition(const Message& msg, const TopicMetadata& topicMetadata) override { return 0; }
};

TEST(ProducerConfigurationTest, testCustomConfig) {
    ProducerConfiguration conf;

    conf.setProducerName("producer");
    ASSERT_EQ(conf.getProducerName(), "producer");

    const std::string exampleSchema =
        "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
        "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
    const SchemaInfo schema(AVRO, "Avro", exampleSchema, StringMap{{"schema-key", "schema-value"}});

    conf.setSchema(schema);
    ASSERT_EQ(conf.getSchema().getName(), schema.getName());
    ASSERT_EQ(conf.getSchema().getSchemaType(), schema.getSchemaType());
    ASSERT_EQ(conf.getSchema().getSchema(), schema.getSchema());
    ASSERT_EQ(conf.getSchema().getProperties(), schema.getProperties());

    conf.setSendTimeout(0);
    ASSERT_EQ(conf.getSendTimeout(), 0);

    conf.setInitialSequenceId(100ll);
    ASSERT_EQ(conf.getInitialSequenceId(), 100ll);

    conf.setCompressionType(CompressionType::CompressionLZ4);
    ASSERT_EQ(conf.getCompressionType(), CompressionType::CompressionLZ4);

    conf.setMaxPendingMessages(2000);
    ASSERT_EQ(conf.getMaxPendingMessages(), 2000);

    conf.setMaxPendingMessagesAcrossPartitions(100000);
    ASSERT_EQ(conf.getMaxPendingMessagesAcrossPartitions(), 100000);

    conf.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    ASSERT_EQ(conf.getPartitionsRoutingMode(), ProducerConfiguration::RoundRobinDistribution);

    const auto router = std::make_shared<MockMessageRoutingPolicy>();
    conf.setMessageRouter(router);
    ASSERT_EQ(conf.getPartitionsRoutingMode(), ProducerConfiguration::CustomPartition);
    ASSERT_EQ(conf.getMessageRouterPtr(), router);

    conf.setHashingScheme(ProducerConfiguration::JavaStringHash);
    ASSERT_EQ(conf.getHashingScheme(), ProducerConfiguration::JavaStringHash);

    conf.setBlockIfQueueFull(true);
    ASSERT_EQ(conf.getBlockIfQueueFull(), true);

    conf.setBatchingEnabled(false);
    ASSERT_EQ(conf.getBatchingEnabled(), false);

    conf.setBatchingMaxMessages(2000);
    ASSERT_EQ(conf.getBatchingMaxMessages(), 2000);

    conf.setBatchingMaxAllowedSizeInBytes(1024);
    ASSERT_EQ(conf.getBatchingMaxAllowedSizeInBytes(), 1024);

    conf.setBatchingMaxPublishDelayMs(1);
    ASSERT_EQ(conf.getBatchingMaxPublishDelayMs(), 1);

    conf.setBatchingType(ProducerConfiguration::KeyBasedBatching);
    ASSERT_EQ(conf.getBatchingType(), ProducerConfiguration::KeyBasedBatching);

    const auto cryptoKeyReader = std::make_shared<NoOpsCryptoKeyReader>();
    conf.setCryptoKeyReader(cryptoKeyReader);
    ASSERT_EQ(conf.getCryptoKeyReader(), cryptoKeyReader);

    conf.setCryptoFailureAction(pulsar::ProducerCryptoFailureAction::SEND);
    ASSERT_EQ(conf.getCryptoFailureAction(), ProducerCryptoFailureAction::SEND);

    conf.addEncryptionKey("key");
    ASSERT_EQ(conf.getEncryptionKeys(), std::set<std::string>{"key"});
    ASSERT_EQ(conf.isEncryptionEnabled(), true);

    conf.setProperty("k1", "v1");
    ASSERT_EQ(conf.getProperties()["k1"], "v1");
    ASSERT_EQ(conf.hasProperty("k1"), true);

    conf.setChunkingEnabled(true);
    ASSERT_EQ(conf.isChunkingEnabled(), true);
}
