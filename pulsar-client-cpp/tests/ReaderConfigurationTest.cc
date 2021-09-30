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
/**
 * This test only tests the ConsumerConfiguration used for the Reader's internal consumer.
 * Because the ReaderConfiguration for Reader itself is meaningless.
 */
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <lib/ReaderImpl.h>
#include "NoOpsCryptoKeyReader.h"

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

TEST(ReaderConfigurationTest, testDefaultConfig) {
    const std::string topic = "ReaderConfigurationTest-default-config";
    Client client(lookupUrl);
    ReaderConfiguration readerConf;
    Reader reader;

    std::unique_lock<std::mutex> lock(test::readerConfigTestMutex);
    test::readerConfigTestEnabled = true;
    ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), readerConf, reader));
    const auto consumerConf = test::consumerConfigOfReader.clone();
    test::readerConfigTestEnabled = false;
    lock.unlock();

    ASSERT_EQ(consumerConf.getConsumerType(), ConsumerExclusive);
    ASSERT_EQ(consumerConf.getReceiverQueueSize(), 1000);
    ASSERT_EQ(consumerConf.isReadCompacted(), false);
    ASSERT_EQ(consumerConf.getSchema().getName(), "BYTES");
    ASSERT_EQ(consumerConf.getUnAckedMessagesTimeoutMs(), 0);
    ASSERT_EQ(consumerConf.getTickDurationInMs(), 1000);
    ASSERT_EQ(consumerConf.getAckGroupingTimeMs(), 100);
    ASSERT_EQ(consumerConf.getAckGroupingMaxSize(), 1000);
    ASSERT_EQ(consumerConf.getCryptoKeyReader().get(), nullptr);
    ASSERT_EQ(consumerConf.getCryptoFailureAction(), ConsumerCryptoFailureAction::FAIL);
    ASSERT_TRUE(consumerConf.getProperties().empty());
    ASSERT_TRUE(consumerConf.getConsumerName().empty());
    ASSERT_FALSE(consumerConf.hasMessageListener());

    client.close();
}

TEST(ReaderConfigurationTest, testCustomConfig) {
    const std::string exampleSchema =
        "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
        "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";

    const std::string topic = "ReaderConfigurationTest-custom-config";
    Client client(lookupUrl);

    const SchemaInfo schema(AVRO, "Avro", exampleSchema, StringMap{{"schema-key", "schema-value"}});

    ProducerConfiguration producerConf;
    producerConf.setSchema(schema);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf, producer));
    ASSERT_FALSE(producer.getSchemaVersion().empty());

    ReaderConfiguration readerConf;
    readerConf.setSchema(schema);
    readerConf.setReaderListener([](Reader, const Message&) {});
    readerConf.setReceiverQueueSize(2000);
    readerConf.setReaderName("my-reader");
    readerConf.setReadCompacted(true);
    readerConf.setUnAckedMessagesTimeoutMs(11000);
    readerConf.setTickDurationInMs(2000);
    readerConf.setAckGroupingTimeMs(0);
    readerConf.setAckGroupingMaxSize(4096);
    const auto cryptoReader = std::make_shared<NoOpsCryptoKeyReader>();
    readerConf.setCryptoKeyReader(cryptoReader);
    readerConf.setCryptoFailureAction(ConsumerCryptoFailureAction::DISCARD);
    const std::map<std::string, std::string> properties{{"key-1", "value-1"}, {"key-2", "value-2"}};
    readerConf.setProperties(properties);

    Reader reader;
    std::unique_lock<std::mutex> lock(test::readerConfigTestMutex);
    test::readerConfigTestEnabled = true;
    ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), readerConf, reader));
    const auto consumerConf = test::consumerConfigOfReader.clone();
    test::readerConfigTestEnabled = false;
    lock.unlock();

    ASSERT_EQ(consumerConf.getSchema().getName(), schema.getName());
    ASSERT_EQ(consumerConf.getSchema().getSchemaType(), schema.getSchemaType());
    ASSERT_EQ(consumerConf.getSchema().getSchema(), schema.getSchema());
    ASSERT_EQ(consumerConf.getSchema().getProperties(), schema.getProperties());

    ASSERT_EQ(consumerConf.getConsumerType(), ConsumerExclusive);
    ASSERT_TRUE(consumerConf.hasMessageListener());
    ASSERT_EQ(consumerConf.getReceiverQueueSize(), 2000);
    ASSERT_EQ(consumerConf.getConsumerName(), "my-reader");
    ASSERT_EQ(consumerConf.isReadCompacted(), true);
    ASSERT_EQ(consumerConf.getUnAckedMessagesTimeoutMs(), 11000);
    ASSERT_EQ(consumerConf.getTickDurationInMs(), 2000);
    ASSERT_EQ(consumerConf.getAckGroupingTimeMs(), 0);
    ASSERT_EQ(consumerConf.getAckGroupingMaxSize(), 4096);
    ASSERT_EQ(consumerConf.getCryptoKeyReader(), cryptoReader);
    ASSERT_EQ(consumerConf.getCryptoFailureAction(), ConsumerCryptoFailureAction::DISCARD);
    ASSERT_EQ(consumerConf.getProperties(), properties);
    ASSERT_TRUE(consumerConf.hasProperty("key-1"));
    ASSERT_EQ(consumerConf.getProperty("key-1"), "value-1");
    ASSERT_TRUE(consumerConf.hasProperty("key-2"));
    ASSERT_EQ(consumerConf.getProperty("key-2"), "value-2");
    ASSERT_FALSE(consumerConf.hasProperty("key-3"));

    client.close();
}
