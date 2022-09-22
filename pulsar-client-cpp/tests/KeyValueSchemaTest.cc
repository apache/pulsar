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
#include "lib/LogUtils.h"

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

class KeyValueSchemaTest : public ::testing::TestWithParam<KeyValueEncodingType> {
   public:
    void TearDown() override { client.close(); }

    void createProducer(const std::string& topic, Producer& producer) {
        ProducerConfiguration configProducer;
        configProducer.setSchema(getKeyValueSchema());
        configProducer.setBatchingEnabled(false);
        ASSERT_EQ(ResultOk, client.createProducer(topic, configProducer, producer));
    }

    void createConsumer(const std::string& topic, Consumer& consumer) {
        ConsumerConfiguration configConsumer;
        configConsumer.setSchema(getKeyValueSchema());
        ASSERT_EQ(ResultOk, client.subscribe(topic, "sub-kv", configConsumer, consumer));
    }

    SchemaInfo getKeyValueSchema() {
        SchemaInfo keySchema(JSON, "key-json", jsonSchema);
        SchemaInfo valueSchema(JSON, "value-json", jsonSchema);
        return SchemaInfo(keySchema, valueSchema, GetParam());
    }

   private:
    Client client{lookupUrl};
    std::string jsonSchema =
        R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
};

TEST_P(KeyValueSchemaTest, testKeyValueSchema) {
    const std::string topicName = "testKeyValueSchema" + std::to_string(time(nullptr));

    Producer producer;
    createProducer(topicName, producer);
    Consumer consumer;
    createConsumer(topicName, consumer);

    // Sending and receiving messages.
    std::string keyData = "{\"re\":2.1,\"im\":1.23}";
    std::string valueData = "{\"re\":2.1,\"im\":1.23}";
    KeyValue keyValue((std::string(keyData)), std::string(valueData));
    Message msg = MessageBuilder().setContent(keyValue).setProperty("x", "1").build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    Message receiveMsg;
    consumer.receive(receiveMsg);
    KeyValue keyValueData = receiveMsg.getKeyValueData();

    auto encodingType = GetParam();
    if (encodingType == pulsar::KeyValueEncodingType::INLINE) {
        ASSERT_EQ(receiveMsg.getPartitionKey(), "");
        ASSERT_EQ(keyValueData.getKey(), keyData);
    } else {
        ASSERT_EQ(receiveMsg.getPartitionKey(), keyData);
        ASSERT_EQ(keyValueData.getKey(), "");
    }
    ASSERT_EQ(keyValueData.getValueAsString(), valueData);
}

INSTANTIATE_TEST_CASE_P(Pulsar, KeyValueSchemaTest,
                        ::testing::Values(KeyValueEncodingType::INLINE, KeyValueEncodingType::SEPARATED));
