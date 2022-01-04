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
#include <ctime>
#include <random>

#include <pulsar/Client.h>
#include <gtest/gtest.h>
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

// See the `maxMessageSize` config in test-conf/standalone-ssl.conf
static constexpr size_t maxMessageSize = 10240;

static std::string toString(CompressionType compressionType) {
    switch (compressionType) {
        case CompressionType::CompressionNone:
            return "None";
        case CompressionType::CompressionLZ4:
            return "LZ4";
        case CompressionType::CompressionZLib:
            return "ZLib";
        case CompressionType::CompressionZSTD:
            return "ZSTD";
        case CompressionType::CompressionSNAPPY:
            return "SNAPPY";
        default:
            return "Unknown (" + std::to_string(compressionType) + ")";
    }
}

class MessageChunkingTest : public ::testing::TestWithParam<CompressionType> {
   public:
    void TearDown() override { client_.close(); }

    void createProducer(const std::string& topic, Producer& producer) {
        ProducerConfiguration conf;
        conf.setBatchingEnabled(false);
        conf.setChunkingEnabled(true);
        conf.setCompressionType(GetParam());
        LOG_INFO("Create producer to topic: " << topic
                                              << ", compression: " << toString(conf.getCompressionType()));
        ASSERT_EQ(ResultOk, client_.createProducer(topic, conf, producer));
    }

    void createConsumer(const std::string& topic, Consumer& consumer) {
        ASSERT_EQ(ResultOk, client_.subscribe(topic, "my-sub", consumer));
    }

   private:
    Client client_{lookupUrl};
};

TEST_F(MessageChunkingTest, testInvalidConfig) {
    Client client(lookupUrl);
    ProducerConfiguration conf;
    conf.setBatchingEnabled(true);
    conf.setChunkingEnabled(true);
    Producer producer;
    ASSERT_THROW(client.createProducer("xxx", conf, producer), std::invalid_argument);
    client.close();
}

TEST_P(MessageChunkingTest, testEndToEnd) {
    const std::string topic =
        "MessageChunkingTest-EndToEnd-" + toString(GetParam()) + std::to_string(time(nullptr));
    Consumer consumer;
    createConsumer(topic, consumer);
    Producer producer;
    createProducer(topic, producer);

    std::string largeMessage(maxMessageSize * 3, 'a');
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<unsigned> u(0, 25);
    for (auto& ch : largeMessage) {
        ch = 'a' + u(e);
    }

    MessageId sendMessageId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), sendMessageId));
    LOG_INFO("Send to " << sendMessageId);

    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
    LOG_INFO("Receive " << msg.getLength() << " bytes from " << msg.getMessageId());
    ASSERT_EQ(msg.getDataAsString(), largeMessage);
    ASSERT_EQ(msg.getMessageId(), sendMessageId);
}

INSTANTIATE_TEST_SUITE_P(Pulsar, MessageChunkingTest, ::testing::Values(CompressionNone),
                         [](const ::testing::TestParamInfo<MessageChunkingTest::ParamType>& info) {
                             return toString(info.param);
                         });
