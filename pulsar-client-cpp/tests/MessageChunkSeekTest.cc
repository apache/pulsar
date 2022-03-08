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
#include "PulsarFriend.h"
#include "MessageIdImpl.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

// See the `maxMessageSize` config in test-conf/standalone-ssl.conf
static constexpr size_t maxMessageSize = 1024000;

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

inline std::string createLargeMessage() {
    std::string largeMessage(maxMessageSize * 3, 'a');
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<unsigned> u(0, 25);
    for (size_t i = 0; i < largeMessage.size(); i++) {
        largeMessage[i] = 'a' + u(e);
    }
    return largeMessage;
}

class MessageChunkingSeekTest : public ::testing::TestWithParam<CompressionType> {
   public:
    static std::string largeMessage;

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

std::string MessageChunkingSeekTest::largeMessage = createLargeMessage();

TEST_P(MessageChunkingSeekTest, testSeek) {
    const std::string topic =
        "MessageChunkingSeekTest-testSeek-" + toString(GetParam()) + std::to_string(time(nullptr));
    Consumer consumer;
    createConsumer(topic, consumer);
    Producer producer;
    createProducer(topic, producer);

    MessageId a(1,1,1,1,2,2,2,2);
    std::string b;
    a.serialize(b);
    MessageId c;
    c.deserialize(b);
    constexpr int numMessages = 20;

    std::vector<MessageId> receiveMessageIds;
    for (int i = 0; i < numMessages; i++) {
        MessageId messageId;
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), messageId));
        LOG_INFO("Send " << i << " to " << messageId);
    }

    Message msg;
    for (int i = 0; i < numMessages; i++) {
        consumer.receive(msg, 3000);
        receiveMessageIds.push_back(msg.getMessageId());
    }
    
    consumer.seek(receiveMessageIds[1]);
    for (int i = 1; i < numMessages; i++) {
        consumer.receive(msg, 3000);
        LOG_INFO("Receive " << i << " to " << msg.getMessageId());
        ASSERT_EQ(receiveMessageIds[i], msg.getMessageId());
    }
}

// The CI env is Ubuntu 16.04, the gtest-dev version is 1.8.0 that doesn't have INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_CASE_P(Pulsar, MessageChunkingSeekTest,
                        ::testing::Values(CompressionNone, CompressionLZ4, CompressionZLib, CompressionZSTD,
                                          CompressionSNAPPY));
