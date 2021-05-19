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
#include <pulsar/Reader.h>
#include "ReaderTest.h"
#include "HttpHelper.h"

#include <gtest/gtest.h>

#include <time.h>
#include <string>

#include <lib/LogUtils.h>
DECLARE_LOG_OBJECT()

using namespace pulsar;

static std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

TEST(ReaderTest, testSimpleReader) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/test-simple-reader";

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testReaderAfterMessagesWerePublished) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderAfterMessagesWerePublished";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testMultipleReaders) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testMultipleReaders";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader1;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader1));

    Reader reader2;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader2));

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader1.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader2.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader1.close();
    reader2.close();
    client.close();
}

TEST(ReaderTest, testReaderOnLastMessage) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderOnLastMessage";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 10; i < 20; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testReaderOnSpecificMessage) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderOnSpecificMessage";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    MessageId lastMessageId;

    for (int i = 0; i < 5; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);

        lastMessageId = msg.getMessageId();
    }

    // Create another reader starting on msgid4
    ASSERT_EQ(ResultOk, client.createReader(topicName, lastMessageId, readerConf, reader));

    for (int i = 5; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

/**
 * Test that we can position on a particular message even within a batch
 */
TEST(ReaderTest, testReaderOnSpecificMessageWithBatches) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderOnSpecificMessageWithBatches";

    Producer producer;
    // Enable batching
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    producerConf.setBatchingMaxPublishDelayMs(1000);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        producer.sendAsync(msg, NULL);
    }

    // Send one sync message, to wait for everything before to be persisted as well
    std::string content = "my-message-10";
    Message msg = MessageBuilder().setContent(content).build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    std::string lastMessageId;

    for (int i = 0; i < 5; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);

        msg.getMessageId().serialize(lastMessageId);
    }

    // Create another reader starting on msgid4
    auto msgId4 = MessageId::deserialize(lastMessageId);
    Reader reader2;
    ASSERT_EQ(ResultOk, client.createReader(topicName, msgId4, readerConf, reader2));

    for (int i = 5; i < 11; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader2.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    reader2.close();
    client.close();
}

TEST(ReaderTest, testReaderReachEndOfTopic) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderReachEndOfTopic";

    // 1. create producer
    Producer producer;
    // Enable batching
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    producerConf.setBatchingMaxPublishDelayMs(1000);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    // 2. create reader, and expect hasMessageAvailable return false since no message produced.
    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    bool hasMessageAvailable;
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    // 3. produce 10 messages.
    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // 4. expect hasMessageAvailable return true, and after read 10 messages out, it return false.
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    int readMessageCount = 0;
    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
    }

    ASSERT_EQ(readMessageCount, 10);
    ASSERT_FALSE(hasMessageAvailable);

    // 5. produce another 10 messages, expect hasMessageAvailable return true,
    //    and after read these 10 messages out, it return false.
    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
    }
    ASSERT_EQ(readMessageCount, 20);
    ASSERT_FALSE(hasMessageAvailable);

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testReaderReachEndOfTopicMessageWithoutBatches) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReaderReachEndOfTopicMessageWithBatches";

    // 1. create producer
    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    // 2. create reader, and expect hasMessageAvailable return false since no message produced.
    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    bool hasMessageAvailable;
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    // 3. produce 10 messages in batches way.
    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        producer.sendAsync(msg, NULL);
    }
    // Send one sync message, to wait for everything before to be persisted as well
    std::string content = "my-message-10";
    Message msg = MessageBuilder().setContent(content).build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    // 4. expect hasMessageAvailable return true, and after read 11 messages out, it return false.
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    std::string lastMessageId;
    int readMessageCount = 0;
    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
        msg.getMessageId().serialize(lastMessageId);
    }
    ASSERT_FALSE(hasMessageAvailable);
    ASSERT_EQ(readMessageCount, 11);

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testReferenceLeak) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testReferenceLeak";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    ConsumerImplBaseWeakPtr consumerPtr = ReaderTest::getConsumer(reader);
    ReaderImplWeakPtr readerPtr = ReaderTest::getReaderImplWeakPtr(reader);

    LOG_INFO("1 consumer use count " << consumerPtr.use_count());
    LOG_INFO("1 reader use count " << readerPtr.use_count());

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    // will be released after exit this method.
    ASSERT_EQ(1, consumerPtr.use_count());
    ASSERT_EQ(1, readerPtr.use_count());
    client.close();
    // will be released after exit this method.
    ASSERT_EQ(1, consumerPtr.use_count());
    ASSERT_EQ(1, readerPtr.use_count());
}

TEST(ReaderTest, testPartitionIndex) {
    Client client(serviceUrl);

    const std::string nonPartitionedTopic = "ReaderTestPartitionIndex-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "ReaderTestPartitionIndex-par-topic-" + std::to_string(time(nullptr));

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    const std::string partition0 = partitionedTopic + "-partition-0";
    const std::string partition1 = partitionedTopic + "-partition-1";

    ReaderConfiguration readerConf;
    Reader readers[3];
    ASSERT_EQ(ResultOk,
              client.createReader(nonPartitionedTopic, MessageId::earliest(), readerConf, readers[0]));
    ASSERT_EQ(ResultOk, client.createReader(partition0, MessageId::earliest(), readerConf, readers[1]));
    ASSERT_EQ(ResultOk, client.createReader(partition1, MessageId::earliest(), readerConf, readers[2]));

    Producer producers[3];
    ASSERT_EQ(ResultOk, client.createProducer(nonPartitionedTopic, producers[0]));
    ASSERT_EQ(ResultOk, client.createProducer(partition0, producers[1]));
    ASSERT_EQ(ResultOk, client.createProducer(partition1, producers[2]));

    for (auto& producer : producers) {
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build()));
    }

    Message msg;
    readers[0].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), -1);
    readers[1].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), 0);
    readers[2].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), 1);

    client.close();
}

TEST(ReaderTest, testSubscriptionNameSetting) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/test-subscription-name-setting";
    std::string subName = "test-sub";

    ReaderConfiguration readerConf;
    readerConf.setInternalSubscriptionName(subName);
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    ASSERT_EQ(subName, ReaderTest::getConsumer(reader)->getSubscriptionName());

    reader.close();
    client.close();
}

TEST(ReaderTest, testSetSubscriptionNameAndPrefix) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testSetSubscriptionNameAndPrefix";
    std::string subName = "test-sub";

    ReaderConfiguration readerConf;
    readerConf.setInternalSubscriptionName(subName);
    readerConf.setSubscriptionRolePrefix("my-prefix");
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    ASSERT_EQ(subName, ReaderTest::getConsumer(reader)->getSubscriptionName());

    reader.close();
    client.close();
}

TEST(ReaderTest, testMultiSameSubscriptionNameReaderShouldFail) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/testMultiSameSubscriptionNameReaderShouldFail";
    std::string subscriptionName = "test-sub";

    ReaderConfiguration readerConf1;
    readerConf1.setInternalSubscriptionName(subscriptionName);
    Reader reader1;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf1, reader1));

    ReaderConfiguration readerConf2;
    readerConf2.setInternalSubscriptionName(subscriptionName);
    Reader reader2;
    ASSERT_EQ(ResultConsumerBusy,
              client.createReader(topicName, MessageId::earliest(), readerConf2, reader2));

    reader1.close();
    reader2.close();
    client.close();
}

TEST(ReaderTest, testIsConnected) {
    const std::string topic = "testReaderIsConnected-" + std::to_string(time(nullptr));
    Client client(serviceUrl);

    Reader reader;
    ASSERT_FALSE(reader.isConnected());

    ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), {}, reader));
    ASSERT_TRUE(reader.isConnected());

    ASSERT_EQ(ResultOk, reader.close());
    ASSERT_FALSE(reader.isConnected());
}
