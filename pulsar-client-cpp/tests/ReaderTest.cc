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

#include <string>

using namespace pulsar;

static std::string serviceUrl = "pulsar://localhost:8885";

TEST(ReaderTest, testSimpleReader) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/test-simple-reader";

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    client.close();
}

TEST(ReaderTest, testReaderAfterMessagesWerePublished) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/testReaderAfterMessagesWerePublished";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
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
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    client.close();
}

TEST(ReaderTest, testMultipleReaders) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/testMultipleReaders";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
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
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader2.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    client.close();
}

TEST(ReaderTest, testReaderOnLastMessage) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/testReaderOnLastMessage";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 10; i < 20; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    client.close();
}

TEST(ReaderTest, testReaderOnSpecificMessage) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/testReaderOnSpecificMessage";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
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
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);

        lastMessageId = msg.getMessageId();
    }

    // Create another reader starting on msgid4
    ASSERT_EQ(ResultOk, client.createReader(topicName, lastMessageId, readerConf, reader));

    for (int i = 5; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    client.close();
}

/**
 * Test that we can position on a particular message even within a batch
 */
TEST(ReaderTest, testReaderOnSpecificMessageWithBatches) {
    Client client(serviceUrl);

    std::string topicName = "persistent://property/cluster/namespace/testReaderOnSpecificMessageWithBatches";

    Producer producer;
    // Enable batching
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    producerConf.setBatchingMaxPublishDelayMs(1000);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + boost::lexical_cast<std::string>(i);
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
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
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
        std::string expected = "my-message-" + boost::lexical_cast<std::string>(i);
        ASSERT_EQ(expected, content);
    }

    reader.close();
    reader2.close();
    client.close();
}
