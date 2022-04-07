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
#include <atomic>
#include <ctime>
#include <functional>
#include <gtest/gtest.h>
#include <sstream>
#include <thread>
#include <unistd.h>

#include <lib/Commands.h>
#include <lib/Future.h>
#include <lib/Latch.h>
#include <lib/LogUtils.h>
#include <lib/TopicName.h>
#include <lib/Utils.h>
#include <pulsar/Client.h>
#include <pulsar/MessageBatch.h>
#include <pulsar/MessageBuilder.h>

#include "ConsumerTest.h"
#include "CustomRoutingPolicy.h"
#include "HttpHelper.h"
#include "PulsarFriend.h"

DECLARE_LOG_OBJECT();

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

// ecpoch time in seconds
const long epochTime = time(NULL);

class MessageCountSendCallback {
   public:
    MessageCountSendCallback(std::atomic_int& numOfMessagesProduced)
        : numOfMessagesProduced_(numOfMessagesProduced) {}

    void operator()(Result result, const MessageId&) {
        ASSERT_EQ(result, ResultOk);
        numOfMessagesProduced_++;
    }

   private:
    std::atomic_int& numOfMessagesProduced_;
};

static void sendFailCallBack(Result r, Result expect_result) { EXPECT_EQ(r, expect_result); }

static int globalPublishCountSuccess = 0;
static int globalPublishCountQueueFull = 0;

static void sendCallBackExpectingErrors(Result r, const MessageId& msgId) {
    if (r == ResultProducerQueueIsFull) {
        globalPublishCountQueueFull++;
    } else if (r == ResultOk) {
        globalPublishCountSuccess++;
    }
}

TEST(BatchMessageTest, testProducerConfig) {
    ProducerConfiguration conf;
    try {
        conf.setBatchingMaxMessages(1);
        FAIL();
    } catch (const std::exception&) {
        // Ok
    }
    ASSERT_EQ(ProducerConfiguration::DefaultBatching, conf.getBatchingType());
    conf.setBatchingType(ProducerConfiguration::KeyBasedBatching);
    ASSERT_EQ(ProducerConfiguration::KeyBasedBatching, conf.getBatchingType());
}

TEST(BatchMessageTest, testProducerTimeout) {
    std::string testName = std::to_string(epochTime) + "testProducerTimeout";

    ClientConfiguration clientConf;
    clientConf.setStatsIntervalInSeconds(1);

    Client client(lookupUrl, clientConf);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 3;
    int numOfMessages = 4;
    int timeout = 4000;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxPublishDelayMs(timeout);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    ProducerStatsImplPtr producerStatsImplPtr = PulsarFriend::getProducerStatsPtr(producer);
    // Send Asynchronously
    std::string prefix = "msg-batch-test-produce-timeout-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder()
                          .setContent(messageContent)
                          .setProperty("type", "batch")
                          .setProperty("msgIndex", std::to_string(i))
                          .build();
        LOG_DEBUG("sending message " << messageContent);
        clock_t start, end;
        /* Start the timer */
        start = time(NULL);
        LOG_DEBUG("start = " << start);
        Promise<Result, MessageId> promise;
        producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));
        MessageId mi;
        promise.getFuture().get(mi);
        /* End the timer */
        end = time(NULL);
        LOG_DEBUG("end = " << end);
        // Greater than or equal to since there may be delay in sending messaging
        ASSERT_GE((double)(end - start), timeout / 1000.0);
        ASSERT_EQ(producerStatsImplPtr->getTotalMsgsSent(), i + 1);
        ASSERT_EQ(PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()), i + 1);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(receivedMsg.getProperty("type"), "batch");
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testBatchSizeInBytes) {
    std::string testName = std::to_string(epochTime) + "testBatchSizeInBytes";

    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 1000;
    int numOfMessages = 30;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxAllowedSizeInBytes(20);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    ProducerStatsImplPtr producerStatsImplPtr = PulsarFriend::getProducerStatsPtr(producer);
    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = "12345678";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        ASSERT_EQ(producerStatsImplPtr->getNumMsgsSent(), i + 1);
        ASSERT_LT(PulsarFriend::sum(producerStatsImplPtr->getSendMap()), i + 1);
        ASSERT_EQ(producerStatsImplPtr->getTotalMsgsSent(), i + 1);
        ASSERT_LT(PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()), i + 1);
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_LT(pulsar::PulsarFriend::getBatchIndex(receivedMsg.getMessageId()), 2);
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }

    // Check stats
    ASSERT_EQ(PulsarFriend::sum(producerStatsImplPtr->getSendMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()), numOfMessages);

    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testSmallReceiverQueueSize) {
    std::string testName = std::to_string(epochTime) + "testSmallReceiverQueueSize";

    ClientConfiguration clientConf;
    clientConf.setStatsIntervalInSeconds(20);

    Client client(lookupUrl, clientConf);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 10;
    int numOfMessages = 1000;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxPublishDelayMs(1);
    conf.setBatchingEnabled(true);
    conf.setMaxPendingMessages(numOfMessages + 1);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(41);

    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    ProducerStatsImplPtr producerStatsImplPtr = PulsarFriend::getProducerStatsPtr(producer);
    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        ASSERT_EQ(producerStatsImplPtr->getTotalMsgsSent(), i + 1);
        ASSERT_LE(PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()), i + 1);
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    for (i = 0; i < numOfMessages; i++) {
        consumer.receive(receivedMsg);
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }

    ConsumerStatsImplPtr consumerStatsImplPtr = PulsarFriend::getConsumerStatsPtr(consumer);
    unsigned long t = consumerStatsImplPtr->getAckedMsgMap().at(
        std::make_pair<Result, proto::CommandAck_AckType>(ResultOk, proto::CommandAck_AckType_Individual));
    ASSERT_EQ(t, numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getAckedMsgMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalAckedMsgMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getReceivedMsgMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalReceivedMsgMap()), numOfMessages);
    ASSERT_EQ(consumerStatsImplPtr->getTotalNumBytesRecieved(), consumerStatsImplPtr->getNumBytesRecieved());
    std::this_thread::sleep_for(std::chrono::seconds(20));
    ASSERT_NE(consumerStatsImplPtr->getTotalNumBytesRecieved(), consumerStatsImplPtr->getNumBytesRecieved());
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalAckedMsgMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalReceivedMsgMap()), numOfMessages);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getAckedMsgMap()), 0);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getReceivedMsgMap()), 0);

    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testIndividualAck) {
    std::string testName = std::to_string(epochTime) + "testIndividualAck";

    ClientConfiguration clientConfig;
    clientConfig.setStatsIntervalInSeconds(1);

    Client client(lookupUrl, clientConfig);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 10;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(1);

    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        LOG_DEBUG("sending message " << messageContent);
    }
    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack every 2nd message
        if (i % 2 == 0) {
            ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
        }
    }
    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Unsubscribe and resubscribe
    // Expecting all messages to be sent again

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack every first 5 and 10th message
        if (i <= 5 || i == 10) {
            ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
        }
    }

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Unsubscribe and resubscribe
    // Expecting only one batch message to be resent

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i + numOfMessages / 2);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++ + numOfMessages / 2));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack first 4 message only
        if (i <= 4) {
            ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
        }
    }

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages / 2);

    // Unsubscribe and resubscribe
    // Expecting only one batch message to be resent

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i + numOfMessages / 2);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++ + numOfMessages / 2));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack all
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages / 2);

    // Unsubscribe and resubscribe
    // Expecting no batch message to be resent

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    // Number of messages consumed
    ASSERT_NE(ResultOk, consumer.receive(receivedMsg, 5000));

    consumer.close();
    client.close();
}

TEST(BatchMessageTest, testCumulativeAck) {
    std::string testName = std::to_string(epochTime) + "testCumulativeAck";

    ClientConfiguration clientConfig;
    clientConfig.setStatsIntervalInSeconds(100);

    Client client(lookupUrl, clientConfig);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 15;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(1);

    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    ProducerStatsImplPtr producerStatsImplPtr = PulsarFriend::getProducerStatsPtr(producer);

    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    ConsumerStatsImplPtr consumerStatsImplPtr = PulsarFriend::getConsumerStatsPtr(consumer);
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Cumm. Ack 7th message
        if (i == 7) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }

    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getReceivedMsgMap()), i + 1);
    // Since last receive call times out
    ASSERT_EQ(consumerStatsImplPtr->getReceivedMsgMap().at(ResultOk), i);
    ASSERT_EQ(consumerStatsImplPtr->getReceivedMsgMap().at(ResultTimeout), 1);
    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getAckedMsgMap()), 1);
    ASSERT_EQ(producerStatsImplPtr->getNumBytesSent(), consumerStatsImplPtr->getNumBytesRecieved());
    unsigned long t = consumerStatsImplPtr->getAckedMsgMap().at(
        std::make_pair<Result, proto::CommandAck_AckType>(ResultOk, proto::CommandAck_AckType_Cumulative));
    ASSERT_EQ(t, 1);

    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Unsubscribe and resubscribe
    // Expecting 10 messages to be sent again

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    consumerStatsImplPtr = PulsarFriend::getConsumerStatsPtr(consumer);
    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i + 5);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++ + 5));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack 10th message
        if (i == 10) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }

    ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getAckedMsgMap()), 1);
    t = consumerStatsImplPtr->getAckedMsgMap().at(
        std::make_pair<Result, proto::CommandAck_AckType>(ResultOk, proto::CommandAck_AckType_Cumulative));
    ASSERT_EQ(t, 1);

    // Number of messages consumed
    ASSERT_EQ(i, 10);

    // Unsubscribe and resubscribe
    // Expecting no batch message to be resent

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    // Number of messages consumed
    ASSERT_NE(ResultOk, consumer.receive(receivedMsg, 5000));
}

TEST(BatchMessageTest, testMixedAck) {
    std::string testName = std::to_string(epochTime) + "testMixedAck";

    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 15;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Cumm. Ack 14th message
        if (i == 14) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }
    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Unsubscribe and resubscribe
    // Expecting 5 messages to be sent again

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i + 10);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++ + 10));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Cumm Ack 9th message
        if (i == 4) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }
    ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));

    // Number of messages consumed
    ASSERT_EQ(i, 5);

    // Unsubscribe and resubscribe
    // Expecting no batch message to be resent

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    // Number of messages consumed
    ASSERT_NE(ResultOk, consumer.receive(receivedMsg, 5000));
}

// Also testing Cumulative Ack test case where greatestCumulativeAck returns
// MessageId()
TEST(BatchMessageTest, testPermits) {
    std::string testName = std::to_string(epochTime) + "testPermits";

    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 10;
    int numOfMessages = 75;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxPublishDelayMs(5);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(5);

    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::atomic_int numOfMessagesProduced{0};
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        LOG_DEBUG("sending message " << messageContent);
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Since all messages are acked
    // Creating 25 new non batched message
    conf.setBatchingEnabled(false);

    client.createProducer(topicName, conf, producer);

    numOfMessagesProduced = 0;
    // Send Asynchronously
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, MessageCountSendCallback(numOfMessagesProduced));
        LOG_DEBUG("sending message " << messageContent);
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));

    ASSERT_LE(ConsumerTest::getNumOfMessagesInQueue(consumer), consumerConfig.getReceiverQueueSize());
    ASSERT_GE(ConsumerTest::getNumOfMessagesInQueue(consumer), consumerConfig.getReceiverQueueSize() / 2);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(numOfMessagesProduced.load(), numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testPartitionedTopics) {
    Client client(lookupUrl);
    std::string topicName =
        "persistent://public/default/test-partitioned-batch-messages-" + std::to_string(epochTime);

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/test-partitioned-batch-messages-" +
                      std::to_string(epochTime) + "/partitions";
    int res = makePutRequest(url, "7");

    LOG_DEBUG("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    Producer producer;
    // Enable batching on producer side
    int batchSize = 100;
    int numOfMessages = 10000;
    ProducerConfiguration conf;

    conf.setCompressionType(CompressionZLib);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);
    conf.setBatchingMaxPublishDelayMs(5);
    conf.setBlockIfQueueFull(false);
    conf.setMaxPendingMessages(10);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string subName = "subscription-name";
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    globalPublishCountSuccess = 0;
    globalPublishCountQueueFull = 0;

    // Send Asynchronously
    std::string prefix = "msg-batch-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBackExpectingErrors);
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 30000) == ResultOk) {
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
        i++;
    }

    LOG_DEBUG("globalPublishCountQueueFull = " << globalPublishCountQueueFull);
    LOG_DEBUG("globalPublishCountSuccess = " << globalPublishCountSuccess);
    LOG_DEBUG("numOfMessages = " << numOfMessages);

    // Number of messages produced
    ASSERT_EQ(globalPublishCountSuccess + globalPublishCountQueueFull, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages - globalPublishCountQueueFull);
}

TEST(BatchMessageTest, producerFailureResult) {
    std::string testName = std::to_string(epochTime) + "testCumulativeAck";

    ClientConfiguration clientConfig;
    clientConfig.setStatsIntervalInSeconds(100);

    Client client(lookupUrl, clientConfig);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    int batchSize = 100;
    ProducerConfiguration conf;

    conf.setCompressionType(CompressionZLib);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);
    conf.setBatchingMaxPublishDelayMs(50000);
    conf.setBlockIfQueueFull(false);
    conf.setMaxPendingMessages(10);

    Result res = Result::ResultBrokerMetadataError;

    client.createProducer(topicName, conf, producer);
    Message msg = MessageBuilder().setContent("test").build();
    producer.sendAsync(msg, std::bind(&sendFailCallBack, std::placeholders::_1, res));
    PulsarFriend::producerFailMessages(producer, res);
}

TEST(BatchMessageTest, testPraseMessageBatchEntry) {
    struct Case {
        std::string content;
        std::string propKey;
        std::string propValue;
    };
    std::vector<Case> cases;
    cases.push_back(Case{"example1", "prop1", "value1"});
    cases.push_back(Case{"example2", "prop2", "value2"});

    SharedBuffer payload = SharedBuffer::allocate(128);
    for (auto it = cases.begin(); it != cases.end(); ++it) {
        MessageBuilder msgBuilder;
        const Message& message =
            msgBuilder.setContent(it->content).setProperty(it->propKey, it->propValue).build();
        Commands::serializeSingleMessageInBatchWithPayload(message, payload, 1024);
    }

    MessageBatch messageBatch;
    MessageId fakeId(0, 5000, 10, -1);
    messageBatch.withMessageId(fakeId).parseFrom(payload, static_cast<uint32_t>(cases.size()));
    const std::vector<Message>& messages = messageBatch.messages();

    ASSERT_EQ(messages.size(), cases.size());
    for (int i = 0; i < cases.size(); ++i) {
        const Message& message = messages[i];
        const Case& expected = cases[i];
        ASSERT_EQ(message.getMessageId().batchIndex(), i);
        ASSERT_EQ(message.getMessageId().ledgerId(), 5000);
        ASSERT_EQ(message.getDataAsString(), expected.content);
        ASSERT_EQ(message.getProperty(expected.propKey), expected.propValue);
    }
}

TEST(BatchMessageTest, testSendCallback) {
    const std::string topicName = "persistent://public/default/BasicMessageTest-testSendCallback";

    Client client(lookupUrl);

    constexpr int numMessagesOfBatch = 3;

    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(true);
    producerConfig.setBatchingMaxMessages(numMessagesOfBatch);
    producerConfig.setBatchingMaxPublishDelayMs(1000);  // 1 s, it's long enough for 3 messages batched
    producerConfig.setMaxPendingMessages(5);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfig, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, "SubscriptionName", consumer));

    Latch latch(numMessagesOfBatch);
    std::set<MessageId> sentIdSet;
    for (int i = 0; i < numMessagesOfBatch; i++) {
        const auto msg = MessageBuilder().setContent("a").build();
        producer.sendAsync(msg, [&sentIdSet, i, &latch](Result result, const MessageId& id) {
            ASSERT_EQ(ResultOk, result);
            ASSERT_EQ(i, id.batchIndex());
            sentIdSet.emplace(id);
            LOG_INFO("id of batch " << i << ": " << id);
            latch.countdown();
        });
    }

    std::set<MessageId> receivedIdSet;
    for (int i = 0; i < numMessagesOfBatch; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        receivedIdSet.emplace(msg.getMessageId());
        consumer.acknowledge(msg);
    }

    latch.wait();
    ASSERT_EQ(sentIdSet, receivedIdSet);

    consumer.close();
    producer.close();
    client.close();
}

TEST(BatchMessageTest, testProducerQueueWithBatches) {
    std::string testName = std::to_string(epochTime) + "testProducerQueueWithBatches";

    ClientConfiguration clientConf;
    clientConf.setStatsIntervalInSeconds(0);

    Client client(lookupUrl, clientConf);
    std::string topicName = "persistent://public/default/" + testName;

    // Enable batching on producer side
    ProducerConfiguration conf;
    conf.setBlockIfQueueFull(false);
    conf.setMaxPendingMessages(10);
    conf.setBatchingMaxMessages(10000);
    conf.setBatchingMaxPublishDelayMs(1000);
    conf.setBatchingEnabled(true);

    Producer producer;
    Result result = client.createProducer(topicName, conf, producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = "msg-batch-test-produce-timeout-";
    int rejectedMessges = 0;
    for (int i = 0; i < 20; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent("hello").build();

        producer.sendAsync(msg, [&rejectedMessges](Result result, const MessageId& id) {
            if (result == ResultProducerQueueIsFull) {
                ++rejectedMessges;
            }
        });
    }

    ASSERT_EQ(rejectedMessges, 10);
}

TEST(BatchMessageTest, testSingleMessageMetadata) {
    const auto topic = "BatchMessageTest-SingleMessageMetadata-" + std::to_string(time(nullptr));
    constexpr int numMessages = 3;

    Client client(lookupUrl);

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(
                            topic, ProducerConfiguration().setBatchingMaxMessages(numMessages), producer));

    producer.sendAsync(MessageBuilder()
                           .setContent("msg-0")
                           .setPartitionKey("key-0")
                           .setOrderingKey("ordering-key-0")
                           .setEventTimestamp(10UL)
                           .setProperty("k0", "v0")
                           .setProperty("k1", "v1")
                           .build(),
                       nullptr);
    producer.sendAsync(MessageBuilder()
                           .setContent("msg-1")
                           .setOrderingKey("ordering-key-1")
                           .setEventTimestamp(11UL)
                           .setProperty("k2", "v2")
                           .build(),
                       nullptr);
    producer.sendAsync(MessageBuilder().setContent("msg-2").build(), nullptr);
    ASSERT_EQ(ResultOk, producer.flush());

    Message msgs[numMessages];
    for (int i = 0; i < numMessages; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        msgs[i] = msg;
        LOG_INFO("message " << i << ": " << msg.getDataAsString()
                            << ", key: " << (msg.hasPartitionKey() ? msg.getPartitionKey() : "(null)")
                            << ", ordering key: " << (msg.hasOrderingKey() ? msg.getOrderingKey() : "(null)")
                            << ", event time: " << (msg.getEventTimestamp())
                            << ", properties count: " << msg.getProperties().size()
                            << ", has schema version: " << msg.hasSchemaVersion());
    }

    ASSERT_EQ(msgs[0].getDataAsString(), "msg-0");
    ASSERT_TRUE(msgs[0].hasPartitionKey());
    ASSERT_EQ(msgs[0].getPartitionKey(), "key-0");
    ASSERT_TRUE(msgs[0].hasOrderingKey());
    ASSERT_EQ(msgs[0].getOrderingKey(), "ordering-key-0");
    ASSERT_EQ(msgs[0].getEventTimestamp(), 10UL);
    ASSERT_EQ(msgs[0].getProperties().size(), 2);
    ASSERT_TRUE(msgs[0].hasProperty("k0"));
    ASSERT_EQ(msgs[0].getProperty("k0"), "v0");
    ASSERT_TRUE(msgs[0].hasProperty("k1"));
    ASSERT_EQ(msgs[0].getProperty("k1"), "v1");
    ASSERT_FALSE(msgs[0].hasSchemaVersion());

    ASSERT_EQ(msgs[1].getDataAsString(), "msg-1");
    ASSERT_FALSE(msgs[1].hasPartitionKey());
    ASSERT_TRUE(msgs[1].hasOrderingKey());
    ASSERT_EQ(msgs[1].getOrderingKey(), "ordering-key-1");
    ASSERT_EQ(msgs[1].getEventTimestamp(), 11UL);
    ASSERT_EQ(msgs[1].getProperties().size(), 1);
    ASSERT_TRUE(msgs[1].hasProperty("k2"));
    ASSERT_EQ(msgs[1].getProperty("k2"), "v2");
    ASSERT_FALSE(msgs[1].hasSchemaVersion());

    ASSERT_EQ(msgs[2].getDataAsString(), "msg-2");
    ASSERT_FALSE(msgs[2].hasPartitionKey());
    ASSERT_FALSE(msgs[2].hasOrderingKey());
    ASSERT_EQ(msgs[2].getEventTimestamp(), 0UL);
    ASSERT_EQ(msgs[2].getProperties().size(), 0);
    ASSERT_FALSE(msgs[2].hasSchemaVersion());

    client.close();
}
