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
#include <lib/LogUtils.h>
#include <pulsar/MessageBuilder.h>
#include <lib/Commands.h>
#include <lib/TopicName.h>
#include <sstream>
#include "CustomRoutingPolicy.h"
#include "lib/Future.h"
#include "lib/Utils.h"
#include <ctime>
#include <thread>
#include "LogUtils.h"
#include "PulsarFriend.h"
#include "ConsumerTest.h"
#include "HttpHelper.h"
DECLARE_LOG_OBJECT();

using namespace pulsar;

static int globalTestBatchMessagesCounter = 0;
static int globalCount = 0;
static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

// ecpoch time in seconds
long epochTime = time(NULL);

static void messageListenerFunction(Consumer consumer, const Message& msg) {
    globalCount++;
    consumer.acknowledge(msg);
}

static void sendCallBack(Result r, const Message& msg) {
    ASSERT_EQ(r, ResultOk);
    globalTestBatchMessagesCounter++;
    LOG_DEBUG("Received publish acknowledgement for " << msg.getDataAsString());
}

static int globalPublishCountSuccess = 0;
static int globalPublishCountQueueFull = 0;

static void sendCallBackExpectingErrors(Result r, const Message& msg) {
    LOG_DEBUG("Received publish acknowledgement for " << msg.getDataAsString() << ", Result = " << r);
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
    } catch (const char* ex) {
        // Ok
    }
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
        Promise<Result, Message> promise;
        producer.sendAsync(msg, WaitForCallbackValue<Message>(promise));
        Message m;
        promise.getFuture().get(m);
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
    globalTestBatchMessagesCounter = 0;

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
    std::string prefix = "12345678";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testSmallReceiverQueueSize) {
    std::string testName = std::to_string(epochTime) + "testSmallReceiverQueueSize";
    globalTestBatchMessagesCounter = 0;

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
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

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
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_DEBUG("sending message " << messageContent);
    }
    globalTestBatchMessagesCounter = 0;
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

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

    globalTestBatchMessagesCounter = 0;

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
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

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

    globalTestBatchMessagesCounter = 0;

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
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

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

    globalTestBatchMessagesCounter = 0;

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
    std::string prefix = testName;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Since all messages are acked
    // Creating 25 new non batched message
    conf.setBatchingEnabled(false);

    client.createProducer(topicName, conf, producer);

    globalTestBatchMessagesCounter = 0;
    // Send Asynchronously
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

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
