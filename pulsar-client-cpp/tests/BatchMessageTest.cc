/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <boost/lexical_cast.hpp>
#include <lib/LogUtils.h>
#include <pulsar/MessageBuilder.h>
#include "DestinationName.h"
#include <lib/Commands.h>
#include <sstream>
#include "boost/date_time/posix_time/posix_time.hpp"
#include "CustomRoutingPolicy.h"
#include <boost/thread.hpp>
#include "lib/Future.h"
#include "lib/Utils.h"
#include <ctime>
#include "LogUtils.h"
#include "PulsarFriend.h"
#include <unistd.h>
#include "ConsumerTest.h"
#include "HttpHelper.h"
DECLARE_LOG_OBJECT();

using namespace pulsar;

static int globalTestBatchMessagesCounter = 0;
static int globalCount = 0;
static std::string lookupUrl = "pulsar://localhost:8885";
static std::string adminUrl = "http://localhost:8765/";

// ecpoch time in seconds
long epochTime=time(NULL);

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
    ASSERT_DEATH(conf.setBatchingMaxMessages(1), "");
}

TEST(BatchMessageTest, testProducerTimeout) {
    std::string testName=boost::lexical_cast<std::string>(epochTime) + "testProducerTimeout";

    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
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

    // Send Asynchronously
    std::string prefix = "msg-batch-test-produce-timeout-";
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("type", "batch").setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        LOG_INFO("sending message " << messageContent);
        clock_t start, end;
        /* Start the timer */
        start = time(NULL);
        LOG_INFO("start = "<<start);
        producer.send(msg);
        /* End the timer */
        end = time(NULL);
        LOG_INFO("end = "<<end);
        ASSERT_EQ(timeout/1000.0, (double)(end - start));
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_INFO("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(receivedMsg.getProperty("type"), "batch");
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testBatchSizeInBytes) {
    std::string testName=boost::lexical_cast<std::string>(epochTime) + "testBatchSizeInBytes";
    globalTestBatchMessagesCounter=0;


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
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

    // Send Asynchronously
    std::string prefix = "12345678";
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_INFO("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_LT(pulsar::PulsarFriend::getBatchIndex((BatchMessageId&)receivedMsg.getMessageId()),2);
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testSmallReceiverQueueSize) {
    std::string testName=boost::lexical_cast<std::string>(epochTime) + "testSmallReceiverQueueSize";
    globalTestBatchMessagesCounter=0;


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 1000;
    int numOfMessages = 100000;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxPublishDelayMs(1);
    conf.setBatchingEnabled(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(41);

    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig, WaitForCallbackValue<Consumer>(consumerPromise));
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
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_DEBUG("sending message " << messageContent);
    }

    usleep(10 * 1000 * 1000);
    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 10000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BatchMessageTest, testIndividualAck) {
    std::string testName = boost::lexical_cast<std::string>(epochTime) + "testIndividualAck";


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 10;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise < Result, Producer > producerPromise;
    client.createProducerAsync(topicName, conf,
                               WaitForCallbackValue < Producer > (producerPromise));
    Future < Result, Producer > producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(1);

    Promise < Result, Consumer > consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue < Consumer > (consumerPromise));
    Future < Result, Consumer > consumerFuture = consumerPromise.getFuture();
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
        std::string messageContent = prefix + boost::lexical_cast < std::string > (i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty(
                "msgIndex", boost::lexical_cast < std::string > (i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }
    globalTestBatchMessagesCounter = 0;
    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
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
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
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
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i + numOfMessages/2);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++ + numOfMessages/2));
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
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i + numOfMessages/2);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++ + numOfMessages/2));
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
}

TEST(BatchMessageTest, testCumulativeAck) {
    std::string testName = boost::lexical_cast<std::string>(epochTime) + "testCumulativeAck";


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    globalTestBatchMessagesCounter=0;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 15;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise < Result, Producer > producerPromise;
    client.createProducerAsync(topicName, conf,
                               WaitForCallbackValue < Producer > (producerPromise));
    Future < Result, Producer > producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(1);

    Promise < Result, Consumer > consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue < Consumer > (consumerPromise));
    Future < Result, Consumer > consumerFuture = consumerPromise.getFuture();
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
        std::string messageContent = prefix + boost::lexical_cast < std::string > (i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty(
                "msgIndex", boost::lexical_cast < std::string > (i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Cumm. Ack 7th message
        if (i == 7) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }
    // Number of messages produced
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

    // Unsubscribe and resubscribe
    // Expecting 10 messages to be sent again

    consumer.close();
    client.subscribe(topicName, subName, consumerConfig, consumer);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i + 5);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++ + 5));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        // Ack 10th message
        if (i == 10) {
            ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(receivedMsg));
        }
    }

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
    std::string testName = boost::lexical_cast<std::string>(epochTime) + "testMixedAck";


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    globalTestBatchMessagesCounter=0;

    // Enable batching on producer side
    int batchSize = 5;
    int numOfMessages = 15;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);

    Promise < Result, Producer > producerPromise;
    client.createProducerAsync(topicName, conf,
                               WaitForCallbackValue < Producer > (producerPromise));
    Future < Result, Producer > producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    Promise < Result, Consumer > consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue < Consumer > (consumerPromise));
    Future < Result, Consumer > consumerFuture = consumerPromise.getFuture();
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
        std::string messageContent = prefix + boost::lexical_cast < std::string > (i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty(
                "msgIndex", boost::lexical_cast < std::string > (i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
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
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i + 10);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++ + 10));
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
    std::string testName = boost::lexical_cast<std::string>(epochTime) + "testPermits";


    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Producer producer;

    globalTestBatchMessagesCounter=0;

    // Enable batching on producer side
    int batchSize = 10;
    int numOfMessages = 75;
    ProducerConfiguration conf;
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingMaxPublishDelayMs(5);
    conf.setBatchingEnabled(true);

    Promise < Result, Producer > producerPromise;
    client.createProducerAsync(topicName, conf,
                               WaitForCallbackValue < Producer > (producerPromise));
    Future < Result, Producer > producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setReceiverQueueSize(5);

    Promise < Result, Consumer > consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue < Consumer > (consumerPromise));
    Future < Result, Consumer > consumerFuture = consumerPromise.getFuture();
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
        std::string messageContent = prefix + boost::lexical_cast < std::string > (i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty(
                "msgIndex", boost::lexical_cast < std::string > (i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }

    usleep(5 * 1000 * 1000);

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
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
        std::string messageContent = prefix + boost::lexical_cast < std::string > (i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty(
                "msgIndex", boost::lexical_cast < std::string > (i)).build();
        producer.sendAsync(msg, &sendCallBack);
        LOG_INFO("sending message " << messageContent);
    }
    usleep(5 * 1000 * 1000);

    ASSERT_LE(ConsumerTest::getNumOfMessagesInQueue(consumer), consumerConfig.getReceiverQueueSize());
    ASSERT_GE(ConsumerTest::getNumOfMessagesInQueue(consumer), consumerConfig.getReceiverQueueSize()/2);

    i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast < std::string > (i);
        LOG_INFO(
                "Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast < std::string > (i++));
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
    std::string topicName = "persistent://property/cluster/namespace/test-partitioned-batch-messages-" + boost::lexical_cast<std::string>(epochTime) ;

    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/test-partitioned-batch-messages-"
      + boost::lexical_cast<std::string>(epochTime) + "/partitions";
    int res = makePutRequest(lookupUrl, "7");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }

    usleep(2 * 1000 * 1000);

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
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        producer.sendAsync(msg, &sendCallBackExpectingErrors);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 30000) == ResultOk) {
        LOG_INFO("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
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
