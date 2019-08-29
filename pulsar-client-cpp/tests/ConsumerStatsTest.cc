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
#include <lib/Commands.h>
#include "CustomRoutingPolicy.h"
#include "lib/Future.h"
#include "lib/Utils.h"
#include "PulsarFriend.h"
#include "ConsumerTest.h"
#include "HttpHelper.h"
#include <lib/Latch.h>
#include <lib/PartitionedConsumerImpl.h>
#include <lib/TopicName.h>

#include <functional>
#include <thread>
DECLARE_LOG_OBJECT();

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

void partitionedCallbackFunction(Result result, BrokerConsumerStats brokerConsumerStats, long expectedBacklog,
                                 Latch& latch, int index, bool accurate) {
    ASSERT_EQ(result, ResultOk);
    PartitionedBrokerConsumerStatsImpl* statsPtr =
        (PartitionedBrokerConsumerStatsImpl*)(brokerConsumerStats.getImpl().get());
    LOG_DEBUG(statsPtr);
    if (accurate) {
        ASSERT_EQ(expectedBacklog, statsPtr->getBrokerConsumerStats(index).getMsgBacklog());
    } else {
        ASSERT_LE(expectedBacklog, statsPtr->getBrokerConsumerStats(index).getMsgBacklog());
    }
    latch.countdown();
}

void simpleCallbackFunction(Result result, BrokerConsumerStats brokerConsumerStats, Result expectedResult,
                            uint64_t expectedBacklog, ConsumerType expectedConsumerType) {
    LOG_DEBUG(brokerConsumerStats);
    ASSERT_EQ(result, expectedResult);
    ASSERT_EQ(brokerConsumerStats.getMsgBacklog(), expectedBacklog);
    ASSERT_EQ(brokerConsumerStats.getType(), expectedConsumerType);
}
TEST(ConsumerStatsTest, testBacklogInfo) {
    long epochTime = time(NULL);
    std::string testName = "testBacklogInfo-" + std::to_string(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    ConsumerConfiguration conf;
    conf.setBrokerConsumerStatsCacheTimeInMs(3 * 1000);
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, conf, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    client.subscribe(topicName, subName, conf, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 10;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    LOG_DEBUG("Calling consumer.getBrokerConsumerStats");
    consumer.getBrokerConsumerStatsAsync(std::bind(simpleCallbackFunction, std::placeholders::_1,
                                                   std::placeholders::_2, ResultOk, numOfMessages,
                                                   ConsumerExclusive));

    for (int i = numOfMessages; i < (numOfMessages * 2); i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(3500));
    BrokerConsumerStats consumerStats;
    Result res = consumer.getBrokerConsumerStats(consumerStats);
    ASSERT_EQ(res, ResultOk);
    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), 2 * numOfMessages);
    ASSERT_EQ(consumerStats.getType(), ConsumerExclusive);
    consumer.unsubscribe();
}

TEST(ConsumerStatsTest, testFailure) {
    long epochTime = time(NULL);
    std::string testName = "testFailure-" + std::to_string(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    client.subscribe(topicName, subName, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 5;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    ASSERT_EQ(ResultOk, consumer.getBrokerConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), numOfMessages);

    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
}

TEST(ConsumerStatsTest, testCachingMechanism) {
    long epochTime = time(NULL);
    std::string testName = "testCachingMechanism-" + std::to_string(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";
    ConsumerConfiguration conf;
    conf.setBrokerConsumerStatsCacheTimeInMs(3.5 * 1000);
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, conf, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    client.subscribe(topicName, subName, conf, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 5;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    ASSERT_EQ(ResultOk, consumer.getBrokerConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), numOfMessages);

    for (int i = numOfMessages; i < (numOfMessages * 2); i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    LOG_DEBUG("Expecting cached results");
    ASSERT_TRUE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), numOfMessages);

    LOG_DEBUG("Still Expecting cached results");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_TRUE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getBrokerConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), numOfMessages);

    LOG_DEBUG("Now expecting new results");
    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_FALSE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getBrokerConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.getMsgBacklog(), numOfMessages * 2);

    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
}

TEST(ConsumerStatsTest, testAsyncCallOnPartitionedTopic) {
    long epochTime = time(NULL);
    std::string testName = "testAsyncCallOnPartitionedTopic-" + std::to_string(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/" + testName;
    std::string subName = "subscription-name";

    // call admin api to create partitioned topics
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + testName + "/partitions";
    int res = makePutRequest(url, "7");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    ConsumerConfiguration conf;
    conf.setBrokerConsumerStatsCacheTimeInMs(3.5 * 1000);
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, conf, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getBrokerConsumerStats(consumerStats));
    client.subscribe(topicName, subName, conf, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 7 * 5;  // 5 message per partition
    Promise<Result, Producer> producerPromise;
    ProducerConfiguration config;
    config.setBatchingEnabled(false);
    config.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    client.createProducerAsync(topicName, config, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    // Expecting return from 4 callbacks
    Latch latch(4);
    consumer.getBrokerConsumerStatsAsync(std::bind(partitionedCallbackFunction, std::placeholders::_1,
                                                   std::placeholders::_2, 5, latch, 0, true));

    // Now we have 10 messages per partition
    for (int i = numOfMessages; i < (numOfMessages * 2); i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    // Expecting cached result
    // Inaccurate judgment is used because it cannot guarantee that the above operations are completed within
    // cache time.
    consumer.getBrokerConsumerStatsAsync(std::bind(partitionedCallbackFunction, std::placeholders::_1,
                                                   std::placeholders::_2, 5, latch, 0, false));

    std::this_thread::sleep_for(std::chrono::milliseconds(4500));
    // Expecting fresh results
    consumer.getBrokerConsumerStatsAsync(std::bind(partitionedCallbackFunction, std::placeholders::_1,
                                                   std::placeholders::_2, 10, latch, 2, true));

    Message msg;
    while (consumer.receive(msg)) {
        // Do nothing
    }

    // Expecting the backlog to be the same since we didn't acknowledge the messages
    consumer.getBrokerConsumerStatsAsync(std::bind(partitionedCallbackFunction, std::placeholders::_1,
                                                   std::placeholders::_2, 10, latch, 3, true));

    // Wait for ten seconds only
    ASSERT_TRUE(latch.wait(std::chrono::seconds(30)));
}
