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
#include "DestinationName.h"
#include <lib/Commands.h>
#include "boost/date_time/posix_time/posix_time.hpp"
#include "CustomRoutingPolicy.h"
#include <boost/thread.hpp>
#include "lib/Future.h"
#include "lib/Utils.h"
#include "PulsarFriend.h"
#include "ConsumerTest.h"
#include "HttpHelper.h"
#include <lib/Latch.h>
DECLARE_LOG_OBJECT();

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:8885";
static std::string adminUrl = "http://localhost:8765/";

void callbackFunction(Result result, const BrokerConsumerStats brokerConsumerStats, long expectedBacklog, Latch& latch) {
    LOG_DEBUG(brokerConsumerStats);
    ASSERT_EQ(expectedBacklog, brokerConsumerStats.msgBacklog_);
    latch.countdown();
}

TEST(ConsumerStatsTest, testBacklogInfo) {
	long epochTime=time(NULL);
	std::string testName="testBacklogInfo-" + boost::lexical_cast<std::string>(epochTime);
	Client client(lookupUrl);
	std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    client.subscribe(topicName, subName, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 10;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    LOG_DEBUG("Calling consumer.getConsumerStats");
    BrokerConsumerStats consumerStats;
    Result res = consumer.getConsumerStats(consumerStats);
    ASSERT_EQ(res, ResultOk);

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages);

    for (int i = numOfMessages; i<(numOfMessages*2); i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    usleep(35 * 1000 * 1000);
    res = consumer.getConsumerStats(consumerStats);
    ASSERT_EQ(res, ResultOk);

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, 2 * numOfMessages);
    consumer.unsubscribe();
}

TEST(ConsumerStatsTest, testFailure) {
    long epochTime=time(NULL);
    std::string testName="testFailure-" + boost::lexical_cast<std::string>(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
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
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    ASSERT_EQ(ResultOk, consumer.getConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages);

    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
}

TEST(ConsumerStatsTest, testCachingMechanism) {
    long epochTime=time(NULL);
    std::string testName="testCachingMechanism-" + boost::lexical_cast<std::string>(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
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
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    ASSERT_EQ(ResultOk, consumer.getConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages);

    for (int i = numOfMessages; i<(numOfMessages*2); i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    LOG_DEBUG("Expecting cached results");
    ASSERT_TRUE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getConsumerStats(consumerStats));
    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages);

    LOG_DEBUG("Still Expecting cached results");
    usleep(10 * 1000 * 1000);
    ASSERT_TRUE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages);

    LOG_DEBUG("Now expecting new results");
    usleep(25 * 1000 * 1000);
    ASSERT_FALSE(consumerStats.isValid());
    ASSERT_EQ(ResultOk, consumer.getConsumerStats(consumerStats));

    LOG_DEBUG(consumerStats);
    ASSERT_EQ(consumerStats.msgBacklog_, numOfMessages * 2);

    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
}


TEST(ConsumerStatsTest, testAsyncCallOnPartitionedTopic) {
    long epochTime=time(NULL);
    std::string testName="testAsyncCallOnPartitionedTopic-" + boost::lexical_cast<std::string>(epochTime);
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/" + testName;
    std::string subName = "subscription-name";

    // call admin api to create partitioned topics
    std::string url = adminUrl + "admin/persistent/property/cluster/namespace/" + testName + "/partitions";
    int res = makePutRequest(url, "7");

    LOG_INFO("res = "<<res);
    ASSERT_FALSE(res != 204 && res != 409);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    BrokerConsumerStats consumerStats;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    ASSERT_NE(ResultOk, consumer.getConsumerStats(consumerStats));
    client.subscribe(topicName, subName, consumer);

    // Producing messages
    Producer producer;
    int numOfMessages = 7 * 5; // 5 message per partition
    Promise<Result, Producer> producerPromise;
    ProducerConfiguration config;
    config.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    client.createProducerAsync(topicName, config, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    std::string prefix = testName + "-";
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    // Expecting return from 4 callbacks
    Latch latch(4);
    consumer.getConsumerStatsAsync(boost::bind(callbackFunction, _1, _2, 5, latch), 0);

    BrokerConsumerStats brokerConsumerStats;
    // Expecting error since partitionIndex is invalid
    ASSERT_EQ(ResultInvalidConfiguration, consumer.getConsumerStats(brokerConsumerStats, -1));
    ASSERT_EQ(ResultInvalidConfiguration, consumer.getConsumerStats(brokerConsumerStats, 7));

    // Now we have 10 messages per partition
    for (int i = numOfMessages; i<(numOfMessages*2); i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().build();
        producer.send(msg);
    }

    // Expecting cached result
    consumer.getConsumerStatsAsync(boost::bind(callbackFunction, _1, _2, 5, latch), 0);

    // Expecting fresh results since the partition index is different
    consumer.getConsumerStatsAsync(boost::bind(callbackFunction, _1, _2, 10, latch), 2);

    Message msg;
    while (consumer.receive(msg)) {
        // Do nothing
    }

    // Expecting the backlog to be the same since we didn't acknowledge the messages
    consumer.getConsumerStatsAsync(boost::bind(callbackFunction, _1, _2, 10, latch), 3);

    // Wait for ten seconds only
    ASSERT_TRUE(latch.wait(milliseconds(10 * 1000)));
}