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
#include <thread>

#include "HttpHelper.h"

#include "lib/Future.h"
#include "lib/Utils.h"
#include "lib/Latch.h"
#include "lib/LogUtils.h"
#include "lib/ProducerImpl.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

TEST(ProducerTest, producerNotInitialized) {
    Producer producer;

    Message msg = MessageBuilder().setContent("test").build();

    ASSERT_EQ(ResultProducerNotInitialized, producer.send(msg));

    Promise<Result, MessageId> promise;
    producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    MessageId mi;
    ASSERT_EQ(ResultProducerNotInitialized, promise.getFuture().get(mi));

    ASSERT_EQ(ResultProducerNotInitialized, producer.close());

    Promise<bool, Result> promiseClose;
    producer.closeAsync(WaitForCallback(promiseClose));

    Result result;
    promiseClose.getFuture().get(result);
    ASSERT_EQ(ResultProducerNotInitialized, result);

    ASSERT_TRUE(producer.getTopic().empty());
}

TEST(ProducerTest, exactlyOnceWithProducerNameSpecified) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/exactlyOnceWithProducerNameSpecified";

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration2, producer2));

    Producer producer3;
    Result result = client.createProducer(topicName, producerConfiguration2, producer3);
    ASSERT_EQ(ResultProducerBusy, result);
}

TEST(ProducerTest, testSynchronouslySend) {
    Client client(serviceUrl);
    const std::string topic = "ProducerTestSynchronouslySend";

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub-name", consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    MessageId messageId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build(), messageId));
    LOG_INFO("Send message to " << messageId);

    Message receivedMessage;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMessage, 3000));
    LOG_INFO("Received message from " << receivedMessage.getMessageId());
    ASSERT_EQ(receivedMessage.getMessageId(), messageId);
    ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMessage));

    client.close();
}

TEST(ProducerTest, testIsConnected) {
    Client client(serviceUrl);
    const std::string nonPartitionedTopic =
        "testProducerIsConnectedNonPartitioned-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "testProducerIsConnectedPartitioned-" + std::to_string(time(nullptr));

    Producer producer;
    ASSERT_FALSE(producer.isConnected());
    // ProducerImpl
    ASSERT_EQ(ResultOk, client.createProducer(nonPartitionedTopic, producer));
    ASSERT_TRUE(producer.isConnected());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_FALSE(producer.isConnected());

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // PartitionedProducerImpl
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producer));
    ASSERT_TRUE(producer.isConnected());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_FALSE(producer.isConnected());

    client.close();
}

TEST(ProducerTest, testSendAsyncAfterCloseAsyncWithLazyProducers) {
    Client client(serviceUrl);
    const std::string partitionedTopic =
        "testProducerIsConnectedPartitioned-" + std::to_string(time(nullptr));

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "10");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setLazyStartPartitionedProducers(true);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfiguration, producer));

    Message msg = MessageBuilder().setContent("test").build();

    Promise<bool, Result> promiseClose;
    producer.closeAsync(WaitForCallback(promiseClose));

    Promise<Result, MessageId> promise;
    producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    MessageId mi;
    ASSERT_EQ(ResultAlreadyClosed, promise.getFuture().get(mi));

    Result result;
    promiseClose.getFuture().get(result);
    ASSERT_EQ(ResultOk, result);
}

TEST(ProducerTest, testGetNumOfChunks) {
    ASSERT_EQ(ProducerImpl::getNumOfChunks(11, 5), 3);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(10, 5), 2);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(8, 5), 2);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(4, 5), 1);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(1, 0), 1);
}

TEST(ProducerTest, testBacklogQuotasExceeded) {
    std::string ns = "public/test-backlog-quotas";
    std::string topic = ns + "/testBacklogQuotasExceeded" + std::to_string(time(nullptr));

    int res = makePutRequest(adminUrl + "admin/v2/persistent/" + topic + "/partitions", "5");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    LOG_INFO("Created topic " << topic << " with 5 partitions");

    auto setBacklogPolicy = [&ns](const std::string& policy, int limitSize) {
        const auto body =
            R"({"policy":")" + policy + R"(","limitSize":)" + std::to_string(limitSize) + "}";
        int res = makePostRequest(adminUrl + "admin/v2/namespaces/" + ns + "/backlogQuota", body);
        LOG_INFO(res << " | Change the backlog policy to: " << body);
        ASSERT_TRUE(res == 204 || res == 409);
    };

    Client client(serviceUrl);

    // Create a topic with backlog size that is greater than 1024
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));  // create a cursor
    Producer producer;

    const auto partition = topic + "-partition-0";
    ASSERT_EQ(ResultOk, client.createProducer(partition, producer));
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(std::string(1024L, 'a')).build()));
    ASSERT_EQ(ResultOk, producer.close());

    setBacklogPolicy("producer_request_hold", 1024);
    ASSERT_EQ(ResultProducerBlockedQuotaExceededError, client.createProducer(topic, producer));
    ASSERT_EQ(ResultProducerBlockedQuotaExceededError, client.createProducer(partition, producer));

    setBacklogPolicy("producer_exception", 1024);
    ASSERT_EQ(ResultProducerBlockedQuotaExceededException, client.createProducer(topic, producer));
    ASSERT_EQ(ResultProducerBlockedQuotaExceededException, client.createProducer(partition, producer));

    setBacklogPolicy("consumer_backlog_eviction", 1024);
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    ASSERT_EQ(ResultOk, client.createProducer(partition, producer));

    client.close();
}
