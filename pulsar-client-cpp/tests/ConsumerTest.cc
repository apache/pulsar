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
#include <time.h>
#include <set>

#include "../lib/Future.h"
#include "../lib/Utils.h"

#include "HttpHelper.h"

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

TEST(ConsumerTest, consumerNotInitialized) {
    Consumer consumer;

    ASSERT_TRUE(consumer.getTopic().empty());
    ASSERT_TRUE(consumer.getSubscriptionName().empty());

    Message msg;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg, 3000));

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msg));

    MessageId msgId;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msgId));

    Result result;
    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msgId));

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.close());

    {
        Promise<bool, Result> promise;
        consumer.closeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.unsubscribe());

    {
        Promise<bool, Result> promise;
        consumer.unsubscribeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }
}

TEST(ConsumerTest, testPartitionIndex) {
    Client client(lookupUrl);

    const std::string nonPartitionedTopic =
        "ConsumerTestPartitionIndex-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic1 =
        "ConsumerTestPartitionIndex-par-topic1-" + std::to_string(time(nullptr));
    const std::string partitionedTopic2 =
        "ConsumerTestPartitionIndex-par-topic2-" + std::to_string(time(nullptr));
    constexpr int numPartitions = 3;

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic1 + "/partitions", "1");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic2 + "/partitions",
                         std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    auto sendMessageToTopic = [&client](const std::string& topic) {
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

        Message msg = MessageBuilder().setContent("hello").build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    };

    // consumers
    //   [0] subscribes a non-partitioned topic
    //   [1] subscribes a partition of a partitioned topic
    //   [2] subscribes a partitioned topic
    Consumer consumers[3];
    ASSERT_EQ(ResultOk, client.subscribe(nonPartitionedTopic, "sub", consumers[0]));
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic1 + "-partition-0", "sub", consumers[1]));
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic2, "sub", consumers[2]));

    sendMessageToTopic(nonPartitionedTopic);
    sendMessageToTopic(partitionedTopic1);
    for (int i = 0; i < numPartitions; i++) {
        sendMessageToTopic(partitionedTopic2 + "-partition-" + std::to_string(i));
    }

    Message msg;
    ASSERT_EQ(ResultOk, consumers[0].receive(msg, 5000));
    ASSERT_EQ(msg.getMessageId().partition(), -1);

    ASSERT_EQ(ResultOk, consumers[1].receive(msg, 5000));
    ASSERT_EQ(msg.getMessageId().partition(), 0);

    std::set<int> partitionIndexes;
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(ResultOk, consumers[2].receive(msg, 5000));
        partitionIndexes.emplace(msg.getMessageId().partition());
    }
    ASSERT_EQ(partitionIndexes, (std::set<int>{0, 1, 2}));

    client.close();
}
