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

#include <set>
#include <chrono>
#include <thread>
#include <memory>

#include "HttpHelper.h"
#include "CustomRoutingPolicy.h"

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

static ClientConfiguration newClientConfig(bool enablePartitionsUpdate) {
    ClientConfiguration clientConfig;
    if (enablePartitionsUpdate) {
        clientConfig.setPartititionsUpdateInterval(1);  // 1s
    } else {
        clientConfig.setPartititionsUpdateInterval(0);  // disable
    }
    return clientConfig;
}

// In round robin routing mode, if N messages were sent to a topic with N partitions, each partition must have
// received 1 message. So we check whether producer/consumer have increased along with partitions by checking
// partitions' count of N messages.
// Use std::set because it doesn't allow repeated elements.
class PartitionsSet {
   public:
    size_t size() const { return names_.size(); }

    Result initProducer(std::string topicName, bool enablePartitionsUpdate,
                        bool lazyStartPartitionedProducers) {
        clientForProducer_.reset(new Client(serviceUrl, newClientConfig(enablePartitionsUpdate)));
        const auto producerConfig = ProducerConfiguration()
                                        .setMessageRouter(std::make_shared<SimpleRoundRobinRoutingPolicy>())
                                        .setLazyStartPartitionedProducers(lazyStartPartitionedProducers);
        return clientForProducer_->createProducer(topicName, producerConfig, producer_);
    }

    Result initConsumer(std::string topicName, bool enablePartitionsUpdate) {
        clientForConsumer_.reset(new Client(serviceUrl, newClientConfig(enablePartitionsUpdate)));
        return clientForConsumer_->subscribe(topicName, "SubscriptionName", consumer_);
    }

    void close() {
        producer_.close();
        clientForProducer_->close();
        consumer_.close();
        clientForConsumer_->close();
    }

    void doSendAndReceive(int numMessagesSend, int numMessagesReceive) {
        names_.clear();
        for (int i = 0; i < numMessagesSend; i++) {
            producer_.send(MessageBuilder().setContent("a").build());
        }
        while (numMessagesReceive > 0) {
            Message msg;
            if (consumer_.receive(msg, 100) == ResultOk) {
                names_.emplace(msg.getTopicName());
                consumer_.acknowledge(msg);
                numMessagesReceive--;
            }
        }
    }

   private:
    std::set<std::string> names_;

    std::unique_ptr<Client> clientForProducer_;
    Producer producer_;

    std::unique_ptr<Client> clientForConsumer_;
    Consumer consumer_;
};

static void waitForPartitionsUpdated() {
    // Assume producer and consumer have updated partitions in 3 seconds if enabled
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

TEST(PartitionsUpdateTest, testConfigPartitionsUpdateInterval) {
    ClientConfiguration clientConfig;
    ASSERT_EQ(60, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(0);
    ASSERT_EQ(0, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(1);
    ASSERT_EQ(1, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(-1);
    ASSERT_EQ(static_cast<unsigned int>(-1), clientConfig.getPartitionsUpdateInterval());
}

void testPartitionsUpdate(bool lazyStartPartitionedProducers, std::string topicNameSuffix) {
    std::string topicName = "persistent://" + topicNameSuffix;
    std::string topicOperateUrl = adminUrl + "admin/v2/persistent/" + topicNameSuffix + "/partitions";

    // Ensure `topicName` doesn't exist before created
    makeDeleteRequest(topicOperateUrl);
    // Create a 2 partitions topic
    int res = makePutRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    PartitionsSet partitionsSet;

    // 1. Both producer and consumer enable partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(topicName, true, lazyStartPartitionedProducers));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(topicName, true));

    res = makePostRequest(topicOperateUrl, "3");  // update partitions to 3
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(3, 3);
    ASSERT_EQ(3, partitionsSet.size());
    partitionsSet.close();

    // 2. Only producer enables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(topicName, true, false));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(topicName, false));

    res = makePostRequest(topicOperateUrl, "5");  // update partitions to 5
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(5, 3);  // can't consume partition-3,4
    ASSERT_EQ(3, partitionsSet.size());
    partitionsSet.close();

    // 3. Only consumer enables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(topicName, false, false));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(topicName, true));

    res = makePostRequest(topicOperateUrl, "7");  // update partitions to 7
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(7, 7);
    ASSERT_EQ(5, partitionsSet.size());
    partitionsSet.close();

    // 4. Both producer and consumer disables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(topicName, false, false));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(topicName, false));

    res = makePostRequest(topicOperateUrl, "10");  // update partitions to 10
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(10, 10);
    ASSERT_EQ(7, partitionsSet.size());
    partitionsSet.close();
}

TEST(PartitionsUpdateTest, testPartitionsUpdate) {
    testPartitionsUpdate(false, "public/default/partitions-update-test-topic");
}

TEST(PartitionsUpdateTest, testPartitionsUpdateWithLazyProducers) {
    testPartitionsUpdate(true, "public/default/partitions-update-test-topic-lazy");
}
