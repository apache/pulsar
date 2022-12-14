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
#include <chrono>
#include <thread>
#include <time.h>
#include <set>
#include <map>
#include <vector>

#include "gtest/gtest.h"

#include "pulsar/Client.h"
#include "PulsarFriend.h"
#include "lib/Future.h"
#include "lib/Utils.h"
#include "lib/LogUtils.h"
#include "lib/MultiTopicsConsumerImpl.h"
#include "HttpHelper.h"

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

DECLARE_LOG_OBJECT()

namespace pulsar {

class ConsumerStateEventListener : public ConsumerEventListener {
   public:
    ConsumerStateEventListener(std::string name) { name_ = name; }

    void becameActive(Consumer consumer, int partitionId) override {
        LOG_INFO("Received consumer active event, partitionId:" << partitionId << ", name: " << name_);
        activeQueue_.push(partitionId);
    }

    void becameInactive(Consumer consumer, int partitionId) override {
        LOG_INFO("Received consumer inactive event, partitionId:" << partitionId << ", name: " << name_);
        inActiveQueue_.push(partitionId);
    }

    std::queue<int> activeQueue_;
    std::queue<int> inActiveQueue_;
    std::string name_;
};

typedef std::shared_ptr<ConsumerStateEventListener> ConsumerStateEventListenerPtr;

void verifyConsumerNotReceiveAnyStateChanges(ConsumerStateEventListenerPtr listener) {
    ASSERT_EQ(0, listener->activeQueue_.size());
    ASSERT_EQ(0, listener->inActiveQueue_.size());
}

void verifyConsumerActive(ConsumerStateEventListenerPtr listener, int partitionId) {
    ASSERT_NE(0, listener->activeQueue_.size());
    int pid = listener->activeQueue_.front();
    listener->activeQueue_.pop();
    ASSERT_EQ(partitionId, pid);
    ASSERT_EQ(0, listener->inActiveQueue_.size());
}

void verifyConsumerInactive(ConsumerStateEventListenerPtr listener, int partitionId) {
    ASSERT_NE(0, listener->inActiveQueue_.size());
    int pid = listener->inActiveQueue_.front();
    listener->inActiveQueue_.pop();
    ASSERT_EQ(partitionId, pid);
    ASSERT_EQ(0, listener->activeQueue_.size());
}

class ActiveInactiveListenerEvent : public ConsumerEventListener {
   public:
    void becameActive(Consumer consumer, int partitionId) override {
        Lock lock(mutex_);
        activePartitonIds_.emplace(partitionId);
        inactivePartitionIds_.erase(partitionId);
    }

    void becameInactive(Consumer consumer, int partitionId) override {
        Lock lock(mutex_);
        activePartitonIds_.erase(partitionId);
        inactivePartitionIds_.emplace(partitionId);
    }

    typedef std::unique_lock<std::mutex> Lock;
    std::set<int> activePartitonIds_;
    std::set<int> inactivePartitionIds_;
    std::mutex mutex_;
};

typedef std::shared_ptr<ActiveInactiveListenerEvent> ActiveInactiveListenerEventPtr;

TEST(ConsumerTest, testConsumerEventWithoutPartition) {
    Client client(lookupUrl);

    const std::string topicName = "testConsumerEventWithoutPartition-topic-" + std::to_string(time(nullptr));
    const std::string subName = "sub";
    const int waitTimeInMs = 1000;
    // constexpr int unAckedMessagesTimeoutMs = 10000;
    // constexpr int tickDurationInMs = 1000;

    // 1. two consumers on the same subscription
    Consumer consumer1;
    ConsumerConfiguration config1;
    ConsumerStateEventListenerPtr listener1 = std::make_shared<ConsumerStateEventListener>("listener-1");
    config1.setConsumerEventListener(listener1);
    config1.setConsumerName("consumer-1");
    config1.setConsumerType(ConsumerType::ConsumerFailover);

    ASSERT_EQ(pulsar::ResultOk, client.subscribe(topicName, subName, config1, consumer1));
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));

    Consumer consumer2;
    ConsumerConfiguration config2;
    ConsumerStateEventListenerPtr listener2 = std::make_shared<ConsumerStateEventListener>("listener-2");
    config2.setConsumerEventListener(listener2);
    config2.setConsumerName("consumer-2");
    config2.setConsumerType(ConsumerType::ConsumerFailover);

    ASSERT_EQ(pulsar::ResultOk, client.subscribe(topicName, subName, config2, consumer2));
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));

    verifyConsumerActive(listener1, -1);
    verifyConsumerInactive(listener2, -1);

    // clear inActiveQueue_
    std::queue<int>().swap(listener2->inActiveQueue_);

    consumer1.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));
    verifyConsumerActive(listener2, -1);
    verifyConsumerNotReceiveAnyStateChanges(listener1);
}

TEST(ConsumerTest, testConsumerEventWithPartition) {
    Client client(lookupUrl);

    const int numPartitions = 4;
    const std::string partitionedTopic =
        "testConsumerEventWithPartition-topic-" + std::to_string(time(nullptr));
    const std::string subName = "sub";
    const int numOfMessages = 100;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // two consumers on the same subscription
    Consumer consumer1;
    ConsumerConfiguration config1;
    ActiveInactiveListenerEventPtr listener1 = std::make_shared<ActiveInactiveListenerEvent>();
    config1.setConsumerEventListener(listener1);
    config1.setConsumerName("consumer-1");
    config1.setConsumerType(ConsumerType::ConsumerFailover);
    config1.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    config1.setTickDurationInMs(tickDurationInMs);
    client.subscribe(partitionedTopic, subName, config1, consumer1);

    Consumer consumer2;
    ConsumerConfiguration config2;
    ActiveInactiveListenerEventPtr listener2 = std::make_shared<ActiveInactiveListenerEvent>();
    config2.setConsumerEventListener(listener2);
    config2.setConsumerName("consumer-2");
    config2.setConsumerType(ConsumerType::ConsumerFailover);
    config1.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    config1.setTickDurationInMs(tickDurationInMs);
    client.subscribe(partitionedTopic, subName, config2, consumer2);

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.flush();
    producer.close();

    // receive message and check partitionIds on consumer1
    std::set<int> receivedPartitionIds;
    while (true) {
        Message msg;
        Result rc = consumer1.receive(msg, 1000);
        if (pulsar::ResultOk != rc) {
            break;
        }

        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        consumer1.acknowledge(msgId);
        receivedPartitionIds.insert(partitionIndex);
    }

    std::set<int> result;
    std::set_difference(listener1->activePartitonIds_.begin(), listener1->activePartitonIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    std::set<int>().swap(result);
    std::set_difference(listener2->inactivePartitionIds_.begin(), listener2->inactivePartitionIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    // receive message and check partitionIds on consumer2
    std::set<int>().swap(receivedPartitionIds);
    while (true) {
        Message msg;
        Result rc = consumer2.receive(msg, 1000);
        if (pulsar::ResultOk != rc) {
            break;
        }
        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        consumer2.acknowledge(msgId);
        receivedPartitionIds.insert(partitionIndex);
    }

    std::set<int>().swap(result);
    std::set_difference(listener2->activePartitonIds_.begin(), listener2->activePartitonIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    std::set<int>().swap(result);
    std::set_difference(listener1->inactivePartitionIds_.begin(), listener1->inactivePartitionIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());
}

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

TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery) {
    Client client(lookupUrl);
    const std::string partitionedTopic =
        "testPartitionedConsumerUnAckedMessageRedelivery" + std::to_string(time(nullptr));
    std::string subName = "sub-partition-consumer-un-acked-msg-redelivery";
    constexpr int numPartitions = 3;
    constexpr int numOfMessages = 15;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumerConfig, consumer));

    MultiTopicsConsumerImplPtr partitionedConsumerImplPtr =
        PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
    ASSERT_EQ(numPartitions, partitionedConsumerImplPtr->consumers_.size());

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.close();

    // receive message and don't acknowledge
    std::set<MessageId> messageIds[numPartitions];
    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));

        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        messageIds[msgId.partition()].emplace(msgId);
    }

    auto partitionedTracker = static_cast<UnAckedMessageTrackerEnabled*>(
        partitionedConsumerImplPtr->unAckedMessageTrackerPtr_.get());
    ASSERT_EQ(numOfMessages, partitionedTracker->size());
    ASSERT_FALSE(partitionedTracker->isEmpty());
    for (auto i = 0; i < numPartitions; i++) {
        auto topicName =
            "persistent://public/default/" + partitionedTopic + "-partition-" + std::to_string(i);
        ASSERT_EQ(numOfMessages / numPartitions, messageIds[i].size());
        auto subConsumerPtr = partitionedConsumerImplPtr->consumers_.find(topicName).value();
        auto tracker =
            static_cast<UnAckedMessageTrackerEnabled*>(subConsumerPtr->unAckedMessageTrackerPtr_.get());
        ASSERT_EQ(0, tracker->size());
        ASSERT_TRUE(tracker->isEmpty());
    }

    // timeout and send redeliver message
    std::this_thread::sleep_for(std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
    ASSERT_EQ(0, partitionedTracker->size());
    ASSERT_TRUE(partitionedTracker->isEmpty());

    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(1, partitionedTracker->size());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, partitionedTracker->size());
    }
    ASSERT_EQ(0, partitionedTracker->size());
    ASSERT_TRUE(partitionedTracker->isEmpty());
    partitionedTracker = NULL;

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}

TEST(ConsumerTest, testPartitionedConsumerUnexpectedAckTimeout) {
    ClientConfiguration clientConfig;
    clientConfig.setMessageListenerThreads(1);
    Client client(lookupUrl, clientConfig);

    const std::string partitionedTopic =
        "testPartitionedConsumerUnexpectedAckTimeout" + std::to_string(time(nullptr));
    std::string subName = "sub";
    constexpr int numPartitions = 2;
    constexpr int numOfMessages = 3;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;
    pulsar::Latch latch(numOfMessages);
    std::vector<Message> messages;
    std::mutex mtx;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setConsumerType(ConsumerShared);
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    consumerConfig.setMessageListener([&](Consumer cons, const Message& msg) {
        // acknowledge received messages immediately, so no ack timeout is expected
        ASSERT_EQ(ResultOk, cons.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, msg.getRedeliveryCount());

        {
            std::lock_guard<std::mutex> lock(mtx);
            messages.emplace_back(msg);
        }

        if (latch.getCount() > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
            latch.countdown();
        }
    });
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumerConfig, consumer));

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.close();

    bool wasUnblocked = latch.wait(
        std::chrono::milliseconds((unAckedMessagesTimeoutMs + tickDurationInMs * 2) * numOfMessages + 5000));
    ASSERT_TRUE(wasUnblocked);

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    // messages are expected not to be redelivered
    ASSERT_EQ(numOfMessages, messages.size());

    consumer.close();
    client.close();
}

TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery) {
    Client client(lookupUrl);
    const std::string nonPartitionedTopic =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic1 =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-par-topic1-" + std::to_string(time(nullptr));
    const std::string partitionedTopic2 =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-par-topic2-" + std::to_string(time(nullptr));
    std::string subName = "sub-multi-topics-consumer-un-acked-msg-redelivery";
    constexpr int numPartitions = 3;
    constexpr int numOfMessages = 15;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic1 + "/partitions", "1");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic2 + "/partitions",
                         std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    const std::vector<std::string> topics = {nonPartitionedTopic, partitionedTopic1, partitionedTopic2};
    ASSERT_EQ(ResultOk, client.subscribe(topics, subName, consumerConfig, consumer));
    MultiTopicsConsumerImplPtr multiTopicsConsumerImplPtr =
        PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
    ASSERT_EQ(numPartitions + 2 /* nonPartitionedTopic + partitionedTopic1 */,
              multiTopicsConsumerImplPtr->consumers_.size());

    // send messages
    auto sendMessageToTopic = [&client](const std::string& topic) {
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

        Message msg = MessageBuilder().setContent("hello").build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    };
    for (int i = 0; i < numOfMessages; i++) {
        sendMessageToTopic(nonPartitionedTopic);
        sendMessageToTopic(partitionedTopic1);
        sendMessageToTopic(partitionedTopic2);
    }

    // receive message and don't acknowledge
    for (auto i = 0; i < numOfMessages * 3; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        MessageId msgId = msg.getMessageId();
    }

    auto multiTopicsTracker = static_cast<UnAckedMessageTrackerEnabled*>(
        multiTopicsConsumerImplPtr->unAckedMessageTrackerPtr_.get());
    ASSERT_EQ(numOfMessages * 3, multiTopicsTracker->size());
    ASSERT_FALSE(multiTopicsTracker->isEmpty());

    std::vector<UnAckedMessageTrackerEnabled*> trackers;
    multiTopicsConsumerImplPtr->consumers_.forEach(
        [&trackers](const std::string& name, const ConsumerImplPtr& consumer) {
            trackers.emplace_back(
                static_cast<UnAckedMessageTrackerEnabled*>(consumer->unAckedMessageTrackerPtr_.get()));
        });
    for (const auto& tracker : trackers) {
        ASSERT_EQ(0, tracker->size());
        ASSERT_TRUE(tracker->isEmpty());
    }

    // timeout and send redeliver message
    std::this_thread::sleep_for(std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
    ASSERT_EQ(0, multiTopicsTracker->size());
    ASSERT_TRUE(multiTopicsTracker->isEmpty());

    for (auto i = 0; i < numOfMessages * 3; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(1, multiTopicsTracker->size());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, multiTopicsTracker->size());
    }
    ASSERT_EQ(0, multiTopicsTracker->size());
    ASSERT_TRUE(multiTopicsTracker->isEmpty());
    multiTopicsTracker = NULL;

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}

TEST(ConsumerTest, testBatchUnAckedMessageTracker) {
    Client client(lookupUrl);
    const std::string topic = "testBatchUnAckedMessageTracker" + std::to_string(time(nullptr));
    std::string subName = "sub-batch-un-acked-msg-tracker";
    constexpr int numOfMessages = 50;
    constexpr int batchSize = 5;
    constexpr int batchCount = numOfMessages / batchSize;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));
    auto consumerImplPtr = PulsarFriend::getConsumerImplPtr(consumer);
    auto tracker =
        static_cast<UnAckedMessageTrackerEnabled*>(consumerImplPtr->unAckedMessageTrackerPtr_.get());

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(true);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setBatchingMaxMessages(batchSize);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        producer.sendAsync(msg, NULL);
    }
    producer.close();

    std::map<MessageId, std::vector<MessageId>> msgIdInBatchMap;
    std::vector<MessageId> messageIds;
    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        MessageId msgId = msg.getMessageId();
        MessageId id(msgId.partition(), msgId.ledgerId(), msgId.entryId(), -1);
        msgIdInBatchMap[id].emplace_back(msgId);
    }

    ASSERT_EQ(batchCount, msgIdInBatchMap.size());
    ASSERT_EQ(batchCount, tracker->size());
    for (const auto& iter : msgIdInBatchMap) {
        ASSERT_EQ(iter.second.size(), batchSize);
    }

    int ackedBatchCount = 0;
    for (auto iter = msgIdInBatchMap.begin(); iter != msgIdInBatchMap.end(); ++iter) {
        ASSERT_EQ(batchSize, iter->second.size());
        for (auto i = 0; i < iter->second.size(); ++i) {
            ASSERT_EQ(ResultOk, consumer.acknowledge(iter->second[i]));
        }
        ackedBatchCount++;
        ASSERT_EQ(batchCount - ackedBatchCount, tracker->size());
    }
    ASSERT_EQ(0, tracker->size());
    ASSERT_TRUE(tracker->isEmpty());

    consumer.close();
    client.close();
}

TEST(ConsumerTest, testGetTopicNameFromReceivedMessage) {
    // topic1 and topic2 are non-partitioned topics, topic3 is a partitioned topic
    const std::string topic1 = "testGetTopicNameFromReceivedMessage1-" + std::to_string(time(nullptr));
    const std::string topic2 = "testGetTopicNameFromReceivedMessage2-" + std::to_string(time(nullptr));
    const std::string topic3 = "testGetTopicNameFromReceivedMessage3-" + std::to_string(time(nullptr));
    int res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + topic3 + "/partitions", "3");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Client client(lookupUrl);

    auto sendMessage = [&client](const std::string& topic, bool enabledBatching) {
        const auto producerConf = ProducerConfiguration().setBatchingEnabled(enabledBatching);
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf, producer));
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build()));
        LOG_INFO("Send 'hello' to " << topic);
    };
    auto validateTopicName = [](Consumer& consumer, const std::string& topic) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));

        const auto fullTopic = "persistent://public/default/" + topic;
        ASSERT_EQ(msg.getTopicName(), fullTopic);
        ASSERT_EQ(msg.getMessageId().getTopicName(), fullTopic);
    };

    // 1. ConsumerImpl
    Consumer consumer1;
    ASSERT_EQ(ResultOk, client.subscribe(topic1, "sub-1", consumer1));

    // 2. MultiTopicsConsumerImpl
    Consumer consumer2;
    ASSERT_EQ(ResultOk, client.subscribe(std::vector<std::string>{topic1, topic2}, "sub-2", consumer2));

    sendMessage(topic1, true);
    validateTopicName(consumer1, topic1);
    validateTopicName(consumer2, topic1);
    sendMessage(topic1, false);
    validateTopicName(consumer1, topic1);
    validateTopicName(consumer2, topic1);

    // 3. PartitionedConsumerImpl
    Consumer consumer3;
    ASSERT_EQ(ResultOk, client.subscribe(topic3, "sub-3", consumer3));
    const auto partition = topic3 + "-partition-0";
    sendMessage(partition, true);
    validateTopicName(consumer3, partition);
    sendMessage(partition, false);
    validateTopicName(consumer3, partition);

    client.close();
}

TEST(ConsumerTest, testIsConnected) {
    Client client(lookupUrl);
    const std::string nonPartitionedTopic1 =
        "testConsumerIsConnectedNonPartitioned1-" + std::to_string(time(nullptr));
    const std::string nonPartitionedTopic2 =
        "testConsumerIsConnectedNonPartitioned2-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "testConsumerIsConnectedPartitioned-" + std::to_string(time(nullptr));
    const std::string subName = "sub";

    Consumer consumer;
    ASSERT_FALSE(consumer.isConnected());
    // ConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(nonPartitionedTopic1, subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());

    // MultiTopicsConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(std::vector<std::string>{nonPartitionedTopic1, nonPartitionedTopic2},
                                         subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // PartitionedConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());
}

}  // namespace pulsar
