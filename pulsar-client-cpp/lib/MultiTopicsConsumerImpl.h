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
#ifndef PULSAR_MULTI_TOPICS_CONSUMER_HEADER
#define PULSAR_MULTI_TOPICS_CONSUMER_HEADER
#include "lib/TestUtil.h"
#include "ConsumerImpl.h"
#include "ClientImpl.h"
#include "BlockingQueue.h"
#include <vector>
#include <queue>
#include <mutex>

#include "ConsumerImplBase.h"
#include "lib/UnAckedMessageTrackerDisabled.h"
#include <lib/Latch.h>
#include <lib/MultiTopicsBrokerConsumerStatsImpl.h>
#include <lib/TopicName.h>
#include <lib/NamespaceName.h>
#include <lib/SynchronizedHashMap.h>

namespace pulsar {
typedef std::shared_ptr<Promise<Result, Consumer>> ConsumerSubResultPromisePtr;

class MultiTopicsConsumerImpl;
class MultiTopicsConsumerImpl : public ConsumerImplBase,
                                public std::enable_shared_from_this<MultiTopicsConsumerImpl> {
   public:
    enum MultiTopicsConsumerState
    {
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };
    MultiTopicsConsumerImpl(ClientImplPtr client, const std::vector<std::string>& topics,
                            const std::string& subscriptionName, TopicNamePtr topicName,
                            const ConsumerConfiguration& conf, LookupServicePtr lookupServicePtr_);
    MultiTopicsConsumerImpl(ClientImplPtr client, TopicNamePtr topicName, int numPartitions,
                            const std::string& subscriptionName, const ConsumerConfiguration& conf,
                            LookupServicePtr lookupServicePtr)
        : MultiTopicsConsumerImpl(client, {topicName->toString()}, subscriptionName, topicName, conf,
                                  lookupServicePtr) {
        topicsPartitions_[topicName->toString()] = numPartitions;
    }
    ~MultiTopicsConsumerImpl();
    // overrided methods from ConsumerImplBase
    Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture() override;
    const std::string& getSubscriptionName() const override;
    const std::string& getTopic() const override;
    Result receive(Message& msg) override;
    Result receive(Message& msg, int timeout) override;
    void receiveAsync(ReceiveCallback& callback) override;
    void unsubscribeAsync(ResultCallback callback) override;
    void acknowledgeAsync(const MessageId& msgId, ResultCallback callback) override;
    void acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) override;
    void closeAsync(ResultCallback callback) override;
    void start() override;
    void shutdown() override;
    bool isClosed() override;
    bool isOpen() override;
    Result pauseMessageListener() override;
    Result resumeMessageListener() override;
    void redeliverUnacknowledgedMessages() override;
    void redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) override;
    const std::string& getName() const override;
    int getNumOfPrefetchedMessages() const override;
    void getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) override;
    void seekAsync(const MessageId& msgId, ResultCallback callback) override;
    void seekAsync(uint64_t timestamp, ResultCallback callback) override;
    void negativeAcknowledge(const MessageId& msgId) override;
    bool isConnected() const override;
    uint64_t getNumberOfConnectedConsumer() override;

    void handleGetConsumerStats(Result, BrokerConsumerStats, LatchPtr, MultiTopicsBrokerConsumerStatsPtr,
                                size_t, BrokerConsumerStatsCallback);
    // return first topic name when all topics name valid, or return null pointer
    static std::shared_ptr<TopicName> topicNamesValid(const std::vector<std::string>& topics);
    void unsubscribeOneTopicAsync(const std::string& topic, ResultCallback callback);
    Future<Result, Consumer> subscribeOneTopicAsync(const std::string& topic);

   protected:
    const ClientImplPtr client_;
    const std::string subscriptionName_;
    std::string consumerStr_;
    std::string topic_;
    const ConsumerConfiguration conf_;
    typedef SynchronizedHashMap<std::string, ConsumerImplPtr> ConsumerMap;
    ConsumerMap consumers_;
    std::map<std::string, int> topicsPartitions_;
    mutable std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    std::atomic<MultiTopicsConsumerState> state_{Pending};
    BlockingQueue<Message> messages_;
    const ExecutorServicePtr listenerExecutor_;
    MessageListener messageListener_;
    DeadlineTimerPtr partitionsUpdateTimer_;
    boost::posix_time::time_duration partitionsUpdateInterval_;
    LookupServicePtr lookupServicePtr_;
    std::shared_ptr<std::atomic<int>> numberTopicPartitions_;
    std::atomic<Result> failedResult{ResultOk};
    Promise<Result, ConsumerImplBaseWeakPtr> multiTopicsConsumerCreatedPromise_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    const std::vector<std::string> topics_;
    std::queue<ReceiveCallback> pendingReceives_;

    /* methods */
    void handleSinglePartitionConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                              unsigned int partitionIndex);
    void notifyResult(CloseCallback closeCallback);
    void messageReceived(Consumer consumer, const Message& msg);
    void internalListener(Consumer consumer);
    void receiveMessages();
    void failPendingReceiveCallback();
    void notifyPendingReceivedCallback(Result result, Message& message, const ReceiveCallback& callback);

    void handleOneTopicSubscribed(Result result, Consumer consumer, const std::string& topic,
                                  std::shared_ptr<std::atomic<int>> topicsNeedCreate);
    void subscribeTopicPartitions(int numPartitions, TopicNamePtr topicName, const std::string& consumerName,
                                  ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleSingleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                     std::shared_ptr<std::atomic<int>> partitionsNeedCreate,
                                     ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleUnsubscribedAsync(Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                 ResultCallback callback);
    void handleOneTopicUnsubscribedAsync(Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                         int numberPartitions, TopicNamePtr topicNamePtr,
                                         std::string& topicPartitionName, ResultCallback callback);
    void runPartitionUpdateTask();
    void topicPartitionUpdate();
    void handleGetPartitions(TopicNamePtr topicName, Result result,
                             const LookupDataResultPtr& lookupDataResult, int currentNumPartitions);
    void subscribeSingleNewConsumer(int numPartitions, TopicNamePtr topicName, int partitionIndex,
                                    ConsumerSubResultPromisePtr topicSubResultPromise,
                                    std::shared_ptr<std::atomic<int>> partitionsNeedCreate);

   private:
    void setNegativeAcknowledgeEnabledForTesting(bool enabled) override;

    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
};

typedef std::shared_ptr<MultiTopicsConsumerImpl> MultiTopicsConsumerImplPtr;
}  // namespace pulsar
#endif  // PULSAR_MULTI_TOPICS_CONSUMER_HEADER
