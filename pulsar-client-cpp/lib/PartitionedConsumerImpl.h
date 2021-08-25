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
#ifndef PULSAR_PARTITIONED_CONSUMER_HEADER
#define PULSAR_PARTITIONED_CONSUMER_HEADER
#include "lib/TestUtil.h"
#include "ConsumerImpl.h"
#include "ClientImpl.h"
#include <vector>
#include <queue>

#include <mutex>
#include "ConsumerImplBase.h"
#include "lib/UnAckedMessageTrackerDisabled.h"
#include <lib/Latch.h>
#include <lib/PartitionedBrokerConsumerStatsImpl.h>
#include <lib/TopicName.h>

namespace pulsar {
class PartitionedConsumerImpl;
class PartitionedConsumerImpl : public ConsumerImplBase,
                                public std::enable_shared_from_this<PartitionedConsumerImpl> {
   public:
    enum PartitionedConsumerState
    {
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };
    PartitionedConsumerImpl(ClientImplPtr client, const std::string& subscriptionName,
                            const TopicNamePtr topicName, const unsigned int numPartitions,
                            const ConsumerConfiguration& conf);
    ~PartitionedConsumerImpl();
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

    void handleGetConsumerStats(Result, BrokerConsumerStats, LatchPtr, PartitionedBrokerConsumerStatsPtr,
                                size_t, BrokerConsumerStatsCallback);

   private:
    const ClientImplPtr client_;
    const std::string subscriptionName_;
    const TopicNamePtr topicName_;
    unsigned int numPartitions_;
    unsigned int numConsumersCreated_ = 0;
    const ConsumerConfiguration conf_;
    typedef std::vector<ConsumerImplPtr> ConsumerList;
    ConsumerList consumers_;
    // consumersMutex_ is used to share consumers_ and numPartitions_
    mutable std::mutex consumersMutex_;
    mutable std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    PartitionedConsumerState state_ = Pending;
    unsigned int unsubscribedSoFar_ = 0;
    BlockingQueue<Message> messages_;
    ExecutorServicePtr listenerExecutor_;
    MessageListener messageListener_;
    const std::string topic_;
    const std::string name_;
    const std::string partitionStr_;
    ExecutorServicePtr internalListenerExecutor_;
    DeadlineTimerPtr partitionsUpdateTimer_;
    boost::posix_time::time_duration partitionsUpdateInterval_;
    LookupServicePtr lookupServicePtr_;

    unsigned int getNumPartitions() const;
    unsigned int getNumPartitionsWithLock() const;
    ConsumerConfiguration getSinglePartitionConsumerConfig() const;
    ConsumerImplPtr newInternalConsumer(unsigned int partition, const ConsumerConfiguration& config) const;
    void setState(PartitionedConsumerState state);
    void handleUnsubscribeAsync(Result result, unsigned int consumerIndex, ResultCallback callback);
    void handleSinglePartitionConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                              unsigned int partitionIndex);
    void handleSinglePartitionConsumerClose(Result result, unsigned int partitionIndex,
                                            CloseCallback callback);
    void notifyResult(CloseCallback closeCallback);
    void messageReceived(Consumer consumer, const Message& msg);
    void internalListener(Consumer consumer);
    void receiveMessages();
    void failPendingReceiveCallback();
    void setNegativeAcknowledgeEnabledForTesting(bool enabled) override;
    Promise<Result, ConsumerImplBaseWeakPtr> partitionedConsumerCreatedPromise_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    std::queue<ReceiveCallback> pendingReceives_;
    void runPartitionUpdateTask();
    void getPartitionMetadata();
    void handleGetPartitions(const Result result, const LookupDataResultPtr& lookupDataResult);

    friend class PulsarFriend;

    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
};
typedef std::weak_ptr<PartitionedConsumerImpl> PartitionedConsumerImplWeakPtr;
typedef std::shared_ptr<PartitionedConsumerImpl> PartitionedConsumerImplPtr;
}  // namespace pulsar
#endif  // PULSAR_PARTITIONED_CONSUMER_HEADER
