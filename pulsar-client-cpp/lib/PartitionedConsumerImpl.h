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
    virtual ~PartitionedConsumerImpl();
    virtual Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture();
    virtual const std::string& getSubscriptionName() const;
    virtual const std::string& getTopic() const;
    virtual Result receive(Message& msg);
    virtual Result receive(Message& msg, int timeout);
    virtual void receiveAsync(ReceiveCallback& callback);
    virtual void unsubscribeAsync(ResultCallback callback);
    virtual void acknowledgeAsync(const MessageId& msgId, ResultCallback callback);
    virtual void acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback);
    virtual void closeAsync(ResultCallback callback);
    virtual void start();
    virtual void shutdown();
    virtual bool isClosed();
    virtual bool isOpen();
    virtual Result pauseMessageListener();
    virtual Result resumeMessageListener();
    virtual void redeliverUnacknowledgedMessages();
    virtual const std::string& getName() const;
    virtual int getNumOfPrefetchedMessages() const;
    virtual void getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback);
    void handleGetConsumerStats(Result, BrokerConsumerStats, LatchPtr, PartitionedBrokerConsumerStatsPtr,
                                size_t, BrokerConsumerStatsCallback);
    virtual void seekAsync(const MessageId& msgId, ResultCallback callback);

    virtual void negativeAcknowledge(const MessageId& msgId);

   private:
    const ClientImplPtr client_;
    const std::string subscriptionName_;
    const TopicNamePtr topicName_;
    unsigned int numPartitions_;
    unsigned int numConsumersCreated_;
    const ConsumerConfiguration conf_;
    typedef std::vector<ConsumerImplPtr> ConsumerList;
    ConsumerList consumers_;
    std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    PartitionedConsumerState state_;
    unsigned int unsubscribedSoFar_;
    BlockingQueue<Message> messages_;
    ExecutorServicePtr listenerExecutor_;
    MessageListener messageListener_;
    const std::string topic_;
    const std::string name_;
    const std::string partitionStr_;
    /* methods */
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
    Promise<Result, ConsumerImplBaseWeakPtr> partitionedConsumerCreatedPromise_;
    UnAckedMessageTrackerScopedPtr unAckedMessageTrackerPtr_;
    std::queue<ReceiveCallback> pendingReceives_;
};
typedef std::weak_ptr<PartitionedConsumerImpl> PartitionedConsumerImplWeakPtr;
typedef std::shared_ptr<PartitionedConsumerImpl> PartitionedConsumerImplPtr;
}  // namespace pulsar
#endif  // PULSAR_PARTITIONED_CONSUMER_HEADER
