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
#ifndef LIB_CONSUMERIMPL_H_
#define LIB_CONSUMERIMPL_H_

#include <string>

#include "pulsar/Result.h"
#include "UnboundedBlockingQueue.h"
#include "HandlerBase.h"
#include "ClientConnection.h"
#include "lib/UnAckedMessageTrackerEnabled.h"
#include "NegativeAcksTracker.h"
#include "Commands.h"
#include "ExecutorService.h"
#include "ConsumerImplBase.h"
#include "lib/UnAckedMessageTrackerDisabled.h"
#include "MessageCrypto.h"
#include "AckGroupingTracker.h"

#include "CompressionCodec.h"
#include <boost/dynamic_bitset.hpp>
#include <map>
#include "BatchAcknowledgementTracker.h"
#include <limits>
#include <lib/BrokerConsumerStatsImpl.h>
#include <lib/stats/ConsumerStatsImpl.h>
#include <lib/stats/ConsumerStatsDisabled.h>
#include <queue>
#include <atomic>

using namespace pulsar;

namespace pulsar {
class UnAckedMessageTracker;
class ExecutorService;
class ConsumerImpl;
class BatchAcknowledgementTracker;
typedef std::shared_ptr<MessageCrypto> MessageCryptoPtr;
typedef std::function<void(Result result, MessageId messageId)> BrokerGetLastMessageIdCallback;

enum ConsumerTopicType
{
    NonPartitioned,
    Partitioned
};

class ConsumerImpl : public ConsumerImplBase,
                     public HandlerBase,
                     public std::enable_shared_from_this<ConsumerImpl> {
   public:
    ConsumerImpl(const ClientImplPtr client, const std::string& topic, const std::string& subscriptionName,
                 const ConsumerConfiguration&,
                 const ExecutorServicePtr listenerExecutor = ExecutorServicePtr(), bool hasParent = false,
                 const ConsumerTopicType consumerTopicType = NonPartitioned,
                 Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                 Optional<MessageId> startMessageId = Optional<MessageId>::empty());
    ~ConsumerImpl();
    void setPartitionIndex(int partitionIndex);
    int getPartitionIndex();
    void sendFlowPermitsToBroker(const ClientConnectionPtr& cnx, int numMessages);
    uint64_t getConsumerId();
    void messageReceived(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                         bool& isChecksumValid, proto::MessageMetadata& msgMetadata, SharedBuffer& payload);
    void messageProcessed(Message& msg, bool track = true);
    void activeConsumerChanged(bool isActive);
    inline proto::CommandSubscribe_SubType getSubType();
    inline proto::CommandSubscribe_InitialPosition getInitialPosition();
    void handleUnsubscribe(Result result, ResultCallback callback);

    /**
     * Send individual ACK request of given message ID to broker.
     * @param[in] messageId ID of the message to be ACKed.
     * @param[in] callback call back function, which is called after sending ACK. For now, it's
     *      always provided with ResultOk.
     */
    void doAcknowledgeIndividual(const MessageId& messageId, ResultCallback callback);

    /**
     * Send cumulative ACK request of given message ID to broker.
     * @param[in] messageId ID of the message to be ACKed.
     * @param[in] callback call back function, which is called after sending ACK. For now, it's
     *      always provided with ResultOk.
     */
    void doAcknowledgeCumulative(const MessageId& messageId, ResultCallback callback);

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

    virtual void disconnectConsumer();
    Result fetchSingleMessageFromBroker(Message& msg);

    virtual bool isCumulativeAcknowledgementAllowed(ConsumerType consumerType);

    virtual void redeliverMessages(const std::set<MessageId>& messageIds);

    void handleSeek(Result result, ResultCallback callback);
    virtual bool isReadCompacted();
    virtual void hasMessageAvailableAsync(HasMessageAvailableCallback callback);
    virtual void getLastMessageIdAsync(BrokerGetLastMessageIdCallback callback);

   protected:
    // overrided methods from HandlerBase
    void connectionOpened(const ClientConnectionPtr& cnx) override;
    void connectionFailed(Result result) override;
    HandlerBaseWeakPtr get_weak_from_this() override { return shared_from_this(); }

    void handleCreateConsumer(const ClientConnectionPtr& cnx, Result result);

    void internalListener();

    void internalConsumerChangeListener(bool isActive);

    void handleClose(Result result, ResultCallback callback, ConsumerImplPtr consumer);
    ConsumerStatsBasePtr consumerStatsBasePtr_;

   private:
    bool waitingForZeroQueueSizeMessage;
    bool uncompressMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                   const proto::MessageMetadata& metadata, SharedBuffer& payload);
    void discardCorruptedMessage(const ClientConnectionPtr& cnx, const proto::MessageIdData& messageId,
                                 proto::CommandAck::ValidationError validationError);
    void increaseAvailablePermits(const ClientConnectionPtr& currentCnx, int delta = 1);
    void drainIncomingMessageQueue(size_t count);
    uint32_t receiveIndividualMessagesFromBatch(const ClientConnectionPtr& cnx, Message& batchedMessage,
                                                int redeliveryCount);
    void brokerConsumerStatsListener(Result, BrokerConsumerStatsImpl, BrokerConsumerStatsCallback);

    bool decryptMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                const proto::MessageMetadata& metadata, SharedBuffer& payload);

    // TODO - Convert these functions to lambda when we move to C++11
    Result receiveHelper(Message& msg);
    Result receiveHelper(Message& msg, int timeout);
    void statsCallback(Result, ResultCallback, proto::CommandAck_AckType);
    void notifyPendingReceivedCallback(Result result, Message& message, const ReceiveCallback& callback);
    void failPendingReceiveCallback();
    void setNegativeAcknowledgeEnabledForTesting(bool enabled) override;
    void trackMessage(const Message& msg);

    Optional<MessageId> clearReceiveQueue();

    std::mutex mutexForReceiveWithZeroQueueSize;
    const ConsumerConfiguration config_;
    const std::string subscription_;
    std::string originalSubscriptionName_;
    MessageListener messageListener_;
    ConsumerEventListenerPtr eventListener_;
    ExecutorServicePtr listenerExecutor_;
    bool hasParent_;
    ConsumerTopicType consumerTopicType_;

    Commands::SubscriptionMode subscriptionMode_;
    Optional<MessageId> startMessageId_;

    Optional<MessageId> lastDequedMessage_;
    UnboundedBlockingQueue<Message> incomingMessages_;
    std::queue<ReceiveCallback> pendingReceives_;
    std::atomic_int availablePermits_;
    const int receiverQueueRefillThreshold_;
    uint64_t consumerId_;
    std::string consumerName_;
    std::string consumerStr_;
    int32_t partitionIndex_ = -1;
    Promise<Result, ConsumerImplBaseWeakPtr> consumerCreatedPromise_;
    std::atomic_bool messageListenerRunning_;
    CompressionCodecProvider compressionCodecProvider_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    BatchAcknowledgementTracker batchAcknowledgementTracker_;
    BrokerConsumerStatsImpl brokerConsumerStats_;
    NegativeAcksTracker negativeAcksTracker_;
    AckGroupingTrackerPtr ackGroupingTrackerPtr_;

    MessageCryptoPtr msgCrypto_;
    const bool readCompacted_;

    Optional<MessageId> lastMessageInBroker_;
    void brokerGetLastMessageIdListener(Result res, MessageId messageId,
                                        BrokerGetLastMessageIdCallback callback);

    const MessageId& lastMessageIdDequed() {
        return lastDequedMessage_.is_present() ? lastDequedMessage_.value() : MessageId::earliest();
    }

    const MessageId& lastMessageIdInBroker() {
        return lastMessageInBroker_.is_present() ? lastMessageInBroker_.value() : MessageId::earliest();
    }

    friend class PulsarFriend;

    // these two declared friend to access setNegativeAcknowledgeEnabledForTesting
    friend class MultiTopicsConsumerImpl;
    friend class PartitionedConsumerImpl;

    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testBatchUnAckedMessageTracker);
};

} /* namespace pulsar */

#endif /* LIB_CONSUMERIMPL_H_ */
