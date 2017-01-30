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

#ifndef LIB_CONSUMERIMPL_H_
#define LIB_CONSUMERIMPL_H_

#include <string>
#include "pulsar/BatchMessageId.h"
#include "pulsar/Result.h"
#include "UnboundedBlockingQueue.h"
#include "HandlerBase.h"
#include "boost/enable_shared_from_this.hpp"
#include "ClientConnection.h"
#include <boost/shared_ptr.hpp>
#include "lib/UnAckedMessageTrackerEnabled.h"
#include "Commands.h"
#include "ExecutorService.h"
#include "ConsumerImplBase.h"
#include "lib/UnAckedMessageTrackerDisabled.h"

#include "CompressionCodec.h"
#include <boost/dynamic_bitset.hpp>
#include <map>
#include "BatchAcknowledgementTracker.h"
#include <limits>

using namespace pulsar;

namespace pulsar {
class UnAckedMessageTracker;
class ExecutorService;
class ConsumerImpl;
class BatchAcknowledgementTracker;
typedef boost::shared_ptr<ConsumerImpl> ConsumerImplPtr;
typedef boost::weak_ptr<ConsumerImpl> ConsumerImplWeakPtr;

enum ConsumerTopicType {
    NonPartitioned,
    Partitioned
};

 class ConsumerImpl : public ConsumerImplBase, public HandlerBase, public boost::enable_shared_from_this<ConsumerImpl> {

 public:
    ConsumerImpl(const ClientImplPtr client, const std::string& topic,
                 const std::string& subscription, const ConsumerConfiguration&,
                 const ExecutorServicePtr listenerExecutor = ExecutorServicePtr(),
                 const ConsumerTopicType consumerTopicType = NonPartitioned);
    ~ConsumerImpl();
    void setPartitionIndex(int partitionIndex);
    int getPartitionIndex();
    void receiveMessages(const ClientConnectionPtr& cnx, unsigned int count);
    uint64_t getConsumerId();
    void messageReceived(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                         bool& isChecksumValid, proto::MessageMetadata& msgMetadata,
                         SharedBuffer& payload);
    int incrementAndGetPermits(uint64_t cnxSequenceId);
    void messageProcessed(Message& msg);
    inline proto::CommandSubscribe_SubType getSubType();
    void unsubscribeAsync(ResultCallback callback);
    void handleUnsubscribe(Result result, ResultCallback callback);
    void doAcknowledge(const BatchMessageId& messageId, proto::CommandAck_AckType ackType,
                       ResultCallback callback);
    virtual void disconnectConsumer();
    virtual Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture();
    virtual const std::string& getSubscriptionName() const;
    virtual const std::string& getTopic() const;
    virtual Result receive(Message& msg);
    virtual Result receive(Message& msg, int timeout);
    Result fetchSingleMessageFromBroker(Message& msg);
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
protected:
    void connectionOpened(const ClientConnectionPtr& cnx);
    void connectionFailed(Result result);
    void handleCreateConsumer(const ClientConnectionPtr& cnx, Result result);

    void internalListener();
    void handleClose(Result result, ResultCallback callback);
    virtual HandlerBaseWeakPtr get_weak_from_this() {
        return shared_from_this();
    }
    virtual const std::string& getName() const;
    virtual int getNumOfPrefetchedMessages() const ;
 private:
    bool waitingForZeroQueueSizeMessage;
    bool uncompressMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                   const proto::MessageMetadata& metadata, SharedBuffer& payload);
    void discardCorruptedMessage(const ClientConnectionPtr& cnx,
                                 const proto::MessageIdData& messageId,
                                 proto::CommandAck::ValidationError validationError);
    void increaseAvailablePermits(const ClientConnectionPtr& currentCnx);
    void drainIncomingMessageQueue(size_t count);
    unsigned int receiveIndividualMessagesFromBatch(Message &batchedMessage);

    boost::mutex mutexForReceiveWithZeroQueueSize;
    const ConsumerConfiguration config_;
    const std::string subscription_;
    std::string originalSubscriptionName_;
    MessageListener messageListener_;
    ExecutorServicePtr listenerExecutor_;
    ConsumerTopicType consumerTopicType_;
    UnboundedBlockingQueue<Message> incomingMessages_;
    int availablePermits_;
    uint64_t consumerId_;
    std::string consumerName_;
    std::string consumerStr_;
    short partitionIndex_;
    Promise<Result, ConsumerImplBaseWeakPtr> consumerCreatedPromise_;
    bool messageListenerRunning_;
    boost::mutex messageListenerMutex_;
    CompressionCodecProvider compressionCodecProvider_;
    UnAckedMessageTrackerScopedPtr unAckedMessageTrackerPtr_;
    BatchAcknowledgementTracker batchAcknowledgementTracker_;

};

} /* namespace pulsar */

#endif /* LIB_CONSUMERIMPL_H_ */
