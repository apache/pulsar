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

#include "ConsumerImpl.h"
#include "MessageImpl.h"
#include "Commands.h"
#include "LogUtils.h"
#include <boost/bind.hpp>
#include "pulsar/Result.h"
#include "pulsar/MessageId.h"
#include "Utils.h"
#include <exception>
#include "DestinationName.h"
#include <algorithm>

using namespace pulsar;
namespace pulsar {

DECLARE_LOG_OBJECT()

ConsumerImpl::ConsumerImpl(const ClientImplPtr client, const std::string& topic,
                           const std::string& subscription, const ConsumerConfiguration& conf,
                           const ExecutorServicePtr listenerExecutor /* = NULL by default */,
                           const ConsumerTopicType consumerTopicType /* = NonPartitioned by default */ )
        : HandlerBase(client, topic),
          waitingForZeroQueueSizeMessage(false),
          config_(conf),
          subscription_(subscription),
          originalSubscriptionName_(subscription),
          messageListener_(config_.getMessageListener()),
          consumerTopicType_(consumerTopicType),
          // This is the initial capacity of the queue
          incomingMessages_(std::max(config_.getReceiverQueueSize(), 1)),
          availablePermits_(conf.getReceiverQueueSize()),
          consumerId_(client->newConsumerId()),
          consumerName_(config_.getConsumerName()),
          partitionIndex_(-1),
          consumerCreatedPromise_(),
          messageListenerRunning_(true),
          batchAcknowledgementTracker_(topic_, subscription, (long)consumerId_) {
    std::stringstream consumerStrStream;
    consumerStrStream << "[" << topic_ << ", " << subscription_ << ", " << consumerId_ << "] ";
    consumerStr_ = consumerStrStream.str();
    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }
    if (listenerExecutor) {
        listenerExecutor_ = listenerExecutor;
    } else {
        listenerExecutor_ = client->getListenerExecutorProvider()->get();
    }
}

ConsumerImpl::~ConsumerImpl() {
    LOG_DEBUG(getName() << "~ConsumerImpl");
    incomingMessages_.clear();
    if (state_ == Ready) {
        LOG_WARN(getName() << "Destroyed consumer which was not properly closed");
        closeAsync(ResultCallback());
    }
}

void ConsumerImpl::setPartitionIndex(int partitionIndex) {
     partitionIndex_ = partitionIndex;
}

int ConsumerImpl::getPartitionIndex() {
    return partitionIndex_;
}

uint64_t ConsumerImpl::getConsumerId() {
    return consumerId_;
}

Future<Result, ConsumerImplBaseWeakPtr> ConsumerImpl::getConsumerCreatedFuture() {
    return consumerCreatedPromise_.getFuture();
}

const std::string& ConsumerImpl::getSubscriptionName() const {
    return originalSubscriptionName_;
}

const std::string& ConsumerImpl::getTopic() const {
    return topic_;
}

void ConsumerImpl::start() {
    grabCnx();
}

void ConsumerImpl::connectionOpened(const ClientConnectionPtr& cnx) {
    Lock lock(mutex_);
    if (state_ == Closed) {
        lock.unlock();
        LOG_DEBUG(getName() << "connectionOpened : Consumer is already closed");
        return;
    }
    lock.unlock();

    ClientImplPtr client = client_.lock();
    int requestId = client->newRequestId();
    SharedBuffer cmd = Commands::newSubscribe(topic_, subscription_, consumerId_, requestId,
                                              getSubType(), consumerName_);
    cnx->sendRequestWithId(cmd, requestId).addListener(
            boost::bind(&ConsumerImpl::handleCreateConsumer, shared_from_this(), cnx, _1));
}

void ConsumerImpl::connectionFailed(Result result) {
    // Keep a reference to ensure object is kept alive
    ConsumerImplPtr ptr = shared_from_this();

    if (consumerCreatedPromise_.setFailed(result)) {
        Lock lock(mutex_);
        state_ = Failed;
    }
}

void ConsumerImpl::receiveMessages(const ClientConnectionPtr& cnx, unsigned int count) {
    SharedBuffer cmd = Commands::newFlow(consumerId_, count);
    cnx->sendCommand(cmd);
}

void ConsumerImpl::handleCreateConsumer(const ClientConnectionPtr& cnx, Result result) {
    static bool firstTime = true;
    if (result == ResultOk) {
        if (firstTime) {
            firstTime = false;
        }
        LOG_INFO(getName() << "Created consumer on broker " << cnx->cnxString());
        connection_ = cnx;
        {
            Lock lock(mutex_);
            incomingMessages_.clear();
            cnx->registerConsumer(consumerId_, shared_from_this());
            state_ = Ready;
            backoff_.reset();
            // Complicated logic since we don't have a isLocked() function for mutex
            if (waitingForZeroQueueSizeMessage) {
                receiveMessages(cnx, 1);
            }
            availablePermits_ = 0;
        }


        LOG_DEBUG(getName() << "Send initial flow permits: " << config_.getReceiverQueueSize());
        if ((consumerTopicType_ == NonPartitioned || !firstTime)
                && config_.getReceiverQueueSize() != 0) {
            receiveMessages(cnx, config_.getReceiverQueueSize());
        }
        consumerCreatedPromise_.setValue(shared_from_this());
    } else {
        if (result == ResultTimeout) {
            // Creating the consumer has timed out. We need to ensure the broker closes the consumer
            // in case it was indeed created, otherwise it might prevent new subscribe operation,
            // since we are not closing the connection
            int requestId = client_.lock()->newRequestId();
            cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId),
                                   requestId);
        }

        if (consumerCreatedPromise_.isComplete()) {
            // Consumer had already been initially created, we need to retry connecting in any case
            LOG_WARN(
                    getName() << "Failed to reconnect consumer: " << strResult(result));
            scheduleReconnection(shared_from_this());
        } else {
            // Consumer was not yet created, retry to connect to broker if it's possible
            if (isRetriableError(result) && (creationTimestamp_ + operationTimeut_ < now())) {
                LOG_WARN(
                        getName() << "Temporary error in creating consumer : " << strResult(result));
                scheduleReconnection(shared_from_this());
            } else {
                LOG_ERROR(getName() << "Failed to create consumer: " << strResult(result));
                consumerCreatedPromise_.setFailed(result);
                state_ = Failed;
            }
        }
    }
}

void ConsumerImpl::unsubscribeAsync(ResultCallback callback) {
    LOG_INFO(getName() << "Unsubscribing");

    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        callback(ResultAlreadyClosed);
        LOG_ERROR(
                getName() << "Can not unsubscribe a closed subscription, please call subscribe again and then call unsubscribe");
        return;
    }

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        LOG_DEBUG(getName() << "Unsubscribe request sent for consumer - " << consumerId_);
        ClientImplPtr client = client_.lock();
        lock.unlock();
        int requestId = client->newRequestId();
        SharedBuffer cmd = Commands::newUnsubscribe(consumerId_, requestId);
        cnx->sendRequestWithId(cmd, requestId).addListener(
                boost::bind(&ConsumerImpl::handleUnsubscribe, shared_from_this(), _1, callback));
    } else {
        Result result = ResultNotConnected;
        lock.unlock();
        LOG_WARN(getName() << "Failed to unsubscribe: " << strResult(result));
        callback(result);
    }
}

void ConsumerImpl::handleUnsubscribe(Result result, ResultCallback callback) {
    if (result == ResultOk) {
        Lock lock(mutex_);
        state_ = Closed;
        LOG_INFO(getName() << "Unsubscribed successfully");
    } else {
        LOG_WARN(getName() << "Failed to unsubscribe: " << strResult(result));
    }
    callback(result);
}

void ConsumerImpl::messageReceived(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                   bool& isChecksumValid, proto::MessageMetadata& metadata,
                                   SharedBuffer& payload) {
    LOG_DEBUG(getName() << "Received Message -- Size: " << payload.readableBytes());

    if (!uncompressMessageIfNeeded(cnx, msg, metadata, payload)) {
        // Message was discarded on decompression error
        return;
    }

    if (!isChecksumValid) {
        // Message discarded for checksum error
        discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::ChecksumMismatch);
        return;
    }

    Message m(msg, metadata, payload);
    m.impl_->messageId.partition_ = partitionIndex_;
    m.impl_->cnx_ = cnx.get();

    LOG_DEBUG(getName() << " metadata.num_messages_in_batch() = "<< metadata.num_messages_in_batch());
    LOG_DEBUG(getName() << " metadata.has_num_messages_in_batch() = "<< metadata.has_num_messages_in_batch());

    unsigned int numOfMessageReceived = 1;
    if (metadata.has_num_messages_in_batch()) {
        Lock lock(mutex_);
        numOfMessageReceived = receiveIndividualMessagesFromBatch(m);
    } else {
        // config_.getReceiverQueueSize() != 0 or waiting For ZeroQueueSize Message`
        if (config_.getReceiverQueueSize() != 0) {
            incomingMessages_.push(m);
        } else {
            Lock lock(mutex_);
            if(waitingForZeroQueueSizeMessage) {
                lock.unlock();
                incomingMessages_.push(m);
            }
        }
    }

    if (messageListener_) {
        Lock lock(messageListenerMutex_);
        if (!messageListenerRunning_) {
            return;
        }
        lock.unlock();
        // Trigger message listener callback in a separate thread
        while(numOfMessageReceived--) {
            listenerExecutor_->postWork(
                boost::bind(&ConsumerImpl::internalListener, shared_from_this()));
        }
    }
}

// Zero Queue size is not supported with Batch Messages
unsigned int ConsumerImpl::receiveIndividualMessagesFromBatch(Message& batchedMessage) {
    unsigned int batchSize = batchedMessage.impl_->metadata.num_messages_in_batch();
    batchAcknowledgementTracker_.receivedMessage(batchedMessage);
    LOG_DEBUG("Received Batch messages of size - " << batchSize);
    for (int i=0; i<batchSize; i++) {
        batchedMessage.impl_->messageId.batchIndex_ = i;
        // This is a cheap copy since message contains only one shared pointer (impl_)
        incomingMessages_.push(Commands::deSerializeSingleMessageInBatch(batchedMessage));
    }
    return batchSize;
}

bool ConsumerImpl::uncompressMessageIfNeeded(const ClientConnectionPtr& cnx,
                                             const proto::CommandMessage& msg,
                                             const proto::MessageMetadata& metadata,
                                             SharedBuffer& payload) {
    if (!metadata.has_compression()) {
        return true;
    }

    CompressionType compressionType = CompressionCodecProvider::convertType(metadata.compression());

    uint32_t uncompressedSize = metadata.uncompressed_size();
    if (uncompressedSize > Commands::MaxMessageSize) {
        // Uncompressed size is itself corrupted since it cannot be bigger than the MaxMessageSize
        LOG_ERROR(getName() << "Got corrupted uncompressed message size " << uncompressedSize  //
                << " at  " << msg.message_id().ledgerid() << ":" << msg.message_id().entryid());
        discardCorruptedMessage(cnx, msg.message_id(),
                                proto::CommandAck::UncompressedSizeCorruption);
        return false;
    }

    if (!CompressionCodecProvider::getCodec(compressionType).decode(payload, uncompressedSize, payload)) {
        LOG_ERROR(getName() << "Failed to decompress message with " << uncompressedSize  //
                << " at  " << msg.message_id().ledgerid() << ":" << msg.message_id().entryid());
        discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::DecompressionError);
        return false;
    }

    return true;
}

void ConsumerImpl::discardCorruptedMessage(const ClientConnectionPtr& cnx,
                                           const proto::MessageIdData& messageId,
                                           proto::CommandAck::ValidationError validationError) {
    LOG_ERROR(
            getName() << "Discarding corrupted message at " << messageId.ledgerid() << ":" << messageId.entryid());

    SharedBuffer cmd  = Commands::newAck(consumerId_, messageId, proto::CommandAck::Individual, validationError);

    cnx->sendCommand(cmd);
    increaseAvailablePermits(cnx);
}

void ConsumerImpl::internalListener() {
    Lock lock(messageListenerMutex_);
    if (!messageListenerRunning_) {
        return;
    }
    lock.unlock();
    Message msg;
    if (!incomingMessages_.pop(msg, boost::posix_time::milliseconds(0))) {
        // This will only happen when the connection got reset and we cleared the queue
        return;
    }
    try {
        messageListener_(Consumer(shared_from_this()), msg);
    } catch (const std::exception& e) {
        LOG_ERROR(getName() << "Exception thrown from listener" << e.what());
    }
    messageProcessed(msg);
}

Result ConsumerImpl::fetchSingleMessageFromBroker(Message& msg) {
    if (config_.getReceiverQueueSize() != 0) {
        LOG_ERROR(getName() << " Can't use receiveForZeroQueueSize if the queue size is not 0");
        return ResultInvalidConfiguration;
    }

    // Using RAII for locking
    ClientConnectionPtr currentCnx = getCnx().lock();
    Lock lock(mutexForReceiveWithZeroQueueSize);

    // Just being cautious
    if (incomingMessages_.size() != 0) {
        LOG_ERROR(
                getName() << "The incoming message queue should never be greater than 0 when Queue size is 0");
        incomingMessages_.clear();
    }
    Lock localLock(mutex_);
    waitingForZeroQueueSizeMessage = true;
    localLock.unlock();

    if (currentCnx) {
        LOG_DEBUG(getName() << "Send more permits: " << 1);
        receiveMessages(currentCnx, 1);
    }

    while (true) {
        incomingMessages_.pop(msg);
        {
            // Lock needed to prevent race between connectionOpened and the check "msg.impl_->cnx_ == currentCnx.get())"
            Lock localLock(mutex_);
            // if message received due to an old flow - discard it and wait for the message from the
            // latest flow command
            if (msg.impl_->cnx_ == currentCnx.get()) {
                waitingForZeroQueueSizeMessage = false;
                // Can't use break here else it may trigger a race with connection opened.
                return ResultOk;
            }
        }
    }
    return ResultOk;
}

Result ConsumerImpl::receive(Message& msg) {
    {
        Lock lock(mutex_);
        if (state_ != Ready) {
            return ResultAlreadyClosed;
        }
    }
    if (messageListener_) {
        LOG_ERROR(getName() << "Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (config_.getReceiverQueueSize() == 0) {
        return fetchSingleMessageFromBroker(msg);
    }

    incomingMessages_.pop(msg);
    messageProcessed(msg);
    unAckedMessageTrackerPtr_->add(msg.getMessageId());
    return ResultOk;
}

Result ConsumerImpl::receive(Message& msg, int timeout) {
    if (config_.getReceiverQueueSize() == 0) {
        LOG_WARN(getName() << "Can't use this function if the queue size is 0");
        return ResultInvalidConfiguration;
    }

    {
        Lock lock(mutex_);
        if (state_ != Ready) {
            return ResultAlreadyClosed;
        }
    }

    if (messageListener_) {
        LOG_ERROR(getName() << "Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (incomingMessages_.pop(msg, milliseconds(timeout))) {
        messageProcessed(msg);
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
        return ResultOk;
    } else {
        return ResultTimeout;
    }
}

void ConsumerImpl::messageProcessed(Message& msg) {
    Lock lock(mutex_);
    ClientConnectionPtr currentCnx = getCnx().lock();
    if (currentCnx && msg.impl_->cnx_ != currentCnx.get()) {
        LOG_DEBUG(getName() << "Not adding permit since connection is different.");
        return;
    }

    increaseAvailablePermits(currentCnx);
}

void ConsumerImpl::increaseAvailablePermits(const ClientConnectionPtr& currentCnx) {
    int additionalPermits =  0;
    if (++availablePermits_ >= config_.getReceiverQueueSize() / 2) {
        additionalPermits = availablePermits_;
        availablePermits_ = 0;
    }
    if (additionalPermits > 0) {
        if (currentCnx) {
            LOG_DEBUG(getName() << "Send more permits: " << additionalPermits);
            receiveMessages(currentCnx, additionalPermits);
        } else {
            LOG_DEBUG(getName() << "Connection is not ready, Unable to send flow Command");
        }
    }
}

inline proto::CommandSubscribe_SubType ConsumerImpl::getSubType() {
    ConsumerType type = config_.getConsumerType();
    switch (type) {
        case ConsumerExclusive:
            return proto::CommandSubscribe_SubType_Exclusive;

        case ConsumerShared:
            return proto::CommandSubscribe_SubType_Shared;

        case ConsumerFailover:
            return proto::CommandSubscribe_SubType_Failover;
    }
}

void ConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    const BatchMessageId& batchMsgId = (const BatchMessageId&)msgId;
    if(batchMsgId.batchIndex_ != -1 && !batchAcknowledgementTracker_.isBatchReady(batchMsgId, proto::CommandAck_AckType_Individual)) {
        callback(ResultOk);
        return;
    }
    doAcknowledge(batchMsgId, proto::CommandAck_AckType_Individual, callback);
}

void ConsumerImpl::acknowledgeCumulativeAsync(const MessageId& mId, ResultCallback callback) {
    const BatchMessageId& msgId = (const BatchMessageId&) mId;
    if(msgId.batchIndex_ != -1 && !batchAcknowledgementTracker_.isBatchReady(msgId, proto::CommandAck_AckType_Cumulative)) {
        BatchMessageId messageId = batchAcknowledgementTracker_.getGreatestCumulativeAckReady(msgId);
        if(messageId == BatchMessageId()) {
            // nothing to ack
            callback(ResultOk);
        } else {
            doAcknowledge(messageId, proto::CommandAck_AckType_Cumulative, callback);
        }
    } else {
        doAcknowledge(msgId, proto::CommandAck_AckType_Cumulative, callback);
    }
}

void ConsumerImpl::doAcknowledge(const BatchMessageId& messageId, proto::CommandAck_AckType ackType,
                                 ResultCallback callback) {

    proto::MessageIdData messageIdData;
    messageIdData.set_ledgerid(messageId.ledgerId_);
    messageIdData.set_entryid(messageId.entryId_);
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        SharedBuffer cmd = Commands::newAck(consumerId_, messageIdData, ackType, -1);
        cnx->sendCommand(cmd);
        if (ackType == proto::CommandAck_AckType_Individual) {
            unAckedMessageTrackerPtr_->remove(messageId);
        } else {
            unAckedMessageTrackerPtr_->removeMessagesTill(messageId);
        }
        batchAcknowledgementTracker_.deleteAckedMessage((BatchMessageId&)messageId, ackType);
        callback(ResultOk);
        LOG_DEBUG(
                getName() << "ack request sent for message - [" << messageIdData.ledgerid() << "," << messageIdData.entryid() << "]");

    } else {
        LOG_DEBUG(getName() << "Connection is not ready, Acknowledge failed for message - ["  //
                << messageIdData.ledgerid() << "," << messageIdData.entryid() << "]");
        callback(ResultNotConnected);
    }
}

void ConsumerImpl::disconnectConsumer() {
    LOG_DEBUG("Broker notification of Closed consumer: " << consumerId_);
    connection_.reset();
    scheduleReconnection(shared_from_this());
}

void ConsumerImpl::closeAsync(ResultCallback callback) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        if (!callback.empty()) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        lock.unlock();
        // If connection is gone, also the consumer is closed on the broker side
        if (!callback.empty()) {
            callback(ResultOk);
        }
        return;
    }


    ClientImplPtr client = client_.lock();
    if (!client) {
        lock.unlock();
        // Client was already destroyed
        if (!callback.empty()) {
            callback(ResultOk);
        }
        return;
    }

    // Lock is no longer required
    lock.unlock();
    int requestId = client->newRequestId();
    Future<Result, std::string> future = cnx->sendRequestWithId(
            Commands::newCloseConsumer(consumerId_, requestId), requestId);
    if (!callback.empty()) {
        future.addListener(
                boost::bind(&ConsumerImpl::handleClose, shared_from_this(), _1, callback));
    }

}

void ConsumerImpl::handleClose(Result result, ResultCallback callback) {
    if (result == ResultOk) {
        Lock lock(mutex_);
        state_ = Closed;
        lock.unlock();

        ClientConnectionPtr cnx = getCnx().lock();
        if (cnx) {
            cnx->removeConsumer(consumerId_);
        }

        LOG_INFO(getName() << "Closed consumer " << consumerId_);
    } else {
        LOG_ERROR(getName() << "Failed to close consumer: " << result);
    }

    callback(result);
}


const std::string& ConsumerImpl::getName() const {
    return consumerStr_;
}

void ConsumerImpl::shutdown() {
    Lock lock(mutex_);
    state_ = Closed;
    lock.unlock();

    consumerCreatedPromise_.setFailed(ResultAlreadyClosed);
}

bool ConsumerImpl::isClosed() {
    Lock lock(mutex_);
    return state_ == Closed;
}

bool ConsumerImpl::isOpen() {
    Lock lock(mutex_);
    return state_ == Ready;
}

Result ConsumerImpl::pauseMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    Lock lock(messageListenerMutex_);
    messageListenerRunning_ = false;
    return ResultOk;
}

Result ConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }

    Lock lock(messageListenerMutex_);
    if (messageListenerRunning_) {
        // Not paused
        return ResultOk;
    }
    messageListenerRunning_ = true;
    const size_t count = incomingMessages_.size();
    lock.unlock();

    for (size_t i = 0; i < count; i++) {
        // Trigger message listener callback in a separate thread
        listenerExecutor_->postWork(
                boost::bind(&ConsumerImpl::internalListener, shared_from_this()));
    }
    return ResultOk;
}

void ConsumerImpl::redeliverUnacknowledgedMessages() {
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if(cnx->getServerProtocolVersion() >= proto::v2) {
            cnx->sendCommand(Commands::newRedeliverUnacknowledgedMessages(consumerId_));
            LOG_DEBUG(
                "Sending RedeliverUnacknowledgedMessages command for Consumer - " << getConsumerId());
        } else {
            LOG_DEBUG("Reconnecting the client to redeliver the messages for Consumer - " << getName());
            cnx->close();
        }
    } else {
        LOG_DEBUG("Connection not ready for Consumer - " << getConsumerId());
    }
}

int ConsumerImpl::getNumOfPrefetchedMessages() const {
    return incomingMessages_.size();
}

} /* namespace pulsar */
