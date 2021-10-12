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
#include "ConsumerImpl.h"
#include "MessageImpl.h"
#include "Commands.h"
#include "LogUtils.h"
#include "TimeUtils.h"
#include <lib/TopicName.h>
#include "pulsar/Result.h"
#include "pulsar/MessageId.h"
#include "Utils.h"
#include "AckGroupingTracker.h"
#include "AckGroupingTrackerEnabled.h"
#include "AckGroupingTrackerDisabled.h"
#include <exception>
#include <algorithm>

namespace pulsar {

DECLARE_LOG_OBJECT()

ConsumerImpl::ConsumerImpl(const ClientImplPtr client, const std::string& topic,
                           const std::string& subscriptionName, const ConsumerConfiguration& conf,
                           const ExecutorServicePtr listenerExecutor /* = NULL by default */,
                           bool hasParent /* = false by default */,
                           const ConsumerTopicType consumerTopicType /* = NonPartitioned by default */,
                           Commands::SubscriptionMode subscriptionMode, Optional<MessageId> startMessageId)
    : HandlerBase(client, topic, Backoff(milliseconds(100), seconds(60), milliseconds(0))),
      waitingForZeroQueueSizeMessage(false),
      config_(conf),
      subscription_(subscriptionName),
      originalSubscriptionName_(subscriptionName),
      messageListener_(config_.getMessageListener()),
      eventListener_(config_.getConsumerEventListener()),
      hasParent_(hasParent),
      consumerTopicType_(consumerTopicType),
      subscriptionMode_(subscriptionMode),
      startMessageId_(startMessageId),
      // This is the initial capacity of the queue
      incomingMessages_(std::max(config_.getReceiverQueueSize(), 1)),
      availablePermits_(0),
      receiverQueueRefillThreshold_(config_.getReceiverQueueSize() / 2),
      consumerId_(client->newConsumerId()),
      consumerName_(config_.getConsumerName()),
      messageListenerRunning_(true),
      batchAcknowledgementTracker_(topic_, subscriptionName, (long)consumerId_),
      negativeAcksTracker_(client, *this, conf),
      ackGroupingTrackerPtr_(std::make_shared<AckGroupingTracker>()),
      readCompacted_(conf.isReadCompacted()),
      lastMessageInBroker_(Optional<MessageId>::of(MessageId())) {
    std::stringstream consumerStrStream;
    consumerStrStream << "[" << topic_ << ", " << subscription_ << ", " << consumerId_ << "] ";
    consumerStr_ = consumerStrStream.str();

    // Initialize un-ACKed messages OT tracker.
    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        if (conf.getTickDurationInMs() > 0) {
            unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
                conf.getUnAckedMessagesTimeoutMs(), conf.getTickDurationInMs(), client, *this));
        } else {
            unAckedMessageTrackerPtr_.reset(
                new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
        }
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }

    // Initialize listener executor.
    if (listenerExecutor) {
        listenerExecutor_ = listenerExecutor;
    } else {
        listenerExecutor_ = client->getListenerExecutorProvider()->get();
    }

    // Setup stats reporter.
    unsigned int statsIntervalInSeconds = client->getClientConfig().getStatsIntervalInSeconds();
    if (statsIntervalInSeconds) {
        consumerStatsBasePtr_ = std::make_shared<ConsumerStatsImpl>(
            consumerStr_, client->getIOExecutorProvider()->get(), statsIntervalInSeconds);
    } else {
        consumerStatsBasePtr_ = std::make_shared<ConsumerStatsDisabled>();
    }

    // Create msgCrypto
    if (conf.isEncryptionEnabled()) {
        msgCrypto_ = std::make_shared<MessageCrypto>(consumerStr_, false);
    }
}

ConsumerImpl::~ConsumerImpl() {
    LOG_DEBUG(getName() << "~ConsumerImpl");
    incomingMessages_.clear();
    if (state_ == Ready) {
        // this could happen at least in this condition:
        //      consumer seek, caused reconnection, if consumer close happened before connection ready,
        //      then consumer will not send closeConsumer to Broker side, and caused a leak of consumer in
        //      broker.
        LOG_WARN(getName() << "Destroyed consumer which was not properly closed");

        ClientConnectionPtr cnx = getCnx().lock();
        ClientImplPtr client = client_.lock();
        int requestId = client->newRequestId();
        if (cnx) {
            cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId);
            cnx->removeConsumer(consumerId_);
            LOG_INFO(getName() << "Closed consumer for race condition: " << consumerId_);
        }
    }
}

void ConsumerImpl::setPartitionIndex(int partitionIndex) { partitionIndex_ = partitionIndex; }

int ConsumerImpl::getPartitionIndex() { return partitionIndex_; }

uint64_t ConsumerImpl::getConsumerId() { return consumerId_; }

Future<Result, ConsumerImplBaseWeakPtr> ConsumerImpl::getConsumerCreatedFuture() {
    return consumerCreatedPromise_.getFuture();
}

const std::string& ConsumerImpl::getSubscriptionName() const { return originalSubscriptionName_; }

const std::string& ConsumerImpl::getTopic() const { return topic_; }

void ConsumerImpl::start() {
    HandlerBase::start();

    // Initialize ackGroupingTrackerPtr_ here because the shared_from_this() was not initialized until the
    // constructor completed.
    if (TopicName::get(topic_)->isPersistent()) {
        if (config_.getAckGroupingTimeMs() > 0) {
            ackGroupingTrackerPtr_.reset(new AckGroupingTrackerEnabled(
                client_.lock(), shared_from_this(), consumerId_, config_.getAckGroupingTimeMs(),
                config_.getAckGroupingMaxSize()));
        } else {
            ackGroupingTrackerPtr_.reset(new AckGroupingTrackerDisabled(*this, consumerId_));
        }
    } else {
        LOG_INFO(getName() << "ACK will NOT be sent to broker for this non-persistent topic.");
    }
    ackGroupingTrackerPtr_->start();
}

void ConsumerImpl::connectionOpened(const ClientConnectionPtr& cnx) {
    Lock lock(mutex_);
    if (state_ == Closed) {
        lock.unlock();
        LOG_DEBUG(getName() << "connectionOpened : Consumer is already closed");
        return;
    }

    // Register consumer so that we can handle other incomming commands (e.g. ACTIVE_CONSUMER_CHANGE) after
    // sending the subscribe request.
    cnx->registerConsumer(consumerId_, shared_from_this());

    Optional<MessageId> firstMessageInQueue = clearReceiveQueue();
    unAckedMessageTrackerPtr_->clear();
    batchAcknowledgementTracker_.clear();

    if (subscriptionMode_ == Commands::SubscriptionModeNonDurable) {
        // Update startMessageId so that we can discard messages after delivery
        // restarts
        startMessageId_ = firstMessageInQueue;
    }

    lock.unlock();

    ClientImplPtr client = client_.lock();
    uint64_t requestId = client->newRequestId();
    SharedBuffer cmd = Commands::newSubscribe(
        topic_, subscription_, consumerId_, requestId, getSubType(), consumerName_, subscriptionMode_,
        startMessageId_, readCompacted_, config_.getProperties(), config_.getSchema(), getInitialPosition(),
        config_.isReplicateSubscriptionStateEnabled(), config_.getKeySharedPolicy(),
        config_.getPriorityLevel());
    cnx->sendRequestWithId(cmd, requestId)
        .addListener(
            std::bind(&ConsumerImpl::handleCreateConsumer, shared_from_this(), cnx, std::placeholders::_1));
}

void ConsumerImpl::connectionFailed(Result result) {
    // Keep a reference to ensure object is kept alive
    ConsumerImplPtr ptr = shared_from_this();

    if (consumerCreatedPromise_.setFailed(result)) {
        Lock lock(mutex_);
        state_ = Failed;
    }
}

void ConsumerImpl::sendFlowPermitsToBroker(const ClientConnectionPtr& cnx, int numMessages) {
    if (cnx && numMessages > 0) {
        LOG_DEBUG(getName() << "Send more permits: " << numMessages);
        SharedBuffer cmd = Commands::newFlow(consumerId_, static_cast<unsigned int>(numMessages));
        cnx->sendCommand(cmd);
    }
}

void ConsumerImpl::handleCreateConsumer(const ClientConnectionPtr& cnx, Result result) {
    static bool firstTime = true;
    if (result == ResultOk) {
        if (firstTime) {
            firstTime = false;
        }
        LOG_INFO(getName() << "Created consumer on broker " << cnx->cnxString());
        {
            Lock lock(mutex_);
            connection_ = cnx;
            incomingMessages_.clear();
            state_ = Ready;
            backoff_.reset();
            // Complicated logic since we don't have a isLocked() function for mutex
            if (waitingForZeroQueueSizeMessage) {
                sendFlowPermitsToBroker(cnx, 1);
            }
            availablePermits_ = 0;
        }

        LOG_DEBUG(getName() << "Send initial flow permits: " << config_.getReceiverQueueSize());
        if (consumerTopicType_ == NonPartitioned || !firstTime) {
            if (config_.getReceiverQueueSize() != 0) {
                sendFlowPermitsToBroker(cnx, config_.getReceiverQueueSize());
            } else if (messageListener_) {
                sendFlowPermitsToBroker(cnx, 1);
            }
        }
        consumerCreatedPromise_.setValue(shared_from_this());
    } else {
        if (result == ResultTimeout) {
            // Creating the consumer has timed out. We need to ensure the broker closes the consumer
            // in case it was indeed created, otherwise it might prevent new subscribe operation,
            // since we are not closing the connection
            int requestId = client_.lock()->newRequestId();
            cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId);
        }

        if (consumerCreatedPromise_.isComplete()) {
            // Consumer had already been initially created, we need to retry connecting in any case
            LOG_WARN(getName() << "Failed to reconnect consumer: " << strResult(result));
            scheduleReconnection(shared_from_this());
        } else {
            // Consumer was not yet created, retry to connect to broker if it's possible
            if (isRetriableError(result) && (creationTimestamp_ + operationTimeut_ < TimeUtils::now())) {
                LOG_WARN(getName() << "Temporary error in creating consumer : " << strResult(result));
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
        LOG_ERROR(getName() << "Can not unsubscribe a closed subscription, please call subscribe again and "
                               "then call unsubscribe");
        return;
    }

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        LOG_DEBUG(getName() << "Unsubscribe request sent for consumer - " << consumerId_);
        ClientImplPtr client = client_.lock();
        lock.unlock();
        int requestId = client->newRequestId();
        SharedBuffer cmd = Commands::newUnsubscribe(consumerId_, requestId);
        cnx->sendRequestWithId(cmd, requestId)
            .addListener(std::bind(&ConsumerImpl::handleUnsubscribe, shared_from_this(),
                                   std::placeholders::_1, callback));
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

    if (!decryptMessageIfNeeded(cnx, msg, metadata, payload)) {
        // Message was discarded or not consumed due to decryption failure
        return;
    }

    if (!uncompressMessageIfNeeded(cnx, msg, metadata, payload)) {
        // Message was discarded on decompression error
        return;
    }

    if (!isChecksumValid) {
        // Message discarded for checksum error
        discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::ChecksumMismatch);
        return;
    }

    Message m(msg, metadata, payload, partitionIndex_);
    m.impl_->cnx_ = cnx.get();
    m.impl_->setTopicName(topic_);
    m.impl_->setRedeliveryCount(msg.redelivery_count());

    if (metadata.has_schema_version()) {
        m.impl_->setSchemaVersion(metadata.schema_version());
    }

    LOG_DEBUG(getName() << " metadata.num_messages_in_batch() = " << metadata.num_messages_in_batch());
    LOG_DEBUG(getName() << " metadata.has_num_messages_in_batch() = "
                        << metadata.has_num_messages_in_batch());

    uint32_t numOfMessageReceived = m.impl_->metadata.num_messages_in_batch();
    if (this->ackGroupingTrackerPtr_->isDuplicate(m.getMessageId())) {
        LOG_DEBUG(getName() << " Ignoring message as it was ACKed earlier by same consumer.");
        increaseAvailablePermits(cnx, numOfMessageReceived);
        return;
    }

    if (metadata.has_num_messages_in_batch()) {
        Lock lock(mutex_);
        numOfMessageReceived = receiveIndividualMessagesFromBatch(cnx, m, msg.redelivery_count());
    } else {
        Lock lock(pendingReceiveMutex_);
        // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
        bool asyncReceivedWaiting = !pendingReceives_.empty();
        ReceiveCallback callback;
        if (asyncReceivedWaiting) {
            callback = pendingReceives_.front();
            pendingReceives_.pop();
        }
        lock.unlock();

        if (asyncReceivedWaiting) {
            listenerExecutor_->postWork(std::bind(&ConsumerImpl::notifyPendingReceivedCallback,
                                                  shared_from_this(), ResultOk, m, callback));
            return;
        }

        // config_.getReceiverQueueSize() != 0 or waiting For ZeroQueueSize Message`
        if (config_.getReceiverQueueSize() != 0 ||
            (config_.getReceiverQueueSize() == 0 && messageListener_)) {
            incomingMessages_.push(m);
        } else {
            Lock lock(mutex_);
            if (waitingForZeroQueueSizeMessage) {
                lock.unlock();
                incomingMessages_.push(m);
            }
        }
    }

    if (messageListener_) {
        if (!messageListenerRunning_) {
            return;
        }
        // Trigger message listener callback in a separate thread
        while (numOfMessageReceived--) {
            listenerExecutor_->postWork(std::bind(&ConsumerImpl::internalListener, shared_from_this()));
        }
    }
}

void ConsumerImpl::activeConsumerChanged(bool isActive) {
    if (eventListener_) {
        listenerExecutor_->postWork(
            std::bind(&ConsumerImpl::internalConsumerChangeListener, shared_from_this(), isActive));
    }
}

void ConsumerImpl::internalConsumerChangeListener(bool isActive) {
    try {
        if (isActive) {
            eventListener_->becameActive(Consumer(shared_from_this()), partitionIndex_);
        } else {
            eventListener_->becameInactive(Consumer(shared_from_this()), partitionIndex_);
        }
    } catch (const std::exception& e) {
        LOG_ERROR(getName() << "Exception thrown from event listener " << e.what());
    }
}

void ConsumerImpl::failPendingReceiveCallback() {
    Message msg;
    Lock lock(pendingReceiveMutex_);
    while (!pendingReceives_.empty()) {
        ReceiveCallback callback = pendingReceives_.front();
        pendingReceives_.pop();
        listenerExecutor_->postWork(std::bind(&ConsumerImpl::notifyPendingReceivedCallback,
                                              shared_from_this(), ResultAlreadyClosed, msg, callback));
    }
    lock.unlock();
}

void ConsumerImpl::notifyPendingReceivedCallback(Result result, Message& msg,
                                                 const ReceiveCallback& callback) {
    if (result == ResultOk && config_.getReceiverQueueSize() != 0) {
        messageProcessed(msg);
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
    }
    callback(result, msg);
}

// Zero Queue size is not supported with Batch Messages
uint32_t ConsumerImpl::receiveIndividualMessagesFromBatch(const ClientConnectionPtr& cnx,
                                                          Message& batchedMessage, int redeliveryCount) {
    unsigned int batchSize = batchedMessage.impl_->metadata.num_messages_in_batch();
    batchAcknowledgementTracker_.receivedMessage(batchedMessage);
    LOG_DEBUG("Received Batch messages of size - " << batchSize
                                                   << " -- msgId: " << batchedMessage.getMessageId());

    int skippedMessages = 0;

    for (int i = 0; i < batchSize; i++) {
        // This is a cheap copy since message contains only one shared pointer (impl_)
        Message msg = Commands::deSerializeSingleMessageInBatch(batchedMessage, i);
        msg.impl_->setRedeliveryCount(redeliveryCount);
        msg.impl_->setTopicName(batchedMessage.getTopicName());

        if (startMessageId_.is_present()) {
            const MessageId& msgId = msg.getMessageId();

            // If we are receiving a batch message, we need to discard messages that were prior
            // to the startMessageId
            if (msgId.ledgerId() == startMessageId_.value().ledgerId() &&
                msgId.entryId() == startMessageId_.value().entryId() &&
                msgId.batchIndex() <= startMessageId_.value().batchIndex()) {
                LOG_DEBUG(getName() << "Ignoring message from before the startMessageId"
                                    << msg.getMessageId());
                ++skippedMessages;
                continue;
            }
        }

        //
        Lock lock(pendingReceiveMutex_);
        if (!pendingReceives_.empty()) {
            ReceiveCallback callback = pendingReceives_.front();
            pendingReceives_.pop();
            lock.unlock();
            listenerExecutor_->postWork(std::bind(&ConsumerImpl::notifyPendingReceivedCallback,
                                                  shared_from_this(), ResultOk, msg, callback));
        } else {
            // Regular path, append individual message to incoming messages queue
            incomingMessages_.push(msg);
            lock.unlock();
        }
    }

    if (skippedMessages > 0) {
        increaseAvailablePermits(cnx, skippedMessages);
    }

    return batchSize - skippedMessages;
}

bool ConsumerImpl::decryptMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                          const proto::MessageMetadata& metadata, SharedBuffer& payload) {
    if (!metadata.encryption_keys_size()) {
        return true;
    }

    // If KeyReader is not configured throw exception based on config param
    if (!config_.isEncryptionEnabled()) {
        if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::CONSUME) {
            LOG_WARN(getName() << "CryptoKeyReader is not implemented. Consuming encrypted message.");
            return true;
        } else if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::DISCARD) {
            LOG_WARN(getName() << "Skipping decryption since CryptoKeyReader is not implemented and config "
                                  "is set to discard");
            discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::DecryptionError);
        } else {
            LOG_ERROR(getName() << "Message delivery failed since CryptoKeyReader is not implemented to "
                                   "consume encrypted message");
        }
        return false;
    }

    SharedBuffer decryptedPayload;
    if (msgCrypto_->decrypt(metadata, payload, config_.getCryptoKeyReader(), decryptedPayload)) {
        payload = decryptedPayload;
        return true;
    }

    if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::CONSUME) {
        // Note, batch message will fail to consume even if config is set to consume
        LOG_WARN(
            getName() << "Decryption failed. Consuming encrypted message since config is set to consume.");
        return true;
    } else if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::DISCARD) {
        LOG_WARN(getName() << "Discarding message since decryption failed and config is set to discard");
        discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::DecryptionError);
    } else {
        LOG_ERROR(getName() << "Message delivery failed since unable to decrypt incoming message");
    }
    return false;
}

bool ConsumerImpl::uncompressMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                             const proto::MessageMetadata& metadata, SharedBuffer& payload) {
    if (!metadata.has_compression()) {
        return true;
    }

    CompressionType compressionType = CompressionCodecProvider::convertType(metadata.compression());

    uint32_t uncompressedSize = metadata.uncompressed_size();
    uint32_t payloadSize = payload.readableBytes();
    if (cnx) {
        if (payloadSize > ClientConnection::getMaxMessageSize()) {
            // Uncompressed size is itself corrupted since it cannot be bigger than the MaxMessageSize
            LOG_ERROR(getName() << "Got corrupted payload message size " << payloadSize  //
                                << " at  " << msg.message_id().ledgerid() << ":"
                                << msg.message_id().entryid());
            discardCorruptedMessage(cnx, msg.message_id(), proto::CommandAck::UncompressedSizeCorruption);
            return false;
        }
    } else {
        LOG_ERROR("Connection not ready for Consumer - " << getConsumerId());
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
    LOG_ERROR(getName() << "Discarding corrupted message at " << messageId.ledgerid() << ":"
                        << messageId.entryid());

    SharedBuffer cmd =
        Commands::newAck(consumerId_, messageId, proto::CommandAck::Individual, validationError);

    cnx->sendCommand(cmd);
    increaseAvailablePermits(cnx);
}

void ConsumerImpl::internalListener() {
    if (!messageListenerRunning_) {
        return;
    }
    Message msg;
    if (!incomingMessages_.pop(msg, std::chrono::milliseconds(0))) {
        // This will only happen when the connection got reset and we cleared the queue
        return;
    }
    trackMessage(msg);
    try {
        consumerStatsBasePtr_->receivedMessage(msg, ResultOk);
        lastDequedMessage_ = Optional<MessageId>::of(msg.getMessageId());
        messageListener_(Consumer(shared_from_this()), msg);
    } catch (const std::exception& e) {
        LOG_ERROR(getName() << "Exception thrown from listener" << e.what());
    }
    messageProcessed(msg, false);
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

    sendFlowPermitsToBroker(currentCnx, 1);

    while (true) {
        incomingMessages_.pop(msg);
        {
            // Lock needed to prevent race between connectionOpened and the check "msg.impl_->cnx_ ==
            // currentCnx.get())"
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
    Result res = receiveHelper(msg);
    consumerStatsBasePtr_->receivedMessage(msg, res);
    return res;
}

void ConsumerImpl::receiveAsync(ReceiveCallback& callback) {
    Message msg;

    // fail the callback if consumer is closing or closed
    Lock stateLock(mutex_);
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, msg);
        return;
    }
    stateLock.unlock();

    Lock lock(pendingReceiveMutex_);
    if (incomingMessages_.pop(msg, std::chrono::milliseconds(0))) {
        lock.unlock();
        messageProcessed(msg);
        callback(ResultOk, msg);
    } else {
        pendingReceives_.push(callback);
        lock.unlock();

        if (config_.getReceiverQueueSize() == 0) {
            sendFlowPermitsToBroker(getCnx().lock(), 1);
        }
    }
}

Result ConsumerImpl::receiveHelper(Message& msg) {
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
    return ResultOk;
}

Result ConsumerImpl::receive(Message& msg, int timeout) {
    Result res = receiveHelper(msg, timeout);
    consumerStatsBasePtr_->receivedMessage(msg, res);
    return res;
}

Result ConsumerImpl::receiveHelper(Message& msg, int timeout) {
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

    if (incomingMessages_.pop(msg, std::chrono::milliseconds(timeout))) {
        messageProcessed(msg);
        return ResultOk;
    } else {
        return ResultTimeout;
    }
}

void ConsumerImpl::messageProcessed(Message& msg, bool track) {
    Lock lock(mutex_);
    lastDequedMessage_ = Optional<MessageId>::of(msg.getMessageId());

    ClientConnectionPtr currentCnx = getCnx().lock();
    if (currentCnx && msg.impl_->cnx_ != currentCnx.get()) {
        LOG_DEBUG(getName() << "Not adding permit since connection is different.");
        return;
    }

    increaseAvailablePermits(currentCnx);
    if (track) {
        trackMessage(msg);
    }
}

/**
 * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that
 * was
 * not seen by the application
 */
Optional<MessageId> ConsumerImpl::clearReceiveQueue() {
    Message nextMessageInQueue;
    if (incomingMessages_.peekAndClear(nextMessageInQueue)) {
        // There was at least one message pending in the queue
        const MessageId& nextMessageId = nextMessageInQueue.getMessageId();
        MessageId previousMessageId;
        if (nextMessageId.batchIndex() >= 0) {
            previousMessageId = MessageId(-1, nextMessageId.ledgerId(), nextMessageId.entryId(),
                                          nextMessageId.batchIndex() - 1);
        } else {
            previousMessageId = MessageId(-1, nextMessageId.ledgerId(), nextMessageId.entryId() - 1, -1);
        }
        return Optional<MessageId>::of(previousMessageId);
    } else if (lastDequedMessage_.is_present()) {
        // If the queue was empty we need to restart from the message just after the last one that has been
        // dequeued
        // in the past
        return lastDequedMessage_;
    } else {
        // No message was received or dequeued by this consumer. Next message would still be the
        // startMessageId
        return startMessageId_;
    }
}

void ConsumerImpl::increaseAvailablePermits(const ClientConnectionPtr& currentCnx, int delta) {
    int newAvailablePermits = availablePermits_.fetch_add(delta) + delta;

    while (newAvailablePermits >= receiverQueueRefillThreshold_ && messageListenerRunning_) {
        if (availablePermits_.compare_exchange_weak(newAvailablePermits, 0)) {
            sendFlowPermitsToBroker(currentCnx, newAvailablePermits);
            break;
        }
    }
}

inline proto::CommandSubscribe_SubType ConsumerImpl::getSubType() {
    ConsumerType type = config_.getConsumerType();
    switch (type) {
        case ConsumerExclusive:
            return proto::CommandSubscribe::Exclusive;

        case ConsumerShared:
            return proto::CommandSubscribe::Shared;

        case ConsumerFailover:
            return proto::CommandSubscribe::Failover;

        case ConsumerKeyShared:
            return proto::CommandSubscribe_SubType_Key_Shared;
    }
    BOOST_THROW_EXCEPTION(std::logic_error("Invalid ConsumerType enumeration value"));
}

inline proto::CommandSubscribe_InitialPosition ConsumerImpl::getInitialPosition() {
    InitialPosition initialPosition = config_.getSubscriptionInitialPosition();
    switch (initialPosition) {
        case InitialPositionLatest:
            return proto::CommandSubscribe_InitialPosition::CommandSubscribe_InitialPosition_Latest;

        case InitialPositionEarliest:
            return proto::CommandSubscribe_InitialPosition::CommandSubscribe_InitialPosition_Earliest;
    }
    BOOST_THROW_EXCEPTION(std::logic_error("Invalid InitialPosition enumeration value"));
}

void ConsumerImpl::statsCallback(Result res, ResultCallback callback, proto::CommandAck_AckType ackType) {
    consumerStatsBasePtr_->messageAcknowledged(res, ackType);
    if (callback) {
        callback(res);
    }
}

void ConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    ResultCallback cb = std::bind(&ConsumerImpl::statsCallback, shared_from_this(), std::placeholders::_1,
                                  callback, proto::CommandAck_AckType_Individual);
    if (msgId.batchIndex() != -1 &&
        !batchAcknowledgementTracker_.isBatchReady(msgId, proto::CommandAck_AckType_Individual)) {
        cb(ResultOk);
        return;
    }
    doAcknowledgeIndividual(msgId, cb);
}

void ConsumerImpl::acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) {
    ResultCallback cb = std::bind(&ConsumerImpl::statsCallback, shared_from_this(), std::placeholders::_1,
                                  callback, proto::CommandAck_AckType_Cumulative);
    if (!isCumulativeAcknowledgementAllowed(config_.getConsumerType())) {
        cb(ResultCumulativeAcknowledgementNotAllowedError);
        return;
    }
    if (msgId.batchIndex() != -1 &&
        !batchAcknowledgementTracker_.isBatchReady(msgId, proto::CommandAck_AckType_Cumulative)) {
        MessageId messageId = batchAcknowledgementTracker_.getGreatestCumulativeAckReady(msgId);
        if (messageId == MessageId()) {
            // Nothing to ACK, because the batch that msgId belongs to is NOT completely consumed.
            cb(ResultOk);
        } else {
            doAcknowledgeCumulative(messageId, cb);
        }
    } else {
        doAcknowledgeCumulative(msgId, cb);
    }
}

bool ConsumerImpl::isCumulativeAcknowledgementAllowed(ConsumerType consumerType) {
    return consumerType != ConsumerKeyShared && consumerType != ConsumerShared;
}

void ConsumerImpl::doAcknowledgeIndividual(const MessageId& messageId, ResultCallback callback) {
    this->unAckedMessageTrackerPtr_->remove(messageId);
    this->batchAcknowledgementTracker_.deleteAckedMessage(messageId, proto::CommandAck::Individual);
    this->ackGroupingTrackerPtr_->addAcknowledge(messageId);
    callback(ResultOk);
}

void ConsumerImpl::doAcknowledgeCumulative(const MessageId& messageId, ResultCallback callback) {
    this->unAckedMessageTrackerPtr_->removeMessagesTill(messageId);
    this->batchAcknowledgementTracker_.deleteAckedMessage(messageId, proto::CommandAck::Cumulative);
    this->ackGroupingTrackerPtr_->addAcknowledgeCumulative(messageId);
    callback(ResultOk);
}

void ConsumerImpl::negativeAcknowledge(const MessageId& messageId) {
    unAckedMessageTrackerPtr_->remove(messageId);
    negativeAcksTracker_.add(messageId);
}

void ConsumerImpl::disconnectConsumer() {
    LOG_INFO("Broker notification of Closed consumer: " << consumerId_);
    Lock lock(mutex_);
    connection_.reset();
    lock.unlock();
    scheduleReconnection(shared_from_this());
}

void ConsumerImpl::closeAsync(ResultCallback callback) {
    Lock lock(mutex_);

    // Keep a reference to ensure object is kept alive
    ConsumerImplPtr ptr = shared_from_this();

    if (state_ != Ready) {
        lock.unlock();
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    LOG_INFO(getName() << "Closing consumer for topic " << topic_);
    state_ = Closing;

    // Flush pending grouped ACK requests.
    if (ackGroupingTrackerPtr_) {
        ackGroupingTrackerPtr_->close();
    }

    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        state_ = Closed;
        lock.unlock();
        // If connection is gone, also the consumer is closed on the broker side
        if (callback) {
            callback(ResultOk);
        }
        return;
    }

    ClientImplPtr client = client_.lock();
    if (!client) {
        state_ = Closed;
        lock.unlock();
        // Client was already destroyed
        if (callback) {
            callback(ResultOk);
        }
        return;
    }

    // Lock is no longer required
    lock.unlock();
    int requestId = client->newRequestId();
    Future<Result, ResponseData> future =
        cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId);
    if (callback) {
        // Pass the shared pointer "ptr" to the handler to prevent the object from being destroyed
        future.addListener(
            std::bind(&ConsumerImpl::handleClose, shared_from_this(), std::placeholders::_1, callback, ptr));
    }

    // fail pendingReceive callback
    failPendingReceiveCallback();
}

void ConsumerImpl::handleClose(Result result, ResultCallback callback, ConsumerImplPtr consumer) {
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

    if (callback) {
        callback(result);
    }
}

const std::string& ConsumerImpl::getName() const { return consumerStr_; }

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
    messageListenerRunning_ = false;
    return ResultOk;
}

Result ConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }

    if (messageListenerRunning_) {
        // Not paused
        return ResultOk;
    }
    messageListenerRunning_ = true;
    const size_t count = incomingMessages_.size();

    for (size_t i = 0; i < count; i++) {
        // Trigger message listener callback in a separate thread
        listenerExecutor_->postWork(std::bind(&ConsumerImpl::internalListener, shared_from_this()));
    }
    // Check current permits and determine whether to send FLOW command
    this->increaseAvailablePermits(getCnx().lock(), 0);
    return ResultOk;
}

void ConsumerImpl::redeliverUnacknowledgedMessages() {
    static std::set<MessageId> emptySet;
    redeliverMessages(emptySet);
    unAckedMessageTrackerPtr_->clear();
}

void ConsumerImpl::redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) {
    if (messageIds.empty()) {
        return;
    }
    if (config_.getConsumerType() != ConsumerShared && config_.getConsumerType() != ConsumerKeyShared) {
        redeliverUnacknowledgedMessages();
        return;
    }
    redeliverMessages(messageIds);
}

void ConsumerImpl::redeliverMessages(const std::set<MessageId>& messageIds) {
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v2) {
            cnx->sendCommand(Commands::newRedeliverUnacknowledgedMessages(consumerId_, messageIds));
            LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for Consumer - " << getConsumerId());
        }
    } else {
        LOG_DEBUG("Connection not ready for Consumer - " << getConsumerId());
    }
}

int ConsumerImpl::getNumOfPrefetchedMessages() const { return incomingMessages_.size(); }

void ConsumerImpl::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        LOG_ERROR(getName() << "Client connection is not open, please try again later.")
        lock.unlock();
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }

    if (brokerConsumerStats_.isValid()) {
        LOG_DEBUG(getName() << "Serving data from cache");
        BrokerConsumerStatsImpl brokerConsumerStats = brokerConsumerStats_;
        lock.unlock();
        callback(ResultOk,
                 BrokerConsumerStats(std::make_shared<BrokerConsumerStatsImpl>(brokerConsumerStats_)));
        return;
    }
    lock.unlock();

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v8) {
            ClientImplPtr client = client_.lock();
            uint64_t requestId = client->newRequestId();
            LOG_DEBUG(getName() << " Sending ConsumerStats Command for Consumer - " << getConsumerId()
                                << ", requestId - " << requestId);

            cnx->newConsumerStats(consumerId_, requestId)
                .addListener(std::bind(&ConsumerImpl::brokerConsumerStatsListener, shared_from_this(),
                                       std::placeholders::_1, std::placeholders::_2, callback));
            return;
        } else {
            LOG_ERROR(getName() << " Operation not supported since server protobuf version "
                                << cnx->getServerProtocolVersion() << " is older than proto::v7");
            callback(ResultUnsupportedVersionError, BrokerConsumerStats());
            return;
        }
    }
    LOG_ERROR(getName() << " Client Connection not ready for Consumer");
    callback(ResultNotConnected, BrokerConsumerStats());
}

void ConsumerImpl::brokerConsumerStatsListener(Result res, BrokerConsumerStatsImpl brokerConsumerStats,
                                               BrokerConsumerStatsCallback callback) {
    if (res == ResultOk) {
        Lock lock(mutex_);
        brokerConsumerStats.setCacheTime(config_.getBrokerConsumerStatsCacheTimeInMs());
        brokerConsumerStats_ = brokerConsumerStats;
    }

    if (callback) {
        callback(res, BrokerConsumerStats(std::make_shared<BrokerConsumerStatsImpl>(brokerConsumerStats)));
    }
}

void ConsumerImpl::handleSeek(Result result, ResultCallback callback) {
    if (result == ResultOk) {
        LOG_INFO(getName() << "Seek successfully");
    } else {
        LOG_ERROR(getName() << "Failed to seek: " << strResult(result));
    }
    callback(result);
}

void ConsumerImpl::seekAsync(const MessageId& msgId, ResultCallback callback) {
    Lock lock(mutex_);
    if (state_ == Closed || state_ == Closing) {
        lock.unlock();
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }
    lock.unlock();

    this->ackGroupingTrackerPtr_->flushAndClean();
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        ClientImplPtr client = client_.lock();
        uint64_t requestId = client->newRequestId();
        LOG_DEBUG(getName() << " Sending seek Command for Consumer - " << getConsumerId() << ", requestId - "
                            << requestId);
        Future<Result, ResponseData> future =
            cnx->sendRequestWithId(Commands::newSeek(consumerId_, requestId, msgId), requestId);

        if (callback) {
            future.addListener(
                std::bind(&ConsumerImpl::handleSeek, shared_from_this(), std::placeholders::_1, callback));
        }
        return;
    }

    LOG_ERROR(getName() << " Client Connection not ready for Consumer");
    callback(ResultNotConnected);
}

void ConsumerImpl::seekAsync(uint64_t timestamp, ResultCallback callback) {
    Lock lock(mutex_);
    if (state_ == Closed || state_ == Closing) {
        lock.unlock();
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }
    lock.unlock();

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        ClientImplPtr client = client_.lock();
        uint64_t requestId = client->newRequestId();
        LOG_DEBUG(getName() << " Sending seek Command for Consumer - " << getConsumerId() << ", requestId - "
                            << requestId);
        Future<Result, ResponseData> future =
            cnx->sendRequestWithId(Commands::newSeek(consumerId_, requestId, timestamp), requestId);

        if (callback) {
            future.addListener(
                std::bind(&ConsumerImpl::handleSeek, shared_from_this(), std::placeholders::_1, callback));
        }
        return;
    }

    LOG_ERROR(getName() << " Client Connection not ready for Consumer");
    callback(ResultNotConnected);
}

bool ConsumerImpl::isReadCompacted() { return readCompacted_; }

void ConsumerImpl::hasMessageAvailableAsync(HasMessageAvailableCallback callback) {
    MessageId lastDequed = this->lastMessageIdDequed();
    MessageId lastInBroker = this->lastMessageIdInBroker();
    if (lastInBroker > lastDequed && lastInBroker.entryId() != -1) {
        callback(ResultOk, true);
        return;
    }

    getLastMessageIdAsync([lastDequed, callback](Result result, MessageId messageId) {
        if (result == ResultOk) {
            if (messageId > lastDequed && messageId.entryId() != -1) {
                callback(ResultOk, true);
            } else {
                callback(ResultOk, false);
            }
        } else {
            callback(result, false);
        }
    });
}

void ConsumerImpl::brokerGetLastMessageIdListener(Result res, MessageId messageId,
                                                  BrokerGetLastMessageIdCallback callback) {
    Lock lock(mutex_);
    if (messageId > lastMessageIdInBroker()) {
        lastMessageInBroker_ = Optional<MessageId>::of(messageId);
        lock.unlock();
        callback(res, messageId);
    } else {
        lock.unlock();
        callback(res, lastMessageIdInBroker());
    }
}

void ConsumerImpl::getLastMessageIdAsync(BrokerGetLastMessageIdCallback callback) {
    Lock lock(mutex_);
    if (state_ == Closed || state_ == Closing) {
        lock.unlock();
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed, MessageId());
        }
        return;
    }
    lock.unlock();

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v12) {
            ClientImplPtr client = client_.lock();
            uint64_t requestId = client->newRequestId();
            LOG_DEBUG(getName() << " Sending getLastMessageId Command for Consumer - " << getConsumerId()
                                << ", requestId - " << requestId);

            cnx->newGetLastMessageId(consumerId_, requestId)
                .addListener(std::bind(&ConsumerImpl::brokerGetLastMessageIdListener, shared_from_this(),
                                       std::placeholders::_1, std::placeholders::_2, callback));
        } else {
            LOG_ERROR(getName() << " Operation not supported since server protobuf version "
                                << cnx->getServerProtocolVersion() << " is older than proto::v12");
            callback(ResultUnsupportedVersionError, MessageId());
        }
    } else {
        LOG_ERROR(getName() << " Client Connection not ready for Consumer");
        callback(ResultNotConnected, MessageId());
    }
}

void ConsumerImpl::setNegativeAcknowledgeEnabledForTesting(bool enabled) {
    negativeAcksTracker_.setEnabledForTesting(enabled);
}

void ConsumerImpl::trackMessage(const Message& msg) {
    if (hasParent_) {
        unAckedMessageTrackerPtr_->remove(msg.getMessageId());
    } else {
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
    }
}

bool ConsumerImpl::isConnected() const {
    Lock lock(mutex_);
    return !getCnx().expired() && state_ == Ready;
}

uint64_t ConsumerImpl::getNumberOfConnectedConsumer() { return isConnected() ? 1 : 0; }

} /* namespace pulsar */
