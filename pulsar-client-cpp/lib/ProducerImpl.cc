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
#include "ProducerImpl.h"
#include "LogUtils.h"
#include "MessageImpl.h"
#include "TimeUtils.h"
#include "PulsarApi.pb.h"
#include "Commands.h"
#include "BatchMessageContainerBase.h"
#include "BatchMessageContainer.h"
#include "BatchMessageKeyBasedContainer.h"
#include <boost/date_time/local_time/local_time.hpp>
#include <lib/TopicName.h>
#include "MessageAndCallbackBatch.h"

namespace pulsar {
DECLARE_LOG_OBJECT()

struct ProducerImpl::PendingCallbacks {
    std::vector<OpSendMsg> opSendMsgs;

    void complete(Result result) {
        for (const auto& opSendMsg : opSendMsgs) {
            opSendMsg.sendCallback_(result, opSendMsg.msg_.getMessageId());
        }
    }
};

ProducerImpl::ProducerImpl(ClientImplPtr client, const std::string& topic, const ProducerConfiguration& conf,
                           int32_t partition)
    : HandlerBase(
          client, topic,
          Backoff(milliseconds(100), seconds(60), milliseconds(std::max(100, conf.getSendTimeout() - 100)))),
      conf_(conf),
      semaphore_(),
      pendingMessagesQueue_(),
      partition_(partition),
      producerName_(conf_.getProducerName()),
      userProvidedProducerName_(false),
      producerStr_("[" + topic_ + ", " + producerName_ + "] "),
      producerId_(client->newProducerId()),
      msgSequenceGenerator_(0),
      dataKeyGenIntervalSec_(4 * 60 * 60),
      memoryLimitController_(client->getMemoryLimitController()) {
    LOG_DEBUG("ProducerName - " << producerName_ << " Created producer on topic " << topic_
                                << " id: " << producerId_);

    int64_t initialSequenceId = conf.getInitialSequenceId();
    lastSequenceIdPublished_ = initialSequenceId;
    msgSequenceGenerator_ = initialSequenceId + 1;

    if (!producerName_.empty()) {
        userProvidedProducerName_ = true;
    }

    if (conf.getMaxPendingMessages() > 0) {
        semaphore_ = std::unique_ptr<Semaphore>(new Semaphore(conf_.getMaxPendingMessages()));
    }

    unsigned int statsIntervalInSeconds = client->getClientConfig().getStatsIntervalInSeconds();
    if (statsIntervalInSeconds) {
        producerStatsBasePtr_ =
            std::make_shared<ProducerStatsImpl>(producerStr_, executor_, statsIntervalInSeconds);
    } else {
        producerStatsBasePtr_ = std::make_shared<ProducerStatsDisabled>();
    }

    if (conf_.isEncryptionEnabled()) {
        std::ostringstream logCtxStream;
        logCtxStream << "[" << topic_ << ", " << producerName_ << ", " << producerId_ << "]";
        std::string logCtx = logCtxStream.str();
        msgCrypto_ = std::make_shared<MessageCrypto>(logCtx, true);
        msgCrypto_->addPublicKeyCipher(conf_.getEncryptionKeys(), conf_.getCryptoKeyReader());
    }

    if (conf_.getBatchingEnabled()) {
        switch (conf_.getBatchingType()) {
            case ProducerConfiguration::DefaultBatching:
                batchMessageContainer_.reset(new BatchMessageContainer(*this));
                break;
            case ProducerConfiguration::KeyBasedBatching:
                batchMessageContainer_.reset(new BatchMessageKeyBasedContainer(*this));
                break;
            default:  // never reached here
                LOG_ERROR("Unknown batching type: " << conf_.getBatchingType());
                return;
        }
        batchTimer_ = executor_->createDeadlineTimer();
    }
}

ProducerImpl::~ProducerImpl() {
    LOG_DEBUG(getName() << "~ProducerImpl");
    cancelTimers();
    printStats();
    if (state_ == Ready || state_ == Pending) {
        LOG_WARN(getName() << "Destroyed producer which was not properly closed");
    }
}

const std::string& ProducerImpl::getTopic() const { return topic_; }

const std::string& ProducerImpl::getProducerName() const { return producerName_; }

int64_t ProducerImpl::getLastSequenceId() const { return lastSequenceIdPublished_; }

const std::string& ProducerImpl::getSchemaVersion() const { return schemaVersion_; }

void ProducerImpl::refreshEncryptionKey(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    msgCrypto_->addPublicKeyCipher(conf_.getEncryptionKeys(), conf_.getCryptoKeyReader());

    dataKeyGenTImer_->expires_from_now(boost::posix_time::seconds(dataKeyGenIntervalSec_));
    dataKeyGenTImer_->async_wait(
        std::bind(&pulsar::ProducerImpl::refreshEncryptionKey, shared_from_this(), std::placeholders::_1));
}

void ProducerImpl::connectionOpened(const ClientConnectionPtr& cnx) {
    Lock lock(mutex_);
    if (state_ == Closed) {
        lock.unlock();
        LOG_DEBUG(getName() << "connectionOpened : Producer is already closed");
        return;
    }
    lock.unlock();

    ClientImplPtr client = client_.lock();
    int requestId = client->newRequestId();

    SharedBuffer cmd = Commands::newProducer(topic_, producerId_, producerName_, requestId,
                                             conf_.getProperties(), conf_.getSchema(), epoch_,
                                             userProvidedProducerName_, conf_.isEncryptionEnabled());
    cnx->sendRequestWithId(cmd, requestId)
        .addListener(std::bind(&ProducerImpl::handleCreateProducer, shared_from_this(), cnx,
                               std::placeholders::_1, std::placeholders::_2));
}

void ProducerImpl::connectionFailed(Result result) {
    // Keep a reference to ensure object is kept alive
    ProducerImplPtr ptr = shared_from_this();

    if (conf_.getLazyStartPartitionedProducers()) {
        // if producers are lazy, then they should always try to restart
        // so don't change the state and allow reconnections
        return;
    } else if (producerCreatedPromise_.setFailed(result)) {
        Lock lock(mutex_);
        state_ = Failed;
    }
}

void ProducerImpl::handleCreateProducer(const ClientConnectionPtr& cnx, Result result,
                                        const ResponseData& responseData) {
    LOG_DEBUG(getName() << "ProducerImpl::handleCreateProducer res: " << strResult(result));

    // make sure we're still in the Pending/Ready state, closeAsync could have been invoked
    // while waiting for this response if using lazy producers
    Lock lock(mutex_);
    if (state_ != Ready && state_ != Pending) {
        LOG_DEBUG("Producer created response received but producer already closed");
        failPendingMessages(ResultAlreadyClosed, false);
        return;
    }

    if (result == ResultOk) {
        // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
        // set the cnx pointer so that new messages will be sent immediately
        LOG_INFO(getName() << "Created producer on broker " << cnx->cnxString());

        cnx->registerProducer(producerId_, shared_from_this());
        producerName_ = responseData.producerName;
        schemaVersion_ = responseData.schemaVersion;
        producerStr_ = "[" + topic_ + ", " + producerName_ + "] ";

        if (lastSequenceIdPublished_ == -1 && conf_.getInitialSequenceId() == -1) {
            lastSequenceIdPublished_ = responseData.lastSequenceId;
            msgSequenceGenerator_ = lastSequenceIdPublished_ + 1;
        }
        resendMessages(cnx);
        connection_ = cnx;
        state_ = Ready;
        backoff_.reset();
        lock.unlock();

        if (!dataKeyGenTImer_ && conf_.isEncryptionEnabled()) {
            dataKeyGenTImer_ = executor_->createDeadlineTimer();
            dataKeyGenTImer_->expires_from_now(boost::posix_time::seconds(dataKeyGenIntervalSec_));
            dataKeyGenTImer_->async_wait(std::bind(&pulsar::ProducerImpl::refreshEncryptionKey,
                                                   shared_from_this(), std::placeholders::_1));
        }

        // if the producer is lazy the send timeout timer is already running
        if (!conf_.getLazyStartPartitionedProducers()) {
            startSendTimeoutTimer();
        }

        producerCreatedPromise_.setValue(shared_from_this());

    } else {
        lock.unlock();

        // Producer creation failed
        if (result == ResultTimeout) {
            // Creating the producer has timed out. We need to ensure the broker closes the producer
            // in case it was indeed created, otherwise it might prevent new create producer operation,
            // since we are not closing the connection
            int requestId = client_.lock()->newRequestId();
            cnx->sendRequestWithId(Commands::newCloseProducer(producerId_, requestId), requestId);
        }

        if (producerCreatedPromise_.isComplete()) {
            if (result == ResultProducerBlockedQuotaExceededException) {
                LOG_WARN(getName() << "Backlog is exceeded on topic. Sending exception to producer");
                failPendingMessages(ResultProducerBlockedQuotaExceededException, true);
            } else if (result == ResultProducerBlockedQuotaExceededError) {
                LOG_WARN(getName() << "Producer is blocked on creation because backlog is exceeded on topic");
            }

            // Producer had already been initially created, we need to retry connecting in any case
            LOG_WARN(getName() << "Failed to reconnect producer: " << strResult(result));
            scheduleReconnection(shared_from_this());
        } else {
            // Producer was not yet created, retry to connect to broker if it's possible
            if (isRetriableError(result) && (creationTimestamp_ + operationTimeut_ < TimeUtils::now())) {
                LOG_WARN(getName() << "Temporary error in creating producer: " << strResult(result));
                scheduleReconnection(shared_from_this());
            } else {
                LOG_ERROR(getName() << "Failed to create producer: " << strResult(result));
                failPendingMessages(result, true);
                producerCreatedPromise_.setFailed(result);
                Lock lock(mutex_);
                state_ = Failed;
            }
        }
    }
}

std::shared_ptr<ProducerImpl::PendingCallbacks> ProducerImpl::getPendingCallbacksWhenFailed() {
    auto callbacks = std::make_shared<PendingCallbacks>();
    callbacks->opSendMsgs.reserve(pendingMessagesQueue_.size());
    LOG_DEBUG(getName() << "# messages in pending queue : " << pendingMessagesQueue_.size());

    // Iterate over a copy of the pending messages queue, to trigger the future completion
    // without holding producer mutex.
    for (auto& op : pendingMessagesQueue_) {
        callbacks->opSendMsgs.push_back(op);
        releaseSemaphoreForSendOp(op);
    }

    if (batchMessageContainer_) {
        OpSendMsg opSendMsg;
        if (batchMessageContainer_->createOpSendMsg(opSendMsg) == ResultOk) {
            callbacks->opSendMsgs.emplace_back(opSendMsg);
        }

        releaseSemaphoreForSendOp(opSendMsg);
        batchMessageContainer_->clear();
    }
    pendingMessagesQueue_.clear();

    return callbacks;
}

std::shared_ptr<ProducerImpl::PendingCallbacks> ProducerImpl::getPendingCallbacksWhenFailedWithLock() {
    Lock lock(mutex_);
    return getPendingCallbacksWhenFailed();
}

void ProducerImpl::failPendingMessages(Result result, bool withLock) {
    if (withLock) {
        getPendingCallbacksWhenFailedWithLock()->complete(result);
    } else {
        getPendingCallbacksWhenFailed()->complete(result);
    }
}

void ProducerImpl::resendMessages(ClientConnectionPtr cnx) {
    if (pendingMessagesQueue_.empty()) {
        return;
    }

    LOG_DEBUG(getName() << "Re-Sending " << pendingMessagesQueue_.size() << " messages to server");

    for (const auto& op : pendingMessagesQueue_) {
        LOG_DEBUG(getName() << "Re-Sending " << op.sequenceId_);
        cnx->sendMessage(op);
    }
}

void ProducerImpl::setMessageMetadata(const Message& msg, const uint64_t& sequenceId,
                                      const uint32_t& uncompressedSize) {
    // Call this function after acquiring the mutex_
    proto::MessageMetadata& msgMetadata = msg.impl_->metadata;
    msgMetadata.set_producer_name(producerName_);
    msgMetadata.set_publish_time(TimeUtils::currentTimeMillis());
    msgMetadata.set_sequence_id(sequenceId);
    if (conf_.getCompressionType() != CompressionNone) {
        msgMetadata.set_compression(CompressionCodecProvider::convertType(conf_.getCompressionType()));
        msgMetadata.set_uncompressed_size(uncompressedSize);
    }
    if (!this->getSchemaVersion().empty()) {
        msgMetadata.set_schema_version(this->getSchemaVersion());
    }
}

void ProducerImpl::statsCallBackHandler(Result res, const MessageId& msgId, SendCallback callback,
                                        boost::posix_time::ptime publishTime) {
    producerStatsBasePtr_->messageReceived(res, publishTime);
    if (callback) {
        callback(res, msgId);
    }
}

void ProducerImpl::flushAsync(FlushCallback callback) {
    if (batchMessageContainer_) {
        Lock lock(mutex_);

        if (state_ == Ready) {
            auto failures = batchMessageAndSend(callback);
            lock.unlock();
            failures.complete();
        } else {
            callback(ResultAlreadyClosed);
        }
    } else {
        callback(ResultOk);
    }
}

void ProducerImpl::triggerFlush() {
    if (batchMessageContainer_) {
        Lock lock(mutex_);
        if (state_ == Ready) {
            auto failures = batchMessageAndSend();
            lock.unlock();
            failures.complete();
        }
    }
}

void ProducerImpl::sendAsync(const Message& msg, SendCallback callback) {
    producerStatsBasePtr_->messageSent(msg);
    SendCallback cb =
        std::bind(&ProducerImpl::statsCallBackHandler, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, callback, boost::posix_time::microsec_clock::universal_time());

    // Compress the payload if required
    SharedBuffer& payload = msg.impl_->payload;

    uint32_t uncompressedSize = payload.readableBytes();
    uint32_t payloadSize = uncompressedSize;
    ClientConnectionPtr cnx = getCnx().lock();
    if (!batchMessageContainer_) {
        // If batching is enabled we compress all the payloads together before sending the batch
        payload = CompressionCodecProvider::getCodec(conf_.getCompressionType()).encode(payload);
        payloadSize = payload.readableBytes();

        // Encrypt the payload if enabled
        SharedBuffer encryptedPayload;
        if (!encryptMessage(msg.impl_->metadata, payload, encryptedPayload)) {
            cb(ResultCryptoError, msg.getMessageId());
            return;
        }
        payload = encryptedPayload;

        if (payloadSize > ClientConnection::getMaxMessageSize()) {
            LOG_DEBUG(getName() << " - compressed Message payload size" << payloadSize << "cannot exceed "
                                << ClientConnection::getMaxMessageSize() << " bytes");
            cb(ResultMessageTooBig, msg.getMessageId());
            return;
        }
    }

    // Reserve a spot in the messages queue before acquiring the ProducerImpl
    // mutex. When the queue is full, this call will block until a spot is
    // available.
    Result res = canEnqueueRequest(payloadSize);
    if (res != ResultOk) {
        // If queue is full sending the batch immediately, no point waiting till batchMessagetimeout
        if (batchMessageContainer_) {
            LOG_DEBUG(getName() << " - sending batch message immediately");
            Lock lock(mutex_);
            auto failures = batchMessageAndSend();
            lock.unlock();
            failures.complete();
        }

        cb(res, msg.getMessageId());
        return;
    }

    Lock lock(mutex_);
    // producers may be lazily starting and be in the pending state
    if (state_ != Ready && state_ != Pending) {
        lock.unlock();
        releaseSemaphore(payloadSize);
        cb(ResultAlreadyClosed, msg.getMessageId());
        return;
    }

    if (msg.impl_->metadata.has_producer_name()) {
        // Message had already been sent before
        lock.unlock();
        releaseSemaphore(payloadSize);
        cb(ResultInvalidMessage, msg.getMessageId());
        return;
    }

    uint64_t sequenceId;
    if (!msg.impl_->metadata.has_sequence_id()) {
        sequenceId = msgSequenceGenerator_++;
    } else {
        sequenceId = msg.impl_->metadata.sequence_id();
    }
    setMessageMetadata(msg, sequenceId, uncompressedSize);

    // If we reach this point then you have a reserved spot on the queue
    if (batchMessageContainer_ && !msg.impl_->metadata.has_deliver_at_time()) {
        // Batching is enabled and the message is not delayed
        if (!batchMessageContainer_->hasEnoughSpace(msg)) {
            batchMessageAndSend().complete();
        }
        bool isFirstMessage = batchMessageContainer_->isFirstMessageToAdd(msg);
        bool isFull = batchMessageContainer_->add(msg, cb);
        if (isFirstMessage) {
            batchTimer_->expires_from_now(
                boost::posix_time::milliseconds(conf_.getBatchingMaxPublishDelayMs()));
            batchTimer_->async_wait(std::bind(&ProducerImpl::batchMessageTimeoutHandler, shared_from_this(),
                                              std::placeholders::_1));
        }

        if (isFull) {
            auto failures = batchMessageAndSend();
            lock.unlock();
            failures.complete();
        }
    } else {
        sendMessage(OpSendMsg{msg, cb, producerId_, sequenceId, conf_.getSendTimeout(), 1, payloadSize});
    }
}

Result ProducerImpl::canEnqueueRequest(uint32_t payloadSize) {
    if (conf_.getBlockIfQueueFull()) {
        if (semaphore_) {
            semaphore_->acquire();
        }
        memoryLimitController_.reserveMemory(payloadSize);
        return ResultOk;
    } else {
        if (semaphore_) {
            if (!semaphore_->tryAcquire()) {
                return ResultProducerQueueIsFull;
            }
        }

        if (!memoryLimitController_.tryReserveMemory(payloadSize)) {
            if (semaphore_) {
                semaphore_->release(1);
            }

            return ResultMemoryBufferIsFull;
        }

        return ResultOk;
    }
}

void ProducerImpl::releaseSemaphore(uint32_t payloadSize) {
    if (semaphore_) {
        semaphore_->release();
    }

    memoryLimitController_.releaseMemory(payloadSize);
}

void ProducerImpl::releaseSemaphoreForSendOp(const OpSendMsg& op) {
    if (semaphore_) {
        semaphore_->release(op.messagesCount_);
    }

    memoryLimitController_.releaseMemory(op.messagesSize_);
}

// It must be called while `mutex_` is acquired
PendingFailures ProducerImpl::batchMessageAndSend(const FlushCallback& flushCallback) {
    PendingFailures failures;
    LOG_DEBUG("batchMessageAndSend " << *batchMessageContainer_);
    batchTimer_->cancel();

    if (PULSAR_UNLIKELY(batchMessageContainer_->isEmpty())) {
        if (flushCallback) {
            flushCallback(ResultOk);
        }
    } else {
        const size_t numBatches = batchMessageContainer_->getNumBatches();
        if (numBatches == 1) {
            OpSendMsg opSendMsg;
            Result result = batchMessageContainer_->createOpSendMsg(opSendMsg, flushCallback);
            if (result == ResultOk) {
                sendMessage(opSendMsg);
            } else {
                // A spot has been reserved for this batch, but the batch failed to be pushed to the queue, so
                // we need to release the spot manually
                LOG_ERROR("batchMessageAndSend | Failed to createOpSendMsg: " << result);
                releaseSemaphoreForSendOp(opSendMsg);
                failures.add(std::bind(opSendMsg.sendCallback_, result, MessageId{}));
            }
        } else if (numBatches > 1) {
            std::vector<OpSendMsg> opSendMsgs;
            std::vector<Result> results = batchMessageContainer_->createOpSendMsgs(opSendMsgs, flushCallback);
            for (size_t i = 0; i < results.size(); i++) {
                if (results[i] == ResultOk) {
                    sendMessage(opSendMsgs[i]);
                } else {
                    // A spot has been reserved for this batch, but the batch failed to be pushed to the
                    // queue, so we need to release the spot manually
                    LOG_ERROR("batchMessageAndSend | Failed to createOpSendMsgs[" << i
                                                                                  << "]: " << results[i]);
                    releaseSemaphoreForSendOp(opSendMsgs[i]);
                    failures.add(std::bind(opSendMsgs[i].sendCallback_, results[i], MessageId{}));
                }
            }
        }  // else numBatches is 0, do nothing
    }

    batchMessageContainer_->clear();
    return failures;
}

// Precondition -
// a. we have a reserved spot on the queue
// b. call this function after acquiring the ProducerImpl mutex_
void ProducerImpl::sendMessage(const OpSendMsg& op) {
    const auto sequenceId = op.msg_.impl_->metadata.sequence_id();
    LOG_DEBUG("Inserting data to pendingMessagesQueue_");
    pendingMessagesQueue_.push_back(op);

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        // If we do have a connection, the message is sent immediately, otherwise
        // we'll try again once a new connection is established
        LOG_DEBUG(getName() << "Sending msg immediately - seq: " << sequenceId);
        cnx->sendMessage(op);
    } else {
        LOG_DEBUG(getName() << "Connection is not ready - seq: " << sequenceId);
    }
}

void ProducerImpl::batchMessageTimeoutHandler(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG(getName() << " Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }
    LOG_DEBUG(getName() << " - Batch Message Timer expired");

    // ignore if the producer is already closing/closed
    Lock lock(mutex_);
    if (state_ == Pending || state_ == Ready) {
        auto failures = batchMessageAndSend();
        lock.unlock();
        failures.complete();
    }
}

void ProducerImpl::printStats() {
    if (batchMessageContainer_) {
        LOG_INFO("Producer - " << producerStr_ << ", [batchMessageContainer = " << *batchMessageContainer_
                               << "]");
    } else {
        LOG_INFO("Producer - " << producerStr_ << ", [batching  = off]");
    }
}

void ProducerImpl::closeAsync(CloseCallback callback) {
    Lock lock(mutex_);

    // if the producer was never started then there is nothing to clean up
    if (state_ == NotStarted) {
        state_ = Closed;
        callback(ResultOk);
        return;
    }

    // Keep a reference to ensure object is kept alive
    ProducerImplPtr ptr = shared_from_this();

    cancelTimers();

    // ensure any remaining send callbacks are called before calling the close callback
    failPendingMessages(ResultAlreadyClosed, false);

    if (state_ != Ready && state_ != Pending) {
        state_ = Closed;
        lock.unlock();
        if (callback) {
            callback(ResultAlreadyClosed);
        }

        return;
    }
    LOG_INFO(getName() << "Closing producer for topic " << topic_);
    state_ = Closing;

    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        state_ = Closed;
        lock.unlock();
        if (callback) {
            callback(ResultOk);
        }
        return;
    }

    // Detach the producer from the connection to avoid sending any other
    // message from the producer
    connection_.reset();

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

    lock.unlock();
    int requestId = client->newRequestId();
    Future<Result, ResponseData> future =
        cnx->sendRequestWithId(Commands::newCloseProducer(producerId_, requestId), requestId);
    if (callback) {
        // Pass the shared pointer "ptr" to the handler to prevent the object from being destroyed
        future.addListener(
            std::bind(&ProducerImpl::handleClose, shared_from_this(), std::placeholders::_1, callback, ptr));
    }
}

void ProducerImpl::handleClose(Result result, ResultCallback callback, ProducerImplPtr producer) {
    if (result == ResultOk) {
        Lock lock(mutex_);
        state_ = Closed;
        LOG_INFO(getName() << "Closed producer");
        ClientConnectionPtr cnx = getCnx().lock();
        if (cnx) {
            cnx->removeProducer(producerId_);
        }
    } else {
        LOG_ERROR(getName() << "Failed to close producer: " << strResult(result));
    }

    if (callback) {
        callback(result);
    }
}

Future<Result, ProducerImplBaseWeakPtr> ProducerImpl::getProducerCreatedFuture() {
    return producerCreatedPromise_.getFuture();
}

uint64_t ProducerImpl::getProducerId() const { return producerId_; }

void ProducerImpl::handleSendTimeout(const boost::system::error_code& err) {
    Lock lock(mutex_);
    if (state_ != Pending && state_ != Ready) {
        return;
    }

    if (err == boost::asio::error::operation_aborted) {
        LOG_DEBUG(getName() << "Timer cancelled: " << err.message());
        return;
    } else if (err) {
        LOG_ERROR(getName() << "Timer error: " << err.message());
        return;
    }

    std::shared_ptr<PendingCallbacks> pendingCallbacks;
    if (pendingMessagesQueue_.empty()) {
        // If there are no pending messages, reset the timeout to the configured value.
        sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
        LOG_DEBUG(getName() << "Producer timeout triggered on empty pending message queue");
    } else {
        // If there is at least one message, calculate the diff between the message timeout and
        // the current time.
        time_duration diff = pendingMessagesQueue_.front().timeout_ - TimeUtils::now();
        if (diff.total_milliseconds() <= 0) {
            // The diff is less than or equal to zero, meaning that the message has been expired.
            LOG_DEBUG(getName() << "Timer expired. Calling timeout callbacks.");
            pendingCallbacks = getPendingCallbacksWhenFailed();
            // Since the pending queue is cleared now, set timer to expire after configured value.
            sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
        } else {
            // The diff is greater than zero, set the timeout to the diff value
            LOG_DEBUG(getName() << "Timer hasn't expired yet, setting new timeout " << diff);
            sendTimer_->expires_from_now(diff);
        }
    }

    // Asynchronously wait for the timeout to trigger
    sendTimer_->async_wait(
        std::bind(&ProducerImpl::handleSendTimeout, shared_from_this(), std::placeholders::_1));
    lock.unlock();
    if (pendingCallbacks) {
        pendingCallbacks->complete(ResultTimeout);
    }
}

bool ProducerImpl::removeCorruptMessage(uint64_t sequenceId) {
    Lock lock(mutex_);
    if (pendingMessagesQueue_.empty()) {
        LOG_DEBUG(getName() << " -- SequenceId - " << sequenceId << "]"  //
                            << "Got send failure for expired message, ignoring it.");
        return true;
    }

    OpSendMsg op = pendingMessagesQueue_.front();
    uint64_t expectedSequenceId = op.sequenceId_;
    if (sequenceId > expectedSequenceId) {
        LOG_WARN(getName() << "Got ack failure for msg " << sequenceId                //
                           << " expecting: " << expectedSequenceId << " queue size="  //
                           << pendingMessagesQueue_.size() << " producer: " << producerId_);
        return false;
    } else if (sequenceId < expectedSequenceId) {
        LOG_DEBUG(getName() << "Corrupt message is already timed out. Ignoring msg " << sequenceId);
        return true;
    } else {
        LOG_DEBUG(getName() << "Remove corrupt message from queue " << sequenceId);
        pendingMessagesQueue_.pop_front();
        lock.unlock();
        if (op.sendCallback_) {
            // to protect from client callback exception
            try {
                op.sendCallback_(ResultChecksumError, op.msg_.getMessageId());
            } catch (const std::exception& e) {
                LOG_ERROR(getName() << "Exception thrown from callback " << e.what());
            }
        }
        releaseSemaphoreForSendOp(op);
        return true;
    }
}

bool ProducerImpl::ackReceived(uint64_t sequenceId, MessageId& rawMessageId) {
    MessageId messageId(partition_, rawMessageId.ledgerId(), rawMessageId.entryId(),
                        rawMessageId.batchIndex());
    Lock lock(mutex_);

    if (pendingMessagesQueue_.empty()) {
        LOG_DEBUG(getName() << " -- SequenceId - " << sequenceId << "]"  //
                            << " -- MessageId - " << messageId << "]"
                            << "Got an SEND_ACK for expired message, ignoring it.");
        return true;
    }

    OpSendMsg op = pendingMessagesQueue_.front();
    uint64_t expectedSequenceId = op.sequenceId_;
    if (sequenceId > expectedSequenceId) {
        LOG_WARN(getName() << "Got ack for msg " << sequenceId                        //
                           << " expecting: " << expectedSequenceId << " queue size="  //
                           << pendingMessagesQueue_.size() << " producer: " << producerId_);
        return false;
    } else if (sequenceId < expectedSequenceId) {
        // Ignoring the ack since it's referring to a message that has already timed out.
        LOG_DEBUG(getName() << "Got ack for timed out msg " << sequenceId  //
                            << " -- MessageId - " << messageId << " last-seq: " << expectedSequenceId
                            << " producer: " << producerId_);
        return true;
    } else {
        // Message was persisted correctly
        LOG_DEBUG(getName() << "Received ack for msg " << sequenceId);
        releaseSemaphoreForSendOp(op);
        lastSequenceIdPublished_ = sequenceId + op.messagesCount_ - 1;

        pendingMessagesQueue_.pop_front();

        lock.unlock();
        if (op.sendCallback_) {
            try {
                op.sendCallback_(ResultOk, messageId);
            } catch (const std::exception& e) {
                LOG_ERROR(getName() << "Exception thrown from callback " << e.what());
            }
        }
        return true;
    }
}

bool ProducerImpl::encryptMessage(proto::MessageMetadata& metadata, SharedBuffer& payload,
                                  SharedBuffer& encryptedPayload) {
    if (!conf_.isEncryptionEnabled() || msgCrypto_ == NULL) {
        encryptedPayload = payload;
        return true;
    }

    return msgCrypto_->encrypt(conf_.getEncryptionKeys(), conf_.getCryptoKeyReader(), metadata, payload,
                               encryptedPayload);
}

void ProducerImpl::disconnectProducer() {
    LOG_DEBUG("Broker notification of Closed producer: " << producerId_);
    Lock lock(mutex_);
    connection_.reset();
    lock.unlock();
    scheduleReconnection(shared_from_this());
}

void ProducerImpl::start() {
    HandlerBase::start();

    if (conf_.getLazyStartPartitionedProducers()) {
        // we need to kick it off now as it is possible that the connection may take
        // longer than sendTimeout to connect
        startSendTimeoutTimer();
    }
}

void ProducerImpl::shutdown() {
    Lock lock(mutex_);
    state_ = Closed;
    cancelTimers();
    producerCreatedPromise_.setFailed(ResultAlreadyClosed);
}

void ProducerImpl::cancelTimers() {
    if (dataKeyGenTImer_) {
        dataKeyGenTImer_->cancel();
        dataKeyGenTImer_.reset();
    }

    if (batchTimer_) {
        batchTimer_->cancel();
        batchTimer_.reset();
    }

    if (sendTimer_) {
        sendTimer_->cancel();
        sendTimer_.reset();
    }
}

bool ProducerImplCmp::operator()(const ProducerImplPtr& a, const ProducerImplPtr& b) const {
    return a->getProducerId() < b->getProducerId();
}

bool ProducerImpl::isClosed() {
    Lock lock(mutex_);
    return state_ == Closed;
}

bool ProducerImpl::isConnected() const {
    Lock lock(mutex_);
    return !getCnx().expired() && state_ == Ready;
}

uint64_t ProducerImpl::getNumberOfConnectedProducer() { return isConnected() ? 1 : 0; }

bool ProducerImpl::isStarted() const {
    Lock lock(mutex_);
    return state_ != NotStarted;
}
void ProducerImpl::startSendTimeoutTimer() {
    // Initialize the sendTimer only once per producer and only when producer timeout is
    // configured. Set the timeout as configured value and asynchronously wait for the
    // timeout to happen.
    if (!sendTimer_ && conf_.getSendTimeout() > 0) {
        sendTimer_ = executor_->createDeadlineTimer();
        sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
        sendTimer_->async_wait(
            std::bind(&ProducerImpl::handleSendTimeout, shared_from_this(), std::placeholders::_1));
    }
}

}  // namespace pulsar
/* namespace pulsar */
