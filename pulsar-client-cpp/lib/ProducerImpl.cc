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

#include "ProducerImpl.h"
#include "LogUtils.h"
#include "MessageImpl.h"
#include "PulsarApi.pb.h"
#include "Commands.h"
#include "DestinationName.h"
#include "BatchMessageContainer.h"
#include <boost/bind.hpp>
#include <boost/date_time/local_time/local_time.hpp>

using namespace pulsar;
namespace pulsar {
DECLARE_LOG_OBJECT()

OpSendMsg ::OpSendMsg()
        : msg_(),
          sendCallback_(),
          producerId_(),
          sequenceId_(),
          timeout_() {
}

OpSendMsg::OpSendMsg(uint64_t producerId, uint64_t sequenceId, const Message& msg,
                     const SendCallback& sendCallback, const ProducerConfiguration& conf)
        : msg_(msg),
          sendCallback_(sendCallback),
          producerId_(producerId),
          sequenceId_(sequenceId),
          timeout_(now() + milliseconds(conf.getSendTimeout())) {
}

ProducerImpl::ProducerImpl(ClientImplPtr client, const std::string& topic,
                           const ProducerConfiguration& producerConfiguration)
        : HandlerBase(client, topic),
          conf_(producerConfiguration),
          executor_(client->getIOExecutorProvider()->get()),
          pendingMessagesQueue_(conf_.getMaxPendingMessages()),
          producerStr_("[" + topic_ + ", " + producerName_ + "] "),
          producerId_(client->newProducerId()),
          msgSequenceGenerator_(0),
          sendTimer_() {
    LOG_DEBUG(
            "ProducerName - " << producerName_ << " Created producer on topic " << topic_ << " id: " << producerId_);
    // boost::ref is used to drop the constantness constraint of make_shared
    if(conf_.getBatchingEnabled()) {
        batchMessageContainer = boost::make_shared<BatchMessageContainer>(boost::ref(*this));
    }
    numOfMsgPublished = 0;
    numOfSendAsyncCalls = 0;
    numOfMsgAckSuccessfully = 0;
}

ProducerImpl::~ProducerImpl() {
    LOG_DEBUG(getName() << "~ProducerImpl");
    closeAsync(ResultCallback());
    printStats();
}

const std::string& ProducerImpl::getTopic() const {
    return topic_;
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

    SharedBuffer cmd = Commands::newProducer(topic_, producerId_, producerName_, requestId);
    cnx->sendRequestWithId(cmd, requestId).addListener(
            boost::bind(&ProducerImpl::handleCreateProducer, shared_from_this(), cnx, _1, _2));
}

void ProducerImpl::connectionFailed(Result result) {
    // Keep a reference to ensure object is kept alive
    ProducerImplPtr ptr = shared_from_this();

    if (producerCreatedPromise_.setFailed(result)) {
        Lock lock(mutex_);
        state_ = Failed;
    }
}

void ProducerImpl::handleCreateProducer(const ClientConnectionPtr& cnx, Result result,
                                        const std::string& producerName) {
    LOG_DEBUG(getName() << "ProducerImpl::handleCreateProducer res: " << strResult(result));

    if (result == ResultOk) {
        // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
        // set the cnx pointer so that new messages will be sent immediately
        LOG_INFO(getName() << "Created producer on broker " << cnx->cnxString());

        Lock lock(mutex_);
        cnx->registerProducer(producerId_, shared_from_this());
        producerName_ = producerName;
        producerStr_ = "[" + topic_ + ", " + producerName_ + "] ";
        if (batchMessageContainer) {
            batchMessageContainer->producerName_ = producerName_;
        }
        resendMessages(cnx);
        connection_ = cnx;
        state_ = Ready;
        backoff_.reset();
        lock.unlock();

        // Initialize the sendTimer only once per producer and only when producer timeout is
        // configured. Set the timeout as configured value and asynchronously wait for the
        // timeout to happen.
        if (!sendTimer_ && conf_.getSendTimeout() > 0) {
            sendTimer_ = executor_->createDeadlineTimer();
            sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
            sendTimer_->async_wait(
                    boost::bind(&ProducerImpl::handleSendTimeout, shared_from_this(), _1));
        }

        producerCreatedPromise_.setValue(shared_from_this());

    } else {
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
                failPendingMessages(ResultProducerBlockedQuotaExceededException);
            } else if (result == ResultProducerBlockedQuotaExceededError) {
                LOG_WARN(
                        getName() << "Producer is blocked on creation because backlog is exceeded on topic");
            }

            // Producer had already been initially created, we need to retry connecting in any case
            LOG_WARN(getName() << "Failed to reconnect producer: " << strResult(result));
            scheduleReconnection(shared_from_this());
        } else {
            // Producer was not yet created, retry to connect to broker if it's possible
            if (isRetriableError(result) && (creationTimestamp_ + operationTimeut_ < now())) {
                LOG_WARN(getName() << "Temporary error in creating producer: " << strResult(result));
                scheduleReconnection(shared_from_this());
            } else {
                LOG_ERROR(getName() << "Failed to create producer: " << strResult(result));
                producerCreatedPromise_.setFailed(result);
                Lock lock(mutex_);
                state_ = Failed;
            }
        }
    }
}

void ProducerImpl::failPendingMessages(Result result) {
    std::vector<OpSendMsg> messagesToFail;
    Lock lock(mutex_);
    messagesToFail.reserve(pendingMessagesQueue_.size());
    LOG_DEBUG(getName() << "# messages in pending queue : " << pendingMessagesQueue_.size());

    // Iterate over a copy of the pending messages queue, to trigger the future completion
    // without holding producer mutex.
    for (MessageQueue::const_iterator it = pendingMessagesQueue_.begin();
            it != pendingMessagesQueue_.end(); it++) {
        messagesToFail.push_back(*it);
    }

    BatchMessageContainer::MessageContainerListPtr messageContainerListPtr;
    if (batchMessageContainer) {
        messageContainerListPtr = batchMessageContainer->messagesContainerListPtr_;
        batchMessageContainer->clear();
    }
    pendingMessagesQueue_.clear();
    lock.unlock();
    for (std::vector<OpSendMsg>::const_iterator it = messagesToFail.begin();
            it != messagesToFail.end(); it++) {
        it->sendCallback_(result, it->msg_);
    }

    // this function can handle null pointer
    BatchMessageContainer::batchMessageCallBack(ResultTimeout, messageContainerListPtr);
}

void ProducerImpl::resendMessages(ClientConnectionPtr cnx) {
    if (pendingMessagesQueue_.empty()) {
        return;
    }

    LOG_DEBUG(getName() << "Re-Sending " << pendingMessagesQueue_.size() << " messages to server");

    for (MessageQueue::const_iterator it = pendingMessagesQueue_.begin();
            it != pendingMessagesQueue_.end(); ++it) {
        LOG_DEBUG(getName() << "Re-Sending " << it->sequenceId_);
        cnx->sendMessage(*it);
    }
}

void ProducerImpl::setMessageMetadata(const Message &msg, const uint64_t& sequenceId, const uint32_t& uncompressedSize) {
    // Call this function after acquiring the mutex_
    proto::MessageMetadata& msgMetadata = msg.impl_->metadata;
    if (! batchMessageContainer) {
        msgMetadata.set_producer_name(producerName_);
    }
    msgMetadata.set_publish_time(currentTimeMillis());
    msgMetadata.set_sequence_id(sequenceId);
    if (conf_.getCompressionType() != CompressionNone) {
        msgMetadata.set_compression(
                CompressionCodecProvider::convertType(conf_.getCompressionType()));
        msgMetadata.set_uncompressed_size(uncompressedSize);
    }
}

void ProducerImpl::sendAsync(const Message& msg, SendCallback callback) {
    if (msg.getLength() > Commands::MaxMessageSize) {
        callback(ResultMessageTooBig, msg);
        return;
    }

    // Reserve a spot in the messages queue before acquiring the ProducerImpl
    // mutex. When the queue is full, this call will block until a spot is
    // available.
    if (conf_.getBlockIfQueueFull()) {
        pendingMessagesQueue_.reserve(1);
    }

    // Compress the payload if required
    SharedBuffer& payload = msg.impl_->payload;

    uint32_t uncompressedSize = payload.readableBytes();

    if (! batchMessageContainer) {
        // If batching is enabled we compress all the payloads together before sending the batch
        payload = CompressionCodecProvider::getCodec(conf_.getCompressionType()).encode(payload);
    }

    Lock lock(mutex_);
    numOfSendAsyncCalls++;
    if (state_ != Ready) {
        lock.unlock();
        if (conf_.getBlockIfQueueFull()) {
            pendingMessagesQueue_.release(1);
        }
        callback(ResultAlreadyClosed, msg);
        return;
    }

    if (msg.impl_->metadata.has_producer_name()) {
        // Message had already been sent before
        lock.unlock();
        if (conf_.getBlockIfQueueFull()) {
            pendingMessagesQueue_.release(1);
        }
        callback(ResultInvalidMessage, msg);
        return;
    }

    setMessageMetadata(msg, msgSequenceGenerator_++, uncompressedSize);

    // reserving a spot and going forward - not blocking
    if (!conf_.getBlockIfQueueFull() && !pendingMessagesQueue_.tryReserve(1)) {
        LOG_DEBUG(getName() << " - Producer Queue is full");
        // If queue is full sending the batch immediately, no point waiting till batchMessageimeout
        if (batchMessageContainer) {
            LOG_DEBUG(getName() << " - sending batch message immediately");
            batchMessageContainer->sendMessage();
        }
        lock.unlock();
        callback(ResultProducerQueueIsFull, msg);
        return;
    }

    // If we reach this point then you have a reserved spot on the queue

    if (batchMessageContainer) {  // Batching is enabled
        batchMessageContainer->add(msg, callback);
        return;
    }
    sendMessage(msg, callback);
}

// Precondition -
// a. we have a reserved spot on the queue
// b. call this function after acquiring the ProducerImpl mutex_
void ProducerImpl::sendMessage(const Message& msg, SendCallback callback) {
    const uint64_t& sequenceId = msg.impl_->metadata.sequence_id();
    LOG_DEBUG(
            getName() << "Sending msg: " << sequenceId << " -- queue_size: " << pendingMessagesQueue_.size());

    OpSendMsg op(producerId_, sequenceId, msg, callback, conf_);

    LOG_DEBUG("Inserting data to pendingMessagesQueue_");
    pendingMessagesQueue_.push(op, true);
    numOfMsgPublished++;
    LOG_DEBUG("Completed Inserting data to pendingMessagesQueue_");

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
        LOG_DEBUG(getName() << " Ignoring timer cancelled event, code[" << ec <<"]");
        return;
    }
    LOG_DEBUG(getName() << " - Batch Message Timer expired");
    Lock lock(mutex_);
    batchMessageContainer->sendMessage();
}

void ProducerImpl::printStats() {
    if (batchMessageContainer) {
        LOG_INFO("Producer - " << producerStr_ << ", [numOfMsgPublished = " << numOfMsgPublished
        << "] [numOfMsgAckSuccessfully = " << numOfMsgAckSuccessfully
        << "] [numOfSendAsyncCalls =" << numOfSendAsyncCalls << "] [batchMessageContainer = "
        << *batchMessageContainer << "]");
    } else {
        LOG_INFO("Producer - " << producerStr_ << ", [numOfMsgPublished = " << numOfMsgPublished
        << "] [numOfMsgAckSuccessfully = " << numOfMsgAckSuccessfully
        << "] [numOfSendAsyncCalls =" << numOfSendAsyncCalls << "] [batching = off]");
    }
}

void ProducerImpl::closeAsync(CloseCallback callback) {
    Lock lock(mutex_);

    if (state_ != Ready) {
        lock.unlock();
        if (!callback.empty()) {
            callback(ResultAlreadyClosed);
        }
        return;
    }
    LOG_DEBUG(getName() << "Closing producer");
    state_ = Closing;

    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        lock.unlock();
        if (!callback.empty()) {
            callback(ResultOk);
        }
        return;
    }

    // Detach the producer from the connection to avoid sending any other
    // message from the producer
    connection_.reset();
    lock.unlock();

    ClientImplPtr client = client_.lock();
    if (!client) {
        // Client was already destroyed
        if (!callback.empty()) {
            callback(ResultOk);
        }
        return;
    }
    int requestId = client->newRequestId();
    Future<Result, std::string> future = cnx->sendRequestWithId(
            Commands::newCloseProducer(producerId_, requestId), requestId);
    if (!callback.empty()) {
        future.addListener(
                boost::bind(&ProducerImpl::handleClose, shared_from_this(), _1, callback));
    }
}

void ProducerImpl::handleClose(Result result, ResultCallback callback) {
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

    callback(result);
}

Future<Result, ProducerImplBaseWeakPtr> ProducerImpl::getProducerCreatedFuture() {
    return producerCreatedPromise_.getFuture();
}

uint64_t ProducerImpl::getProducerId() const {
    return producerId_;
}

void ProducerImpl::handleSendTimeout(const boost::system::error_code& err) {
    if (err == boost::asio::error::operation_aborted) {
        LOG_DEBUG(getName() << "Timer cancelled: " << err.message());
        return;
    } else if (err) {
        LOG_ERROR(getName() << "Timer error: " << err.message());
        return;
    }

    OpSendMsg msg;
    if (!pendingMessagesQueue_.peek(msg)) {
        // If there are no pending messages, reset the timeout to the configured value.
        sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
        LOG_DEBUG(getName() << "Producer timeout triggered on empty pending message queue");
    } else {
        // If there is at least one message, calculate the diff between the message timeout and
        // the current time.
        time_duration diff = msg.timeout_ - now();
        if (diff.total_milliseconds() <= 0) {
            // The diff is less than or equal to zero, meaning that the message has been expired.
            LOG_DEBUG(getName() << "Timer expired. Calling timeout callbacks.");
            failPendingMessages(ResultTimeout);
            // Since the pending queue is cleared now, set timer to expire after configured value.
            sendTimer_->expires_from_now(milliseconds(conf_.getSendTimeout()));
        } else {
            // The diff is greater than zero, set the timeout to the diff value
            LOG_DEBUG(getName() << "Timer hasn't expired yet, setting new timeout " << diff);
            sendTimer_->expires_from_now(diff);
        }
    }

    // Asynchronously wait for the timeout to trigger
    sendTimer_->async_wait(boost::bind(&ProducerImpl::handleSendTimeout, shared_from_this(), _1));
}

bool ProducerImpl::removeCorruptMessage(uint64_t sequenceId) {

    OpSendMsg op;
    Lock lock(mutex_);
    bool havePendingAck = pendingMessagesQueue_.peek(op);
    if (!havePendingAck) {
        LOG_DEBUG(getName() << " -- SequenceId - " << sequenceId << "]"  //
                << "Got send failure for expired message, ignoring it.");
        return true;
    }
    uint64_t expectedSequenceId = op.sequenceId_;
    if (sequenceId > expectedSequenceId) {
        LOG_WARN(getName() << "Got ack failure for msg " << sequenceId  //
                << " expecting: " << expectedSequenceId << " queue size="//
                << pendingMessagesQueue_.size() << " producer: " << producerId_);
        return false;
    } else if (sequenceId < expectedSequenceId) {
        LOG_DEBUG(getName() << "Corrupt message is already timed out. Ignoring msg " << sequenceId);
        return true;
    } else {
        LOG_DEBUG(getName() << "Remove corrupt message from queue " << sequenceId);
        pendingMessagesQueue_.pop();
        numOfMsgAckSuccessfully++;
        if (op.msg_.impl_->metadata.has_num_messages_in_batch()) {
            // batch message - need to release more spots
            // -1 since the pushing batch message into the queue already released a spot
            pendingMessagesQueue_.release(op.msg_.impl_->metadata.num_messages_in_batch() - 1);
        }
        lock.unlock();
        if (op.sendCallback_) {
            // to protect from client callback exception
            try {
                op.sendCallback_(ResultChecksumError, op.msg_);
            } catch (const std::exception& e) {
                LOG_ERROR(getName() << "Exception thrown from callback " << e.what());
            }
        }
        return true;
    }

}

bool ProducerImpl::ackReceived(uint64_t sequenceId) {
    OpSendMsg op;
    Lock lock(mutex_);
    bool havePendingAck = pendingMessagesQueue_.peek(op);
    if (!havePendingAck) {
        LOG_DEBUG(getName() << " -- SequenceId - " << sequenceId << "]"  //
                << "Got an SEND_ACK for expired message, ignoring it.");
        return true;
    }
    uint64_t expectedSequenceId = op.sequenceId_;
    if (sequenceId > expectedSequenceId) {
        LOG_WARN(getName() << "Got ack for msg " << sequenceId  //
                << " expecting: " << expectedSequenceId << " queue size="//
                << pendingMessagesQueue_.size() << " producer: " << producerId_);
        return false;
    } else if (sequenceId < expectedSequenceId) {
        // Ignoring the ack since it's referring to a message that has already timed out.
        LOG_DEBUG(getName() << "Got ack for timed out msg " << sequenceId  //
                << " last-seq: " << expectedSequenceId << " producer: " << producerId_);
        return true;
    } else {
        // Message was persisted correctly
        LOG_DEBUG(getName() << "Received ack for msg " << sequenceId);
        pendingMessagesQueue_.pop();
        numOfMsgAckSuccessfully++;
        if (op.msg_.impl_->metadata.has_num_messages_in_batch()) {
            // batch message - need to release more spots
            // -1 since the pushing batch message into the queue already released a spot
            pendingMessagesQueue_.release(op.msg_.impl_->metadata.num_messages_in_batch() - 1);
        }
        lock.unlock();
        if (op.sendCallback_) {
            try {
                op.sendCallback_(ResultOk, op.msg_);
            } catch (const std::exception& e) {
                LOG_ERROR(getName() << "Exception thrown from callback " << e.what());
            }
        }
        return true;
    }
}

void ProducerImpl::disconnectProducer() {
	LOG_DEBUG("Broker notification of Closed producer: " << producerId_);
	connection_.reset();
	scheduleReconnection(shared_from_this());

}

const std::string& ProducerImpl::getName() const{
    return producerStr_;
}

void ProducerImpl::start() {
    HandlerBase::start();
}
void ProducerImpl::shutdown() {
    Lock lock(mutex_);
    state_ = Closed;
    sendTimer_.reset();
    producerCreatedPromise_.setFailed(ResultAlreadyClosed);
}

bool ProducerImplCmp::operator()(const ProducerImplPtr &a, const ProducerImplPtr &b) const {
    return a->getProducerId() < b->getProducerId();
}

bool ProducerImpl::isClosed() {
    Lock lock(mutex_);
    return state_ == Closed;
}

}
/* namespace pulsar */
