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
#include "BatchMessageContainer.h"
#include <memory>
#include <functional>

namespace pulsar {

static ObjectPool<MessageImpl, 1000> messagePool;
static ObjectPool<BatchMessageContainer::MessageContainerList, 1000> messageContainerListPool;
DECLARE_LOG_OBJECT()

BatchMessageContainer::BatchMessageContainer(ProducerImpl& producer)
    : maxAllowedNumMessagesInBatch_(producer.conf_.getBatchingMaxMessages()),
      maxAllowedMessageBatchSizeInBytes_(producer.conf_.getBatchingMaxAllowedSizeInBytes()),
      topicName_(producer.topic_),
      producerName_(producer.producerName_),
      compressionType_(producer.conf_.getCompressionType()),
      producer_(producer),
      impl_(messagePool.create()),
      timer_(producer.executor_->createDeadlineTimer()),
      batchSizeInBytes_(0),
      messagesContainerListPtr_(messageContainerListPool.create()),
      averageBatchSize_(0),
      numberOfBatchesSent_(0) {
    messagesContainerListPtr_->reserve(1000);
    LOG_INFO(*this << " BatchMessageContainer constructed");
}

bool BatchMessageContainer::add(const Message& msg, SendCallback sendCallback, bool disableCheck) {
    // disableCheck is needed to avoid recursion in case the batchSizeInKB < IndividualMessageSizeInKB
    LOG_DEBUG(*this << " Called add function for [message = " << msg << "] [disableCheck = " << disableCheck
                    << "]");
    if (!(disableCheck || hasSpaceInBatch(msg))) {
        LOG_DEBUG(*this << " Batch is full");
        bool hasMessages = !messagesContainerListPtr_->empty();
        bool pushedToPendingQueue = sendMessage(NULL);
        bool result = add(msg, sendCallback, true);
        if (hasMessages && !pushedToPendingQueue) {
            // The msg failed to be pushed to the producer's queue, so the reserved spot before won't be
            // released and we must return false to tell the producer to release the spot.
            // Exceptionally, `hasSpaceInBatch` returns false just because `msg` is too big before compressed,
            // while there're no messages before. In this case, the spots have already been released so we
            // can't return false simply.
            return false;
        }
        return result;
    }
    if (messagesContainerListPtr_->empty()) {
        // First message to be added
        startTimer();
        Commands::initBatchMessageMetadata(msg, impl_->metadata);
        // TODO - add this to Commands.cc
        impl_->metadata.set_producer_name(producerName_);
    }
    batchSizeInBytes_ += msg.impl_->payload.readableBytes();

    LOG_DEBUG(*this << " Before serialization payload size in bytes = " << impl_->payload.readableBytes());
    Commands::serializeSingleMessageInBatchWithPayload(msg, impl_->payload,
                                                       maxAllowedMessageBatchSizeInBytes_);
    LOG_DEBUG(*this << " After serialization payload size in bytes = " << impl_->payload.readableBytes());

    messagesContainerListPtr_->emplace_back(msg, sendCallback);

    LOG_DEBUG(*this << " Number of messages in Batch = " << messagesContainerListPtr_->size());
    LOG_DEBUG(*this << " Batch Payload Size In Bytes = " << batchSizeInBytes_);
    bool hasOnlyOneMessage = (messagesContainerListPtr_->size() == 1);
    if (isFull()) {
        LOG_DEBUG(*this << " Batch is full.");
        // If there're more than one messages in the batch, even if it was pushed to the queue successfully,
        // we also returns false to release one spot, because there're two spots to be released. One is
        // reserved when the first message arrived, another is reserved when the current message arrived.
        bool pushedToPendingQueue = sendMessage(NULL);
        return hasOnlyOneMessage && pushedToPendingQueue;
    }
    // A batch of messages only need one spot, so returns false when more messages were added to the batch,
    // then outer ProducerImpl::sendAsync() will release unnecessary reserved spots
    return hasOnlyOneMessage;
}

void BatchMessageContainer::startTimer() {
    const unsigned long& publishDelayInMs = producer_.conf_.getBatchingMaxPublishDelayMs();
    LOG_DEBUG(*this << " Timer started with expiry after " << publishDelayInMs);
    timer_->expires_from_now(boost::posix_time::milliseconds(publishDelayInMs));
    timer_->async_wait(
        std::bind(&pulsar::ProducerImpl::batchMessageTimeoutHandler, &producer_, std::placeholders::_1));
}

bool BatchMessageContainer::sendMessage(FlushCallback flushCallback) {
    // Call this function after acquiring the ProducerImpl lock
    LOG_DEBUG(*this << "Sending the batch message container");
    if (isEmpty()) {
        LOG_DEBUG(*this << " Batch is empty - returning.");
        if (flushCallback) {
            flushCallback(ResultOk);
        }
        return false;
    }
    impl_->metadata.set_num_messages_in_batch(messagesContainerListPtr_->size());
    compressPayLoad();

    SharedBuffer encryptedPayload;
    if (!producer_.encryptMessage(impl_->metadata, impl_->payload, encryptedPayload)) {
        batchMessageCallBack(ResultCryptoError, MessageId{}, messagesContainerListPtr_, nullptr);
        clear();
        return false;
    }
    impl_->payload = encryptedPayload;

    if (impl_->payload.readableBytes() > producer_.keepMaxMessageSize_) {
        // At this point the compressed batch is above the overall MaxMessageSize. There
        // can only 1 single message in the batch at this point.
        batchMessageCallBack(ResultMessageTooBig, MessageId{}, messagesContainerListPtr_, nullptr);
        clear();
        return false;
    }

    Message msg;
    msg.impl_ = impl_;

    // bind keeps a copy of the parameters
    SendCallback callback = std::bind(&BatchMessageContainer::batchMessageCallBack, std::placeholders::_1,
                                      std::placeholders::_2, messagesContainerListPtr_, flushCallback);

    producer_.sendMessage(msg, callback);
    clear();
    return true;
}

void BatchMessageContainer::compressPayLoad() {
    if (compressionType_ != CompressionNone) {
        impl_->metadata.set_compression(CompressionCodecProvider::convertType(compressionType_));
        impl_->metadata.set_uncompressed_size(impl_->payload.readableBytes());
    }
    impl_->payload = CompressionCodecProvider::getCodec(compressionType_).encode(impl_->payload);
}

SharedBuffer BatchMessageContainer::getBatchedPayload() { return impl_->payload; }

void BatchMessageContainer::clear() {
    LOG_DEBUG(*this << " BatchMessageContainer::clear() called");
    timer_->cancel();
    averageBatchSize_ = (messagesContainerListPtr_->size() + (averageBatchSize_ * numberOfBatchesSent_)) /
                        (numberOfBatchesSent_ + 1);
    numberOfBatchesSent_++;
    messagesContainerListPtr_ = messageContainerListPool.create();
    // Try to optimize this
    messagesContainerListPtr_->reserve(10000);
    impl_ = messagePool.create();
    batchSizeInBytes_ = 0;
}

void BatchMessageContainer::batchMessageCallBack(Result r, const MessageId& messageId,
                                                 MessageContainerListPtr messagesContainerListPtr,
                                                 FlushCallback flushCallback) {
    if (!messagesContainerListPtr) {
        if (flushCallback) {
            flushCallback(ResultOk);
        }
        return;
    }
    LOG_DEBUG("BatchMessageContainer::batchMessageCallBack called with [Result = "
              << r << "] [numOfMessages = " << messagesContainerListPtr->size() << "]");
    size_t batch_size = messagesContainerListPtr->size();
    for (size_t i = 0; i < batch_size; i++) {
        MessageId messageIdInBatch(messageId.partition(), messageId.ledgerId(), messageId.entryId(), i);
        messagesContainerListPtr->operator[](i).callBack(r, messageIdInBatch);
    }
    if (flushCallback) {
        flushCallback(ResultOk);
    }
}

BatchMessageContainer::~BatchMessageContainer() {
    timer_->cancel();
    LOG_DEBUG(*this << " BatchMessageContainer Object destructed");
    LOG_INFO("[numberOfBatchesSent = " << numberOfBatchesSent_
                                       << "] [averageBatchSize = " << averageBatchSize_ << "]");
}
}  // namespace pulsar
