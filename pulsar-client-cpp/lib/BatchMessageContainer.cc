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
#include "ClientConnection.h"
#include "Commands.h"
#include "LogUtils.h"
#include "MessageImpl.h"
#include "ObjectPool.h"
#include "ProducerImpl.h"
#include "TimeUtils.h"
#include <stdexcept>

DECLARE_LOG_OBJECT()

namespace pulsar {

BatchMessageContainer::BatchMessageContainer(const ProducerImpl& producer)
    : BatchMessageContainerBase(producer) {}

BatchMessageContainer::~BatchMessageContainer() {
    LOG_DEBUG(*this << " destructed");
    LOG_INFO("[numberOfBatchesSent = " << numberOfBatchesSent_
                                       << "] [averageBatchSize_ = " << averageBatchSize_ << "]");
}

bool BatchMessageContainer::add(const Message& msg, const SendCallback& callback) {
    LOG_DEBUG("Before add: " << *this << " [message = " << msg << "]");
    batch_.add(msg, callback);
    updateStats(msg);
    LOG_DEBUG("After add: " << *this);
    return isFull();
}

void BatchMessageContainer::clear() {
    averageBatchSize_ =
        (batch_.size() + averageBatchSize_ * numberOfBatchesSent_) / (numberOfBatchesSent_ + 1);
    numberOfBatchesSent_++;
    batch_.clear();
    resetStats();
    LOG_DEBUG(*this << " clear() called");
}

Result BatchMessageContainer::createOpSendMsg(OpSendMsg& opSendMsg,
                                              const FlushCallback& flushCallback) const {
    if (batch_.empty()) {
        return ResultOperationNotSupported;
    }

    MessageImplPtr impl = batch_.msgImpl();
    impl->metadata.set_num_messages_in_batch(batch_.size());
    auto compressionType = producerConfig_.getCompressionType();
    if (compressionType != CompressionNone) {
        impl->metadata.set_compression(CompressionCodecProvider::convertType(compressionType));
        impl->metadata.set_uncompressed_size(impl->payload.readableBytes());
    }
    impl->payload = CompressionCodecProvider::getCodec(compressionType).encode(impl->payload);

    SharedBuffer encryptedPayload;
    if (!encryptMessage(impl->metadata, impl->payload, encryptedPayload)) {
        return ResultCryptoError;
    }

    if (impl->payload.readableBytes() > ClientConnection::getMaxMessageSize()) {
        return ResultMessageTooBig;
    }

    opSendMsg.msg_.impl_ = impl;
    if (flushCallback) {
        opSendMsg.sendCallback_ = [this, flushCallback](Result result, const MessageId& id) {
            batch_.complete(result, id);
            flushCallback(result);
        };
    } else {
        opSendMsg.sendCallback_ = batch_.createSendCallback();
    }
    opSendMsg.sequenceId_ = impl->metadata.sequence_id();
    opSendMsg.producerId_ = producerId_;
    opSendMsg.timeout_ = TimeUtils::now() + milliseconds(producerConfig_.getSendTimeout());

    return ResultOk;
}

std::vector<Result> BatchMessageContainer::createOpSendMsgs(std::vector<OpSendMsg>& opSendMsgs,
                                                            const FlushCallback& flushCallback) const {
    throw std::runtime_error("createOpSendMsgs is not supported for BatchMessageContainer");
}

void BatchMessageContainer::serialize(std::ostream& os) const {
    os << "{ BatchMessageContainer [ size = " << numMessages_    //
       << "] [ bytes = " << sizeInBytes_                         //
       << "] [ maxSize = " << getMaxNumMessages()                //
       << "] [ maxBytes = " << getMaxSizeInBytes()               //
       << "] [ topicName = " << topicName_                       //
       << "] [ numberOfBatchesSent_ = " << numberOfBatchesSent_  //
       << "] [ averageBatchSize_ = " << averageBatchSize_        //
       << "]}";
}

}  // namespace pulsar
