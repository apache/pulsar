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
#include "ProducerImpl.h"
#include "TimeUtils.h"
#include <stdexcept>

DECLARE_LOG_OBJECT()

namespace pulsar {

BatchMessageContainer::BatchMessageContainer(const ProducerImpl& producer)
    : BatchMessageContainerBase(producer) {}

BatchMessageContainer::~BatchMessageContainer() {
    LOG_DEBUG(*this << " destructed");
    LOG_DEBUG("[numberOfBatchesSent = " << numberOfBatchesSent_
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
    return createOpSendMsgHelper(opSendMsg, flushCallback, batch_);
}

std::vector<Result> BatchMessageContainer::createOpSendMsgs(std::vector<OpSendMsg>& opSendMsgs,
                                                            const FlushCallback& flushCallback) const {
    throw std::runtime_error("createOpSendMsgs is not supported for BatchMessageContainer");
}

void BatchMessageContainer::serialize(std::ostream& os) const {
    os << "{ BatchMessageContainer [size = " << numMessages_    //
       << "] [bytes = " << sizeInBytes_                         //
       << "] [maxSize = " << getMaxNumMessages()                //
       << "] [maxBytes = " << getMaxSizeInBytes()               //
       << "] [topicName = " << topicName_                       //
       << "] [numberOfBatchesSent_ = " << numberOfBatchesSent_  //
       << "] [averageBatchSize_ = " << averageBatchSize_        //
       << "] }";
}

}  // namespace pulsar
