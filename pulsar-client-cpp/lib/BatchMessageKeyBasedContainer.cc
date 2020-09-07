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
#include "BatchMessageKeyBasedContainer.h"
#include "ClientConnection.h"
#include "Commands.h"
#include "LogUtils.h"
#include "MessageImpl.h"
#include "ProducerImpl.h"
#include "TimeUtils.h"

#include <algorithm>
#include <map>

DECLARE_LOG_OBJECT()

namespace pulsar {

inline std::string getKey(const Message& msg) {
    return msg.hasOrderingKey() ? msg.getOrderingKey() : msg.getPartitionKey();
}

BatchMessageKeyBasedContainer::BatchMessageKeyBasedContainer(const ProducerImpl& producer)
    : BatchMessageContainerBase(producer) {}

BatchMessageKeyBasedContainer::~BatchMessageKeyBasedContainer() {
    LOG_DEBUG(*this << " destructed");
    LOG_INFO("[numberOfBatchesSent = " << numberOfBatchesSent_
                                       << "] [averageBatchSize_ = " << averageBatchSize_ << "]");
}

bool BatchMessageKeyBasedContainer::isFirstMessageToAdd(const Message& msg) const {
    auto it = batches_.find(getKey(msg));
    if (it == batches_.end()) {
        return true;
    } else {
        return it->second.empty();
    }
}

bool BatchMessageKeyBasedContainer::add(const Message& msg, const SendCallback& callback) {
    LOG_DEBUG("Before add: " << *this << " [message = " << msg << "]");
    batches_[getKey(msg)].add(msg, callback);
    updateStats(msg);
    LOG_DEBUG("After add: " << *this);
    return isFull();
}

void BatchMessageKeyBasedContainer::clear() {
    averageBatchSize_ =
        (numMessages_ + averageBatchSize_ * numberOfBatchesSent_) / (numberOfBatchesSent_ + batches_.size());
    numberOfBatchesSent_ += batches_.size();
    batches_.clear();
    resetStats();
    LOG_DEBUG(*this << " clear() called");
}

Result BatchMessageKeyBasedContainer::createOpSendMsg(OpSendMsg& opSendMsg,
                                                      const FlushCallback& flushCallback) const {
    if (batches_.size() < 1) {
        return ResultOperationNotSupported;
    }
    return createOpSendMsgHelper(opSendMsg, flushCallback, batches_.begin()->second);
}

std::vector<Result> BatchMessageKeyBasedContainer::createOpSendMsgs(
    std::vector<OpSendMsg>& opSendMsgs, const FlushCallback& flushCallback) const {
    // Sorted the batches by sequence id
    std::vector<const MessageAndCallbackBatch*> sortedBatches;
    for (const auto& kv : batches_) {
        sortedBatches.emplace_back(&kv.second);
    }
    std::sort(sortedBatches.begin(), sortedBatches.end(),
              [](const MessageAndCallbackBatch* lhs, const MessageAndCallbackBatch* rhs) {
                  return lhs->sequenceId() < rhs->sequenceId();
              });

    size_t numBatches = sortedBatches.size();
    opSendMsgs.resize(numBatches);

    std::vector<Result> results(numBatches);
    for (size_t i = 0; i + 1 < numBatches; i++) {
        results[i] = createOpSendMsgHelper(opSendMsgs[i], nullptr, *sortedBatches[i]);
    }
    if (numBatches > 0) {
        // Add flush callback to the last batch
        results.back() = createOpSendMsgHelper(opSendMsgs.back(), flushCallback, *sortedBatches.back());
    }
    return results;
}

void BatchMessageKeyBasedContainer::serialize(std::ostream& os) const {
    os << "{ BatchMessageKeyBasedContainer [size = " << numMessages_  //
       << "] [bytes = " << sizeInBytes_                               //
       << "] [maxSize = " << getMaxNumMessages()                      //
       << "] [maxBytes = " << getMaxSizeInBytes()                     //
       << "] [topicName = " << topicName_                             //
       << "] [numberOfBatchesSent_ = " << numberOfBatchesSent_        //
       << "] [averageBatchSize_ = " << averageBatchSize_              //
       << "]";

    std::map<std::string, const MessageAndCallbackBatch*> sortedBatches;
    for (const auto& kv : batches_) {
        sortedBatches.emplace(kv.first, &kv.second);
    }
    for (const auto& kv : sortedBatches) {
        const auto& key = kv.first;
        const auto& batch = *(kv.second);
        os << "\n  key: " << key << " | numMessages: " << batch.size();
    }
    os << " }";
}

}  // namespace pulsar
