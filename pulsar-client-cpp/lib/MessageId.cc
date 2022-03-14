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

#include <pulsar/defines.h>
#include <pulsar/MessageId.h>

#include "MessageIdImpl.h"
#include "ChunkMessageIdImpl.h"
#include "PulsarApi.pb.h"
#include "MessageIdUtil.h"

#include <iostream>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <math.h>
#include <memory>

namespace pulsar {

MessageId::MessageId() {
    static const MessageIdImplPtr emptyMessageId = std::make_shared<MessageIdImpl>();
    impl_ = emptyMessageId;
}

MessageId& MessageId::operator=(const MessageId& m) {
    impl_ = m.impl_;
    return *this;
}

MessageId::MessageId(int32_t partition, int64_t ledgerId, int64_t entryId, int32_t batchIndex)
    : impl_(std::make_shared<MessageIdImpl>(partition, ledgerId, entryId, batchIndex)) {}

MessageId::MessageId(const MessageId& firstMessageId, const MessageId& lastMessageId) {
    auto firstImpl = firstMessageId.impl_;
    auto lastImpl = lastMessageId.impl_;
    impl_ = std::make_shared<ChunkMessageIdImpl>(
                firstImpl->partition_, firstImpl->ledgerId_, firstImpl->entryId_, firstImpl->batchIndex_,
                lastImpl->partition_, lastImpl->ledgerId_, lastImpl->entryId_, lastImpl->batchIndex_)
                ->getLastChunkMessageIdImpl();
}

const MessageId& MessageId::earliest() {
    static const MessageId _earliest(-1, -1, -1, -1);
    return _earliest;
}

const MessageId& MessageId::latest() {
    static const int64_t long_max = std::numeric_limits<int64_t>::max();
    static const MessageId _latest(-1, long_max, long_max, -1);
    return _latest;
}

bool MessageId::isChunkMessageid() const {
    return std::dynamic_pointer_cast<ChunkMessageIdImpl>(impl_) != nullptr;
}

void MessageId::serialize(std::string& result) const {
    proto::MessageIdData idData;
    writeMessageIdData(impl_, &idData);
    if (isChunkMessageid()) {
        auto chunkMsgIdImpl = std::dynamic_pointer_cast<ChunkMessageIdImpl>(impl_);
        writeMessageIdData(chunkMsgIdImpl->getFirstChunkMessageIdImpl(),
                           idData.mutable_first_chunk_message_id());
    }
    idData.SerializeToString(&result);
}

/**
 * Deserialize a message id from a binary string
 */
MessageId MessageId::deserialize(const std::string& serializedMessageId) {
    proto::MessageIdData idData;
    if (!idData.ParseFromString(serializedMessageId)) {
        throw std::invalid_argument("Failed to parse serialized message id");
    }
    if (idData.has_first_chunk_message_id()) {
        auto firData = idData.first_chunk_message_id();
        return MessageId(
            MessageId(firData.partition(), firData.ledgerid(), firData.entryid(), firData.batch_index()),
            MessageId(idData.partition(), idData.ledgerid(), idData.entryid(), idData.batch_index()));
    }
    return MessageId(idData.partition(), idData.ledgerid(), idData.entryid(), idData.batch_index());
}

int64_t MessageId::ledgerId() const { return impl_->ledgerId_; }

int64_t MessageId::entryId() const { return impl_->entryId_; }

int32_t MessageId::batchIndex() const { return impl_->batchIndex_; }

int32_t MessageId::partition() const { return impl_->partition_; }

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const pulsar::MessageId& messageId) {
    std::function<void(std::ostream&, const MessageIdImplPtr)> printMsgIdImpl =
        [](std::ostream& s, const MessageIdImplPtr impl) {
            s << '(' << impl->ledgerId_ << ',' << impl->entryId_ << ',' << impl->partition_ << ','
              << impl->batchIndex_ << ')';
        };
    if (messageId.isChunkMessageid() == false) {
        printMsgIdImpl(s, messageId.impl_);
    } else {
        auto firstMessageidImplPtr =
            std::dynamic_pointer_cast<ChunkMessageIdImpl>(messageId.impl_)->getFirstChunkMessageIdImpl();
        printMsgIdImpl(s, firstMessageidImplPtr);
        s << "->";
        printMsgIdImpl(s, messageId.impl_);
    }
    return s;
}

PULSAR_PUBLIC bool MessageId::operator<(const MessageId& other) const {
    if (impl_->ledgerId_ < other.impl_->ledgerId_) {
        return true;
    } else if (impl_->ledgerId_ > other.impl_->ledgerId_) {
        return false;
    }

    if (impl_->entryId_ < other.impl_->entryId_) {
        return true;
    } else if (impl_->entryId_ > other.impl_->entryId_) {
        return false;
    }

    if (impl_->batchIndex_ < other.impl_->batchIndex_) {
        return true;
    } else {
        return false;
    }
}

PULSAR_PUBLIC bool MessageId::operator<=(const MessageId& other) const {
    return *this < other || *this == other;
}

PULSAR_PUBLIC bool MessageId::operator>(const MessageId& other) const { return !(*this <= other); }

PULSAR_PUBLIC bool MessageId::operator>=(const MessageId& other) const { return !(*this < other); }

PULSAR_PUBLIC bool MessageId::operator==(const MessageId& other) const {
    return impl_->ledgerId_ == other.impl_->ledgerId_ && impl_->entryId_ == other.impl_->entryId_ &&
           impl_->batchIndex_ == other.impl_->batchIndex_ && impl_->partition_ == other.impl_->partition_;
}

PULSAR_PUBLIC bool MessageId::operator!=(const MessageId& other) const { return !(*this == other); }

PULSAR_PUBLIC const std::string& MessageId::getTopicName() const { return impl_->getTopicName(); }

PULSAR_PUBLIC void MessageId::setTopicName(const std::string& topicName) {
    return impl_->setTopicName(topicName);
}

}  // namespace pulsar
