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

#ifndef LIB_BATCHMESSAGEID_H_
#define LIB_BATCHMESSAGEID_H_

#include <pulsar/MessageId.h>

namespace pulsar {
class BatchMessageId : public MessageId {
 public:
    BatchMessageId(int64_t ledgerId, int64_t entryId, int batchIndex = -1)
            : MessageId(ledgerId, entryId),
              batchIndex_(batchIndex) {
    }

    BatchMessageId()
            : batchIndex_(-1) {
    }
    // These functions compare the message order as stored in bookkeeper
    inline bool operator<(const BatchMessageId& mID) const;
    inline bool operator<=(const BatchMessageId& mID) const;
 protected:
    friend class ConsumerImpl;
    friend class Message;
    friend class MessageImpl;
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class BatchAcknowledgementTracker;
    friend class PulsarFriend;
    int64_t batchIndex_;
};

bool BatchMessageId::operator<(const BatchMessageId& mID) const {
    return (ledgerId_ < mID.ledgerId_) || (ledgerId_ == mID.ledgerId_ && entryId_ < mID.entryId_);
}

bool BatchMessageId::operator<=(const BatchMessageId& mID) const {
    return (ledgerId_ < mID.ledgerId_) || (ledgerId_ == mID.ledgerId_ && entryId_ <= mID.entryId_);
}

}
#endif /* LIB_BATCHMESSAGEID_H_ */
