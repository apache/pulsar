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

#ifndef MESSAGE_ID_H
#define MESSAGE_ID_H

#include <iosfwd>
#include <stdint.h>

#pragma GCC visibility push(default)

namespace pulsar {

class ConsumerImpl;
class UnAckedMessageTrackerEnabled;

class MessageId {
 public:
    MessageId& operator=(const MessageId&);
    MessageId();
    // These functions compare the message order as stored in bookkeeper
    inline bool operator<(const MessageId& mID) const;
    inline bool operator==(const MessageId& mID) const;
 protected:
    friend class ConsumerImpl;
    friend class Message;
    friend class MessageImpl;
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class UnAckedMessageTrackerEnabled;
    friend class BatchAcknowledgementTracker;
    MessageId(int64_t, int64_t);
    friend std::ostream& operator<<(std::ostream& s, const MessageId& messageId);
    int64_t ledgerId_;
    int64_t entryId_ :48;
    short partition_ :16;
};

bool MessageId::operator<(const MessageId& mID) const {
    return (ledgerId_ < mID.ledgerId_) || (ledgerId_ == mID.ledgerId_ && entryId_ < mID.entryId_);
}

bool MessageId::operator==(const MessageId& mID) const {
    return (ledgerId_ == mID.ledgerId_ && entryId_ == mID.entryId_);
}

}

#pragma GCC visibility pop

#endif //MESSAGE_ID_H
