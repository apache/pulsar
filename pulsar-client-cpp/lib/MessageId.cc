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

#include <pulsar/MessageId.h>
#include <pulsar/BatchMessageId.h>

#include "PulsarApi.pb.h"

#include <iostream>
#include <limits>
#include <tuple>
#include <math.h>
#include <boost/make_shared.hpp>

namespace pulsar {


MessageId::MessageId()
        : ledgerId_(-1),
          entryId_(-1),
          partition_(-1) {
}

MessageId& MessageId::operator=(const MessageId& m) {
    entryId_ = m.entryId_;
    ledgerId_ = m.ledgerId_;
    partition_ = m.partition_;
    return *this;
}

MessageId::MessageId(int64_t ledgerId, int64_t entryId)
        : ledgerId_(ledgerId),
          entryId_(entryId),
          partition_(-1) {
     // partition is set explicitly in consumerImpl when message is received
     // consumer's partition is assigned to this partition
}

int64_t MessageId::getBatchIndex() const {
    // It's only relevant for batch message ids
    return -1;
}

const MessageId& MessageId::earliest() {
    static const BatchMessageId _earliest(-1, -1);
    return _earliest;
}

const MessageId& MessageId::latest() {
    // For entry-id we only have 48bits
    static const BatchMessageId _latest(std::numeric_limits<int64_t>::max(),
                                        (int64_t)(pow(2, 47) - 1));
    return _latest;
}

void MessageId::serialize(std::string& result) const {
    proto::MessageIdData idData;
    idData.set_ledgerid(ledgerId_);
    idData.set_entryid(entryId_);
    if (partition_ != -1) {
        idData.set_partition(partition_);
    }

    idData.SerializeToString(&result);
}

/**
 * Deserialize a message id from a binary string
 */
boost::shared_ptr<MessageId> MessageId::deserialize(const std::string& serializedMessageId) {
    proto::MessageIdData idData;
    if (!idData.ParseFromString(serializedMessageId)) {
        throw "Failed to parse serialized message id";
    }

    return boost::make_shared<BatchMessageId>(idData.ledgerid(), idData.entryid(),
                                              idData.batch_index());
}


#pragma GCC visibility push(default)
std::ostream& operator<<(std::ostream& s, const pulsar::MessageId& messageId) {
    s << '(' << messageId.ledgerId_ << ',' << messageId.entryId_ << ',' << messageId.partition_ << ')';
    return s;
}

bool MessageId::operator<(const MessageId& other) const {
    if (ledgerId_ < other.ledgerId_) {
        return true;
    } else if (ledgerId_ > other.ledgerId_) {
        return false;
    }

    if (entryId_ < other.entryId_) {
        return true;
    } else {
        return false;
    }
}

bool MessageId::operator==(const MessageId& other) const {
    return ledgerId_ == other.ledgerId_ && entryId_ == other.entryId_;
}

#pragma GCC visibility pop

}
