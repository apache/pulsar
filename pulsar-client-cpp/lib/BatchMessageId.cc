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

#include <pulsar/BatchMessageId.h>

#include "PulsarApi.pb.h"

#include <tuple>
#include <iostream>

namespace pulsar {

BatchMessageId::BatchMessageId(const MessageId& msgId) :
        MessageId(msgId.ledgerId_, msgId.entryId_), batchIndex_(msgId.getBatchIndex()) {
}

void BatchMessageId::serialize(std::string& result) const {
    proto::MessageIdData idData;
    idData.set_ledgerid(ledgerId_);
    idData.set_entryid(entryId_);
    idData.set_batch_index(batchIndex_);

    if (partition_ != -1) {
        idData.set_partition(partition_);
    }

    idData.SerializeToString(&result);
}

int64_t BatchMessageId::getBatchIndex() const {
    return batchIndex_;
}

#pragma GCC visibility push(default)

bool BatchMessageId::operator<(const BatchMessageId& other) const {
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

bool BatchMessageId::operator<=(const BatchMessageId& other) const {
    return *this < other || *this == other;
}

bool BatchMessageId::operator==(const BatchMessageId& other) const {
    return ledgerId_ == other.ledgerId_ && entryId_ == other.entryId_
            && batchIndex_ == other.batchIndex_;
}

std::ostream& operator<<(std::ostream& s, const BatchMessageId& messageId) {
    s << '(' << messageId.ledgerId_ << ':' << messageId.entryId_ << ':' << messageId.batchIndex_
      << ':' << messageId.partition_ << ')';
    return s;
}

#pragma GCC visibility pop

}
