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

#include <pulsar/MessageId.h>

#include <iostream>

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


#pragma GCC visibility push(default)
std::ostream& operator<<(std::ostream& s, const pulsar::MessageId& messageId) {
    s << '(' << messageId.ledgerId_ << ',' << messageId.entryId_ << ',' << messageId.partition_ << ')';
    return s;
}
#pragma GCC visibility pop

}
