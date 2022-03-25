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
#include "PulsarApi.pb.h"

namespace pulsar {

inline MessageId toMessageId(const proto::MessageIdData& messageIdData) {
    return MessageId{messageIdData.partition(), static_cast<int64_t>(messageIdData.ledgerid()),
                     static_cast<int64_t>(messageIdData.entryid()), messageIdData.batch_index()};
}

namespace internal {
template <typename T>
static int compare(T lhs, T rhs) {
    return (lhs < rhs) ? -1 : ((lhs == rhs) ? 0 : 1);
}
}  // namespace internal

inline int compareLedgerAndEntryId(const MessageId& lhs, const MessageId& rhs) {
    auto result = internal::compare(lhs.ledgerId(), rhs.ledgerId());
    if (result != 0) {
        return result;
    }
    return internal::compare(lhs.entryId(), rhs.entryId());
}

}  // namespace pulsar
