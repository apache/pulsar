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
#pragma once

#include <pulsar/MessageId.h>
#include <iostream>

namespace pulsar {

class GetLastMessageIdResponse {
    friend std::ostream& operator<<(std::ostream& os, const GetLastMessageIdResponse& response) {
        os << "lastMessageId: " << response.lastMessageId_;
        if (response.hasMarkDeletePosition_) {
            os << ", markDeletePosition: " << response.markDeletePosition_;
        }
        return os;
    }

   public:
    GetLastMessageIdResponse() = default;

    GetLastMessageIdResponse(const MessageId& lastMessageId)
        : lastMessageId_(lastMessageId), hasMarkDeletePosition_{false} {}

    GetLastMessageIdResponse(const MessageId& lastMessageId, const MessageId& markDeletePosition)
        : lastMessageId_(lastMessageId),
          markDeletePosition_(markDeletePosition),
          hasMarkDeletePosition_(true) {}

    const MessageId& getLastMessageId() const noexcept { return lastMessageId_; }
    const MessageId& getMarkDeletePosition() const noexcept { return markDeletePosition_; }
    bool hasMarkDeletePosition() const noexcept { return hasMarkDeletePosition_; }

   private:
    MessageId lastMessageId_;
    MessageId markDeletePosition_;
    bool hasMarkDeletePosition_;
};

}  // namespace pulsar
