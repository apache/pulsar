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

#ifndef LIB_MESSAGE_BATCH_H
#define LIB_MESSAGE_BATCH_H
#include <vector>

#include <pulsar/defines.h>
#include <pulsar/Message.h>

namespace pulsar {

class PULSAR_PUBLIC MessageBatch {
   public:
    MessageBatch();

    MessageBatch& withMessageId(const MessageId& messageId);

    MessageBatch& parseFrom(const std::string& payload, uint32_t batchSize);

    MessageBatch& parseFrom(const SharedBuffer& payload, uint32_t batchSize);

    const std::vector<Message>& messages();

   private:
    typedef std::shared_ptr<MessageImpl> MessageImplPtr;
    MessageImplPtr impl_;
    Message batchMessage_;

    std::vector<Message> batch_;
};
}  // namespace pulsar
#endif  // LIB_MESSAGE_BATCH_H
