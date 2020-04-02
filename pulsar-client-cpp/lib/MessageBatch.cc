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
#include <pulsar/MessageBatch.h>

#include "Commands.h"
#include "MessageImpl.h"
#include "SharedBuffer.h"

namespace pulsar {

const static std::string emptyString;

MessageBatch::MessageBatch() : impl_(std::make_shared<MessageImpl>()), batchMessage_(impl_) {
    impl_->setTopicName(emptyString);
}

MessageBatch& MessageBatch::withMessageId(const MessageId& messageId) {
    impl_->messageId = messageId;
    return *this;
}

MessageBatch& MessageBatch::parseFrom(const std::string& payload, uint32_t batchSize) {
    const SharedBuffer& payloadBuffer =
        SharedBuffer::copy((char*)payload.data(), static_cast<uint32_t>(payload.size()));
    return parseFrom(payloadBuffer, batchSize);
}

MessageBatch& MessageBatch::parseFrom(const SharedBuffer& payload, uint32_t batchSize) {
    impl_->payload = payload;
    impl_->metadata.set_num_messages_in_batch(batchSize);
    batch_.clear();

    for (int i = 0; i < batchSize; ++i) {
        batch_.push_back(Commands::deSerializeSingleMessageInBatch(batchMessage_, i));
    }
    return *this;
}

const std::vector<Message>& MessageBatch::messages() { return batch_; }

}  // namespace pulsar
