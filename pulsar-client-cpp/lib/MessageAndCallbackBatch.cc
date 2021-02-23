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
#include "MessageAndCallbackBatch.h"
#include "ClientConnection.h"
#include "Commands.h"
#include "LogUtils.h"
#include "MessageImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

void MessageAndCallbackBatch::add(const Message& msg, const SendCallback& callback) {
    if (empty()) {
        msgImpl_.reset(new MessageImpl);
        Commands::initBatchMessageMetadata(msg, msgImpl_->metadata);
    }
    LOG_DEBUG(" Before serialization payload size in bytes = " << msgImpl_->payload.readableBytes());
    sequenceId_ = Commands::serializeSingleMessageInBatchWithPayload(msg, msgImpl_->payload,
                                                                     ClientConnection::getMaxMessageSize());
    LOG_DEBUG(" After serialization payload size in bytes = " << msgImpl_->payload.readableBytes());
    callbacks_.emplace_back(callback);

    ++messagesCount_;
    messagesSize_ += msg.getLength();
}

void MessageAndCallbackBatch::clear() {
    msgImpl_.reset();
    callbacks_.clear();
    messagesCount_ = 0;
    messagesSize_ = 0;
}

static void completeSendCallbacks(const std::vector<SendCallback>& callbacks, Result result,
                                  const MessageId& id) {
    int32_t numOfMessages = static_cast<int32_t>(callbacks.size());
    LOG_DEBUG("Batch complete [Result = " << result << "] [numOfMessages = " << numOfMessages << "]");
    for (int32_t i = 0; i < numOfMessages; i++) {
        MessageId idInBatch(id.partition(), id.ledgerId(), id.entryId(), i);
        callbacks[i](result, idInBatch);
    }
}

void MessageAndCallbackBatch::complete(Result result, const MessageId& id) const {
    completeSendCallbacks(callbacks_, result, id);
}

SendCallback MessageAndCallbackBatch::createSendCallback() const {
    const auto& callbacks = callbacks_;
    return [callbacks]  // save a copy of `callbacks_`
        (Result result, const MessageId& id) { completeSendCallbacks(callbacks, result, id); };
}

}  // namespace pulsar
