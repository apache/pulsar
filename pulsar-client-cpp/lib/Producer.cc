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
#include <pulsar/Producer.h>
#include "SharedBuffer.h"
#include <pulsar/MessageBuilder.h>

#include "Utils.h"
#include "ProducerImpl.h"

namespace pulsar {

static const std::string EMPTY_STRING;

Producer::Producer() : impl_() {}

Producer::Producer(ProducerImplBasePtr impl) : impl_(impl) {}

const std::string& Producer::getTopic() const { return impl_ != NULL ? impl_->getTopic() : EMPTY_STRING; }

Result Producer::send(const Message& msg) {
    Promise<Result, MessageId> promise;
    sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    if (!promise.isComplete()) {
        impl_->triggerFlush();
    }

    MessageId mi;
    Result result = promise.getFuture().get(mi);
    msg.setMessageId(mi);

    return result;
}

Result Producer::send(const Message& msg, MessageId& messageId) {
    Promise<Result, MessageId> promise;
    sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    if (!promise.isComplete()) {
        impl_->triggerFlush();
    }

    return promise.getFuture().get(messageId);
}

void Producer::sendAsync(const Message& msg, SendCallback callback) {
    if (!impl_) {
        callback(ResultProducerNotInitialized, msg.getMessageId());
        return;
    }

    impl_->sendAsync(msg, callback);
}

const std::string& Producer::getProducerName() const { return impl_->getProducerName(); }

int64_t Producer::getLastSequenceId() const { return impl_->getLastSequenceId(); }

const std::string& Producer::getSchemaVersion() const { return impl_->getSchemaVersion(); }

Result Producer::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Producer::closeAsync(CloseCallback callback) {
    if (!impl_) {
        callback(ResultProducerNotInitialized);
        return;
    }

    impl_->closeAsync(callback);
}

Result Producer::flush() {
    Promise<bool, Result> promise;
    flushAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Producer::flushAsync(FlushCallback callback) {
    if (!impl_) {
        callback(ResultProducerNotInitialized);
        return;
    }

    impl_->flushAsync(callback);
}

void Producer::producerFailMessages(Result result) {
    if (impl_) {
        ProducerImpl* producerImpl = static_cast<ProducerImpl*>(impl_.get());
        producerImpl->failPendingMessages(result, true);
    }
}

bool Producer::isConnected() const { return impl_ && impl_->isConnected(); }

}  // namespace pulsar
