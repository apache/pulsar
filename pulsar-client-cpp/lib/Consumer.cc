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

#include <pulsar/Consumer.h>
#include <pulsar/MessageBuilder.h>
#include "ConsumerImpl.h"
#include "Utils.h"

namespace pulsar {

const std::string EMPTY_STRING;

struct ConsumerConfiguration::Impl {
    long unAckedMessagesTimeoutMs;
    ConsumerType consumerType;
    MessageListener messageListener;
    bool hasMessageListener;
    int receiverQueueSize;
    std::string consumerName;
    Impl()
            : unAckedMessagesTimeoutMs(0),
              consumerType(ConsumerExclusive),
              messageListener(),
              hasMessageListener(false),
              receiverQueueSize(1000) {
    }
};

ConsumerConfiguration::ConsumerConfiguration()
        : impl_(boost::make_shared<Impl>()) {
}

ConsumerConfiguration::~ConsumerConfiguration() {
}

ConsumerConfiguration::ConsumerConfiguration(const ConsumerConfiguration& x)
    : impl_(x.impl_) {
}

ConsumerConfiguration& ConsumerConfiguration::operator=(const ConsumerConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ConsumerConfiguration& ConsumerConfiguration::setConsumerType(ConsumerType consumerType) {
    impl_->consumerType = consumerType;
    return *this;
}

ConsumerType ConsumerConfiguration::getConsumerType() const {
    return impl_->consumerType;
}

ConsumerConfiguration& ConsumerConfiguration::setMessageListener(MessageListener messageListener) {
    impl_->messageListener = messageListener;
    impl_->hasMessageListener = true;
    return *this;
}

MessageListener ConsumerConfiguration::getMessageListener() const {
    return impl_->messageListener;
}

bool ConsumerConfiguration::hasMessageListener() const {
    return impl_->hasMessageListener;
}

void ConsumerConfiguration::setReceiverQueueSize(int size) {
    impl_->receiverQueueSize = size;
}

int ConsumerConfiguration::getReceiverQueueSize() const {
    return impl_->receiverQueueSize;
}

const std::string& ConsumerConfiguration::getConsumerName() const {
    return impl_->consumerName;
}

void ConsumerConfiguration::setConsumerName(const std::string& consumerName) {
    impl_->consumerName = consumerName;
}

long ConsumerConfiguration::getUnAckedMessagesTimeoutMs() const {
    return impl_->unAckedMessagesTimeoutMs;
}

void ConsumerConfiguration::setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds) {
    if (milliSeconds < 10000) {
        throw "Consumer Config Exception: Unacknowledged message timeout should be greater than 10 seconds.";
    }
    impl_->unAckedMessagesTimeoutMs = milliSeconds;
}
//////////////////////////////////////////////////////

Consumer::Consumer()
        : impl_() {
}

Consumer::Consumer(ConsumerImplBasePtr impl)
        : impl_(impl) {
}

const std::string& Consumer::getTopic() const {
    return impl_ != NULL ? impl_->getTopic() : EMPTY_STRING;
}

const std::string& Consumer::getSubscriptionName() const {
    return impl_ != NULL ? impl_->getSubscriptionName() : EMPTY_STRING;
}

Result Consumer::unsubscribe() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->unsubscribeAsync(WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::unsubscribeAsync(ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->unsubscribeAsync(callback);
}

Result Consumer::receive(Message& msg) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->receive(msg);
}

Result Consumer::receive(Message& msg, int timeoutMs) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->receive(msg, timeoutMs);
}

Result Consumer::acknowledge(const Message& message) {
    return acknowledge(message.getMessageId());
}

Result Consumer::acknowledge(const MessageId& messageId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->acknowledgeAsync(messageId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::acknowledgeAsync(const Message& message, ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeAsync(message.getMessageId(), callback);
}

void Consumer::acknowledgeAsync(const MessageId& messageId, ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeAsync(messageId, callback);
}

Result Consumer::acknowledgeCumulative(const Message& message) {
    return acknowledgeCumulative(message.getMessageId());
}

Result Consumer::acknowledgeCumulative(const MessageId& messageId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->acknowledgeCumulativeAsync(messageId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::acknowledgeCumulativeAsync(const Message& message, ResultCallback callback) {
    acknowledgeCumulativeAsync(message.getMessageId(), callback);
}

void Consumer::acknowledgeCumulativeAsync(const MessageId& messageId, ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeCumulativeAsync(messageId, callback);
}

Result Consumer::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::closeAsync(ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->closeAsync(callback);
}

Result Consumer::pauseMessageListener() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->pauseMessageListener();
}

Result Consumer::resumeMessageListener() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->resumeMessageListener();
}

void Consumer::redeliverUnacknowledgedMessages() {
    if (impl_) {
        impl_->redeliverUnacknowledgedMessages();
    }
}
}
