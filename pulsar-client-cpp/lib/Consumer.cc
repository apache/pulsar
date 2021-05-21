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
#include <pulsar/Consumer.h>
#include <pulsar/MessageBuilder.h>
#include "ConsumerImpl.h"
#include "Utils.h"
#include <lib/BrokerConsumerStatsImpl.h>
#include <lib/Latch.h>

namespace pulsar {

static const std::string EMPTY_STRING;

Consumer::Consumer() : impl_() {}

Consumer::Consumer(ConsumerImplBasePtr impl) : impl_(impl) {}

const std::string& Consumer::getTopic() const { return impl_ != NULL ? impl_->getTopic() : EMPTY_STRING; }

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

void Consumer::receiveAsync(ReceiveCallback callback) {
    if (!impl_) {
        Message msg;
        callback(ResultConsumerNotInitialized, msg);
        return;
    }
    impl_->receiveAsync(callback);
}

Result Consumer::acknowledge(const Message& message) { return acknowledge(message.getMessageId()); }

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

void Consumer::negativeAcknowledge(const Message& message) { negativeAcknowledge(message.getMessageId()); }

void Consumer::negativeAcknowledge(const MessageId& messageId) {
    if (impl_) {
        impl_->negativeAcknowledge(messageId);
        ;
    }
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

Result Consumer::getBrokerConsumerStats(BrokerConsumerStats& brokerConsumerStats) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<Result, BrokerConsumerStats> promise;
    getBrokerConsumerStatsAsync(WaitForCallbackValue<BrokerConsumerStats>(promise));
    return promise.getFuture().get(brokerConsumerStats);
}

void Consumer::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }
    impl_->getBrokerConsumerStatsAsync(callback);
}

void Consumer::seekAsync(const MessageId& msgId, ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }
    impl_->seekAsync(msgId, callback);
}

void Consumer::seekAsync(uint64_t timestamp, ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }
    impl_->seekAsync(timestamp, callback);
}

Result Consumer::seek(const MessageId& msgId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->seekAsync(msgId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

Result Consumer::seek(uint64_t timestamp) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->seekAsync(timestamp, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

bool Consumer::isConnected() const { return impl_ && impl_->isConnected(); }

}  // namespace pulsar
