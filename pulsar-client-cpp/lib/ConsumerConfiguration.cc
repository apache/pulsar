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
#include <lib/ConsumerConfigurationImpl.h>

namespace pulsar {

ConsumerConfiguration::ConsumerConfiguration()
        : impl_(boost::make_shared<ConsumerConfigurationImpl>()) {
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

long ConsumerConfiguration::getBrokerConsumerStatsCacheTimeInMs() const {
    return impl_->brokerConsumerStatsCacheTimeInMs;
}

void ConsumerConfiguration::setBrokerConsumerStatsCacheTimeInMs(const long cacheTimeInMs) {
    impl_->brokerConsumerStatsCacheTimeInMs = cacheTimeInMs;
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
}
