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

#include <stdexcept>

namespace pulsar {

const static std::string emptyString;

ConsumerConfiguration::ConsumerConfiguration() : impl_(std::make_shared<ConsumerConfigurationImpl>()) {}

ConsumerConfiguration::~ConsumerConfiguration() {}

ConsumerConfiguration::ConsumerConfiguration(const ConsumerConfiguration& x) : impl_(x.impl_) {}

ConsumerConfiguration& ConsumerConfiguration::operator=(const ConsumerConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ConsumerConfiguration ConsumerConfiguration::clone() const {
    ConsumerConfiguration newConf;
    newConf.impl_.reset(new ConsumerConfigurationImpl(*this->impl_));
    return newConf;
}

ConsumerConfiguration& ConsumerConfiguration::setSchema(const SchemaInfo& schemaInfo) {
    impl_->schemaInfo = schemaInfo;
    return *this;
}

const SchemaInfo& ConsumerConfiguration::getSchema() const { return impl_->schemaInfo; }

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

ConsumerType ConsumerConfiguration::getConsumerType() const { return impl_->consumerType; }

ConsumerConfiguration& ConsumerConfiguration::setMessageListener(MessageListener messageListener) {
    impl_->messageListener = messageListener;
    impl_->hasMessageListener = true;
    return *this;
}

MessageListener ConsumerConfiguration::getMessageListener() const { return impl_->messageListener; }

bool ConsumerConfiguration::hasMessageListener() const { return impl_->hasMessageListener; }

ConsumerConfiguration& ConsumerConfiguration::setConsumerEventListener(
    ConsumerEventListenerPtr eventListener) {
    impl_->eventListener = eventListener;
    impl_->hasConsumerEventListener = true;
    return *this;
}

ConsumerEventListenerPtr ConsumerConfiguration::getConsumerEventListener() const {
    return impl_->eventListener;
}

bool ConsumerConfiguration::hasConsumerEventListener() const { return impl_->hasConsumerEventListener; }

void ConsumerConfiguration::setReceiverQueueSize(int size) { impl_->receiverQueueSize = size; }

int ConsumerConfiguration::getReceiverQueueSize() const { return impl_->receiverQueueSize; }

void ConsumerConfiguration::setMaxTotalReceiverQueueSizeAcrossPartitions(int size) {
    impl_->maxTotalReceiverQueueSizeAcrossPartitions = size;
}

int ConsumerConfiguration::getMaxTotalReceiverQueueSizeAcrossPartitions() const {
    return impl_->maxTotalReceiverQueueSizeAcrossPartitions;
}

const std::string& ConsumerConfiguration::getConsumerName() const { return impl_->consumerName; }

void ConsumerConfiguration::setConsumerName(const std::string& consumerName) {
    impl_->consumerName = consumerName;
}

long ConsumerConfiguration::getUnAckedMessagesTimeoutMs() const { return impl_->unAckedMessagesTimeoutMs; }

void ConsumerConfiguration::setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds) {
    if (milliSeconds < 10000 && milliSeconds != 0) {
        throw std::invalid_argument(
            "Consumer Config Exception: Unacknowledged message timeout should be greater than 10 seconds.");
    }
    impl_->unAckedMessagesTimeoutMs = milliSeconds;
}

long ConsumerConfiguration::getTickDurationInMs() const { return impl_->tickDurationInMs; }

void ConsumerConfiguration::setTickDurationInMs(const uint64_t milliSeconds) {
    impl_->tickDurationInMs = milliSeconds;
}

void ConsumerConfiguration::setNegativeAckRedeliveryDelayMs(long redeliveryDelayMillis) {
    impl_->negativeAckRedeliveryDelayMs = redeliveryDelayMillis;
}

long ConsumerConfiguration::getNegativeAckRedeliveryDelayMs() const {
    return impl_->negativeAckRedeliveryDelayMs;
}

void ConsumerConfiguration::setAckGroupingTimeMs(long ackGroupingMillis) {
    impl_->ackGroupingTimeMs = ackGroupingMillis;
}

long ConsumerConfiguration::getAckGroupingTimeMs() const { return impl_->ackGroupingTimeMs; }

void ConsumerConfiguration::setAckGroupingMaxSize(long maxGroupingSize) {
    impl_->ackGroupingMaxSize = maxGroupingSize;
}

long ConsumerConfiguration::getAckGroupingMaxSize() const { return impl_->ackGroupingMaxSize; }

bool ConsumerConfiguration::isEncryptionEnabled() const { return (impl_->cryptoKeyReader != NULL); }

const CryptoKeyReaderPtr ConsumerConfiguration::getCryptoKeyReader() const { return impl_->cryptoKeyReader; }

ConsumerConfiguration& ConsumerConfiguration::setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader) {
    impl_->cryptoKeyReader = cryptoKeyReader;
    return *this;
}

ConsumerCryptoFailureAction ConsumerConfiguration::getCryptoFailureAction() const {
    return impl_->cryptoFailureAction;
}

ConsumerConfiguration& ConsumerConfiguration::setCryptoFailureAction(ConsumerCryptoFailureAction action) {
    impl_->cryptoFailureAction = action;
    return *this;
}

bool ConsumerConfiguration::isReadCompacted() const { return impl_->readCompacted; }

void ConsumerConfiguration::setReadCompacted(bool compacted) { impl_->readCompacted = compacted; }

void ConsumerConfiguration::setSubscriptionInitialPosition(InitialPosition subscriptionInitialPosition) {
    impl_->subscriptionInitialPosition = subscriptionInitialPosition;
}

InitialPosition ConsumerConfiguration::getSubscriptionInitialPosition() const {
    return impl_->subscriptionInitialPosition;
}

void ConsumerConfiguration::setPatternAutoDiscoveryPeriod(int periodInSeconds) {
    impl_->patternAutoDiscoveryPeriod = periodInSeconds;
}

int ConsumerConfiguration::getPatternAutoDiscoveryPeriod() const { return impl_->patternAutoDiscoveryPeriod; }

void ConsumerConfiguration::setReplicateSubscriptionStateEnabled(bool enabled) {
    impl_->replicateSubscriptionStateEnabled = enabled;
}

bool ConsumerConfiguration::isReplicateSubscriptionStateEnabled() const {
    return impl_->replicateSubscriptionStateEnabled;
}

bool ConsumerConfiguration::hasProperty(const std::string& name) const {
    const std::map<std::string, std::string>& m = impl_->properties;
    return m.find(name) != m.end();
}

const std::string& ConsumerConfiguration::getProperty(const std::string& name) const {
    if (hasProperty(name)) {
        const std::map<std::string, std::string>& m = impl_->properties;
        return m.at(name);
    } else {
        return emptyString;
    }
}

std::map<std::string, std::string>& ConsumerConfiguration::getProperties() const { return impl_->properties; }

ConsumerConfiguration& ConsumerConfiguration::setProperty(const std::string& name, const std::string& value) {
    impl_->properties.insert(std::make_pair(name, value));
    return *this;
}

ConsumerConfiguration& ConsumerConfiguration::setProperties(
    const std::map<std::string, std::string>& properties) {
    for (std::map<std::string, std::string>::const_iterator it = properties.begin(); it != properties.end();
         it++) {
        setProperty(it->first, it->second);
    }
    return *this;
}

ConsumerConfiguration& ConsumerConfiguration::setPriorityLevel(int priorityLevel) {
    if (priorityLevel < 0) {
        throw std::invalid_argument("Consumer Config Exception: PriorityLevel should be nonnegative number.");
    }
    impl_->priorityLevel = priorityLevel;
    return *this;
}

int ConsumerConfiguration::getPriorityLevel() const { return impl_->priorityLevel; }

ConsumerConfiguration& ConsumerConfiguration::setKeySharedPolicy(KeySharedPolicy keySharedPolicy) {
    impl_->keySharedPolicy = keySharedPolicy.clone();
    return *this;
}

KeySharedPolicy ConsumerConfiguration::getKeySharedPolicy() const { return impl_->keySharedPolicy; }

}  // namespace pulsar
