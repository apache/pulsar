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
#include <lib/ReaderConfigurationImpl.h>

namespace pulsar {

const static std::string emptyString;

ReaderConfiguration::ReaderConfiguration() : impl_(std::make_shared<ReaderConfigurationImpl>()) {}

ReaderConfiguration::~ReaderConfiguration() {}

ReaderConfiguration::ReaderConfiguration(const ReaderConfiguration& x) : impl_(x.impl_) {}

ReaderConfiguration& ReaderConfiguration::operator=(const ReaderConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ReaderConfiguration& ReaderConfiguration::setSchema(const SchemaInfo& schemaInfo) {
    impl_->schemaInfo = schemaInfo;
    return *this;
}

const SchemaInfo& ReaderConfiguration::getSchema() const { return impl_->schemaInfo; }

ReaderConfiguration& ReaderConfiguration::setReaderListener(ReaderListener readerListener) {
    impl_->readerListener = readerListener;
    impl_->hasReaderListener = true;
    return *this;
}

ReaderListener ReaderConfiguration::getReaderListener() const { return impl_->readerListener; }

bool ReaderConfiguration::hasReaderListener() const { return impl_->hasReaderListener; }

void ReaderConfiguration::setReceiverQueueSize(int size) { impl_->receiverQueueSize = size; }

int ReaderConfiguration::getReceiverQueueSize() const { return impl_->receiverQueueSize; }

const std::string& ReaderConfiguration::getReaderName() const { return impl_->readerName; }

void ReaderConfiguration::setReaderName(const std::string& readerName) { impl_->readerName = readerName; }

const std::string& ReaderConfiguration::getSubscriptionRolePrefix() const {
    return impl_->subscriptionRolePrefix;
}

void ReaderConfiguration::setSubscriptionRolePrefix(const std::string& subscriptionRolePrefix) {
    impl_->subscriptionRolePrefix = subscriptionRolePrefix;
}

bool ReaderConfiguration::isReadCompacted() const { return impl_->readCompacted; }

void ReaderConfiguration::setReadCompacted(bool compacted) { impl_->readCompacted = compacted; }

void ReaderConfiguration::setInternalSubscriptionName(std::string internalSubscriptionName) {
    impl_->internalSubscriptionName = internalSubscriptionName;
}

const std::string& ReaderConfiguration::getInternalSubscriptionName() const {
    return impl_->internalSubscriptionName;
}

void ReaderConfiguration::setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds) {
    impl_->unAckedMessagesTimeoutMs = milliSeconds;
}

long ReaderConfiguration::getUnAckedMessagesTimeoutMs() const { return impl_->unAckedMessagesTimeoutMs; }

void ReaderConfiguration::setTickDurationInMs(const uint64_t milliSeconds) {
    impl_->tickDurationInMs = milliSeconds;
}

long ReaderConfiguration::getTickDurationInMs() const { return impl_->tickDurationInMs; }

void ReaderConfiguration::setAckGroupingTimeMs(long ackGroupingMillis) {
    impl_->ackGroupingTimeMs = ackGroupingMillis;
}

long ReaderConfiguration::getAckGroupingTimeMs() const { return impl_->ackGroupingTimeMs; }

void ReaderConfiguration::setAckGroupingMaxSize(long maxGroupingSize) {
    impl_->ackGroupingMaxSize = maxGroupingSize;
}

long ReaderConfiguration::getAckGroupingMaxSize() const { return impl_->ackGroupingMaxSize; }

bool ReaderConfiguration::isEncryptionEnabled() const { return impl_->cryptoKeyReader != nullptr; }

const CryptoKeyReaderPtr ReaderConfiguration::getCryptoKeyReader() const { return impl_->cryptoKeyReader; }

ReaderConfiguration& ReaderConfiguration::setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader) {
    impl_->cryptoKeyReader = cryptoKeyReader;
    return *this;
}

ConsumerCryptoFailureAction ReaderConfiguration::getCryptoFailureAction() const {
    return impl_->cryptoFailureAction;
}

ReaderConfiguration& ReaderConfiguration::setCryptoFailureAction(ConsumerCryptoFailureAction action) {
    impl_->cryptoFailureAction = action;
    return *this;
}

bool ReaderConfiguration::hasProperty(const std::string& name) const {
    const auto& properties = impl_->properties;
    return properties.find(name) != properties.cend();
}

const std::string& ReaderConfiguration::getProperty(const std::string& name) const {
    const auto& properties = impl_->properties;
    const auto it = properties.find(name);
    return (it != properties.cend()) ? (it->second) : emptyString;
}

std::map<std::string, std::string>& ReaderConfiguration::getProperties() const { return impl_->properties; }

ReaderConfiguration& ReaderConfiguration::setProperty(const std::string& name, const std::string& value) {
    auto& properties = impl_->properties;
    auto it = properties.find(name);
    if (it != properties.end()) {
        it->second = value;
    } else {
        properties.emplace(name, value);
    }
    return *this;
}

ReaderConfiguration& ReaderConfiguration::setProperties(
    const std::map<std::string, std::string>& properties) {
    for (const auto& kv : properties) {
        setProperty(kv.first, kv.second);
    }
    return *this;
}

}  // namespace pulsar
