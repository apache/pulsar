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
#include <lib/ProducerConfigurationImpl.h>

namespace pulsar {

const static std::string emptyString;

ProducerConfiguration::ProducerConfiguration() : impl_(std::make_shared<ProducerConfigurationImpl>()) {}

ProducerConfiguration::~ProducerConfiguration() {}

ProducerConfiguration::ProducerConfiguration(const ProducerConfiguration& x) : impl_(x.impl_) {}

ProducerConfiguration& ProducerConfiguration::operator=(const ProducerConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ProducerConfiguration& ProducerConfiguration::setProducerName(const std::string& producerName) {
    impl_->producerName = Optional<std::string>::of(producerName);
    return *this;
}

const std::string& ProducerConfiguration::getProducerName() const {
    return impl_->producerName.is_present() ? impl_->producerName.value() : emptyString;
}

ProducerConfiguration& ProducerConfiguration::setInitialSequenceId(int64_t initialSequenceId) {
    impl_->initialSequenceId = Optional<int64_t>::of(initialSequenceId);
    return *this;
}

int64_t ProducerConfiguration::getInitialSequenceId() const {
    return impl_->initialSequenceId.is_present() ? impl_->initialSequenceId.value() : -1ll;
}

ProducerConfiguration& ProducerConfiguration::setSendTimeout(int sendTimeoutMs) {
    impl_->sendTimeoutMs = sendTimeoutMs;
    return *this;
}

int ProducerConfiguration::getSendTimeout() const { return impl_->sendTimeoutMs; }

ProducerConfiguration& ProducerConfiguration::setCompressionType(CompressionType compressionType) {
    impl_->compressionType = compressionType;
    return *this;
}

CompressionType ProducerConfiguration::getCompressionType() const { return impl_->compressionType; }

ProducerConfiguration& ProducerConfiguration::setMaxPendingMessages(int maxPendingMessages) {
    if (maxPendingMessages <= 0) {
        throw "maxPendingMessages needs to be greater than 0";
    }
    impl_->maxPendingMessages = maxPendingMessages;
    return *this;
}

int ProducerConfiguration::getMaxPendingMessages() const { return impl_->maxPendingMessages; }

ProducerConfiguration& ProducerConfiguration::setMaxPendingMessagesAcrossPartitions(int maxPendingMessages) {
    if (maxPendingMessages <= 0) {
        throw "maxPendingMessages needs to be greater than 0";
    }
    impl_->maxPendingMessagesAcrossPartitions = maxPendingMessages;
    return *this;
}

int ProducerConfiguration::getMaxPendingMessagesAcrossPartitions() const {
    return impl_->maxPendingMessagesAcrossPartitions;
}

ProducerConfiguration& ProducerConfiguration::setPartitionsRoutingMode(const PartitionsRoutingMode& mode) {
    impl_->routingMode = mode;
    return *this;
}

ProducerConfiguration::PartitionsRoutingMode ProducerConfiguration::getPartitionsRoutingMode() const {
    return impl_->routingMode;
}

ProducerConfiguration& ProducerConfiguration::setMessageRouter(const MessageRoutingPolicyPtr& router) {
    impl_->routingMode = ProducerConfiguration::CustomPartition;
    impl_->messageRouter = router;
    return *this;
}

const MessageRoutingPolicyPtr& ProducerConfiguration::getMessageRouterPtr() const {
    return impl_->messageRouter;
}

ProducerConfiguration& ProducerConfiguration::setHashingScheme(const HashingScheme& scheme) {
    impl_->hashingScheme = scheme;
    return *this;
}

ProducerConfiguration::HashingScheme ProducerConfiguration::getHashingScheme() const {
    return impl_->hashingScheme;
}

ProducerConfiguration& ProducerConfiguration::setBlockIfQueueFull(bool flag) {
    impl_->blockIfQueueFull = flag;
    return *this;
}

bool ProducerConfiguration::getBlockIfQueueFull() const { return impl_->blockIfQueueFull; }

ProducerConfiguration& ProducerConfiguration::setBatchingEnabled(const bool& batchingEnabled) {
    impl_->batchingEnabled = batchingEnabled;
    return *this;
}
const bool& ProducerConfiguration::getBatchingEnabled() const { return impl_->batchingEnabled; }

ProducerConfiguration& ProducerConfiguration::setBatchingMaxMessages(
    const unsigned int& batchingMaxMessages) {
    if (batchingMaxMessages <= 1) {
        throw "batchingMaxMessages needs to be greater than 1";
    }
    impl_->batchingMaxMessages = batchingMaxMessages;
    return *this;
}

const unsigned int& ProducerConfiguration::getBatchingMaxMessages() const {
    return impl_->batchingMaxMessages;
}

ProducerConfiguration& ProducerConfiguration::setBatchingMaxAllowedSizeInBytes(
    const unsigned long& batchingMaxAllowedSizeInBytes) {
    impl_->batchingMaxAllowedSizeInBytes = batchingMaxAllowedSizeInBytes;
    return *this;
}
const unsigned long& ProducerConfiguration::getBatchingMaxAllowedSizeInBytes() const {
    return impl_->batchingMaxAllowedSizeInBytes;
}

ProducerConfiguration& ProducerConfiguration::setBatchingMaxPublishDelayMs(
    const unsigned long& batchingMaxPublishDelayMs) {
    impl_->batchingMaxPublishDelayMs = batchingMaxPublishDelayMs;
    return *this;
}

const unsigned long& ProducerConfiguration::getBatchingMaxPublishDelayMs() const {
    return impl_->batchingMaxPublishDelayMs;
}

const CryptoKeyReaderPtr ProducerConfiguration::getCryptoKeyReader() const { return impl_->cryptoKeyReader; }

ProducerConfiguration& ProducerConfiguration::setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader) {
    impl_->cryptoKeyReader = cryptoKeyReader;
    return *this;
}

ProducerCryptoFailureAction ProducerConfiguration::getCryptoFailureAction() const {
    return impl_->cryptoFailureAction;
}

ProducerConfiguration& ProducerConfiguration::setCryptoFailureAction(ProducerCryptoFailureAction action) {
    impl_->cryptoFailureAction = action;
    return *this;
}

std::set<std::string>& ProducerConfiguration::getEncryptionKeys() { return impl_->encryptionKeys; }

bool ProducerConfiguration::isEncryptionEnabled() const {
    return (!impl_->encryptionKeys.empty() && (impl_->cryptoKeyReader != NULL));
}

ProducerConfiguration& ProducerConfiguration::addEncryptionKey(std::string key) {
    impl_->encryptionKeys.insert(key);
    return *this;
}

ProducerConfiguration& ProducerConfiguration::setSchema(const SchemaInfo& schemaInfo) {
    impl_->schemaInfo = schemaInfo;
    return *this;
}

const SchemaInfo& ProducerConfiguration::getSchema() const { return impl_->schemaInfo; }

bool ProducerConfiguration::hasProperty(const std::string& name) const {
    const std::map<std::string, std::string>& m = impl_->properties;
    return m.find(name) != m.end();
}

const std::string& ProducerConfiguration::getProperty(const std::string& name) const {
    if (hasProperty(name)) {
        const std::map<std::string, std::string>& m = impl_->properties;
        return m.at(name);
    } else {
        return emptyString;
    }
}

std::map<std::string, std::string>& ProducerConfiguration::getProperties() const { return impl_->properties; }

ProducerConfiguration& ProducerConfiguration::setProperty(const std::string& name, const std::string& value) {
    impl_->properties.insert(std::make_pair(name, value));
    return *this;
}

ProducerConfiguration& ProducerConfiguration::setProperties(
    const std::map<std::string, std::string>& properties) {
    for (std::map<std::string, std::string>::const_iterator it = properties.begin(); it != properties.end();
         it++) {
        setProperty(it->first, it->second);
    }
    return *this;
}

}  // namespace pulsar
