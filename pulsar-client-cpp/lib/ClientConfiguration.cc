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
#include <lib/ClientConfigurationImpl.h>

namespace pulsar {

ClientConfiguration::ClientConfiguration() : impl_(std::make_shared<ClientConfigurationImpl>()) {}

ClientConfiguration::~ClientConfiguration() {}

ClientConfiguration::ClientConfiguration(const ClientConfiguration& x) : impl_(x.impl_) {}

ClientConfiguration& ClientConfiguration::operator=(const ClientConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ClientConfiguration& ClientConfiguration::setAuth(const AuthenticationPtr& authentication) {
    impl_->authenticationPtr = authentication;
    return *this;
}

const Authentication& ClientConfiguration::getAuth() const { return *impl_->authenticationPtr; }

const AuthenticationPtr& ClientConfiguration::getAuthPtr() const { return impl_->authenticationPtr; }

ClientConfiguration& ClientConfiguration::setOperationTimeoutSeconds(int timeout) {
    impl_->operationTimeoutSeconds = timeout;
    return *this;
}

int ClientConfiguration::getOperationTimeoutSeconds() const { return impl_->operationTimeoutSeconds; }

ClientConfiguration& ClientConfiguration::setIOThreads(int threads) {
    impl_->ioThreads = threads;
    return *this;
}

int ClientConfiguration::getIOThreads() const { return impl_->ioThreads; }

ClientConfiguration& ClientConfiguration::setMessageListenerThreads(int threads) {
    impl_->messageListenerThreads = threads;
    return *this;
}

int ClientConfiguration::getMessageListenerThreads() const { return impl_->messageListenerThreads; }

ClientConfiguration& ClientConfiguration::setUseTls(bool useTls) {
    impl_->useTls = useTls;
    return *this;
}

bool ClientConfiguration::isUseTls() const { return impl_->useTls; }

ClientConfiguration& ClientConfiguration::setValidateHostName(bool validateHostName) {
    impl_->validateHostName = validateHostName;
    return *this;
}

bool ClientConfiguration::isValidateHostName() const { return impl_->validateHostName; }

ClientConfiguration& ClientConfiguration::setTlsTrustCertsFilePath(const std::string& filePath) {
    impl_->tlsTrustCertsFilePath = filePath;
    return *this;
}

std::string ClientConfiguration::getTlsTrustCertsFilePath() const { return impl_->tlsTrustCertsFilePath; }

ClientConfiguration& ClientConfiguration::setTlsAllowInsecureConnection(bool allowInsecure) {
    impl_->tlsAllowInsecureConnection = allowInsecure;
    return *this;
}

bool ClientConfiguration::isTlsAllowInsecureConnection() const { return impl_->tlsAllowInsecureConnection; }

ClientConfiguration& ClientConfiguration::setConcurrentLookupRequest(int concurrentLookupRequest) {
    impl_->concurrentLookupRequest = concurrentLookupRequest;
    return *this;
}

int ClientConfiguration::getConcurrentLookupRequest() const { return impl_->concurrentLookupRequest; }

ClientConfiguration& ClientConfiguration::setLogConfFilePath(const std::string& logConfFilePath) {
    impl_->logConfFilePath = logConfFilePath;
    return *this;
}

const std::string& ClientConfiguration::getLogConfFilePath() const { return impl_->logConfFilePath; }

ClientConfiguration& ClientConfiguration::setLogger(LoggerFactoryPtr loggerFactory) {
    impl_->loggerFactory = loggerFactory;
    return *this;
}

LoggerFactoryPtr ClientConfiguration::getLogger() const { return impl_->loggerFactory; }

ClientConfiguration& ClientConfiguration::setStatsIntervalInSeconds(
    const unsigned int& statsIntervalInSeconds) {
    impl_->statsIntervalInSeconds = statsIntervalInSeconds;
    return *this;
}

const unsigned int& ClientConfiguration::getStatsIntervalInSeconds() const {
    return impl_->statsIntervalInSeconds;
}
}  // namespace pulsar
