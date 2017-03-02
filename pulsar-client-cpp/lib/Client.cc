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

#include <iostream>
#include <pulsar/Client.h>
#include <utility>

#include <boost/make_shared.hpp>
#include <boost/smart_ptr.hpp>

#include "ClientImpl.h"
#include "Utils.h"
#include "ExecutorService.h"
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

struct ClientConfiguration::Impl {
    AuthenticationPtr authData;
    int ioThreads;
    int operationTimeoutSeconds;
    int messageListenerThreads;
    int concurrentLookupRequest;
    std::string logConfFilePath;
    bool useTls;
    std::string tlsTrustCertsFilePath;
    bool tlsAllowInsecureConnection;
    Impl() : authData(Auth::Disabled()),
             ioThreads(1),
             operationTimeoutSeconds(30),
             messageListenerThreads(1),
             concurrentLookupRequest(5000),
             logConfFilePath(),
             useTls(false),
             tlsAllowInsecureConnection(true) {}
};

ClientConfiguration::ClientConfiguration()
        : impl_(boost::make_shared<Impl>()) {
}

ClientConfiguration::~ClientConfiguration() {
}

ClientConfiguration::ClientConfiguration(const ClientConfiguration& x)
    : impl_(x.impl_) {
}

ClientConfiguration& ClientConfiguration::operator=(const ClientConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ClientConfiguration& ClientConfiguration::setAuthentication(const AuthenticationPtr& authentication) {
    impl_->authData = authentication;
    return *this;
}

const Authentication& ClientConfiguration::getAuthentication() const {
    return *impl_->authData;
}

const AuthenticationPtr& ClientConfiguration::getAuthenticationPtr() const {
    return impl_->authData;
}

ClientConfiguration& ClientConfiguration::setOperationTimeoutSeconds(int timeout) {
    impl_->operationTimeoutSeconds = timeout;
    return *this;
}

int ClientConfiguration::getOperationTimeoutSeconds() const {
    return impl_->operationTimeoutSeconds;
}

ClientConfiguration& ClientConfiguration::setIOThreads(int threads) {
    impl_->ioThreads = threads;
    return *this;
}

int ClientConfiguration::getIOThreads() const {
    return impl_->ioThreads;
}

ClientConfiguration& ClientConfiguration::setMessageListenerThreads(int threads) {
    impl_->messageListenerThreads = threads;
    return *this;
}

int ClientConfiguration::getMessageListenerThreads() const {
    return impl_->messageListenerThreads;
}

ClientConfiguration& ClientConfiguration::setUseTls(bool useTls) {
    impl_->useTls = useTls;
    return *this;
}

bool ClientConfiguration::isUseTls() const {
    return impl_->useTls;
}

ClientConfiguration& ClientConfiguration::setTlsTrustCertsFilePath(const std::string &filePath) {
    impl_->tlsTrustCertsFilePath = filePath;
    return *this;
}

std::string ClientConfiguration::getTlsTrustCertsFilePath() const {
    return impl_->tlsTrustCertsFilePath;
}

ClientConfiguration& ClientConfiguration::setTlsAllowInsecureConnection(bool allowInsecure) {
    impl_->tlsAllowInsecureConnection = allowInsecure;
    return *this;
}

bool ClientConfiguration::isTlsAllowInsecureConnection() const {
    return impl_->tlsAllowInsecureConnection;
}

ClientConfiguration& ClientConfiguration::setConcurrentLookupRequest(int concurrentLookupRequest) {
    impl_->concurrentLookupRequest = concurrentLookupRequest;
    return *this;
}

int ClientConfiguration::getConcurrentLookupRequest() const {
    return impl_->concurrentLookupRequest;
}

ClientConfiguration& ClientConfiguration::setLogConfFilePath(const std::string& logConfFilePath) {
    impl_->logConfFilePath = logConfFilePath;
    return *this;
}

const std::string& ClientConfiguration::getLogConfFilePath() const {
    return impl_->logConfFilePath;
}

/////////////////////////////////////////////////////////////////

Client::Client(const std::string& serviceUrl)
        : impl_(boost::make_shared<ClientImpl>(serviceUrl, ClientConfiguration(), true)) {
}

Client::Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration)
        : impl_(boost::make_shared<ClientImpl>(serviceUrl, clientConfiguration, true)) {
}

Client::Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
               bool poolConnections)
        : impl_(boost::make_shared<ClientImpl>(serviceUrl, clientConfiguration, poolConnections)) {
}

Result Client::createProducer(const std::string& topic, Producer& producer) {
    return createProducer(topic, ProducerConfiguration(), producer);
}

Result Client::createProducer(const std::string& topic, const ProducerConfiguration& conf,
                              Producer& producer) {
    Promise<Result, Producer> promise;
    createProducerAsync(topic, conf, WaitForCallbackValue<Producer>(promise));
    Future<Result, Producer> future = promise.getFuture();

    return future.get(producer);
}

void Client::createProducerAsync(const std::string& topic, CreateProducerCallback callback) {
    createProducerAsync(topic, ProducerConfiguration(), callback);
}

void Client::createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                                 CreateProducerCallback callback) {
    impl_->createProducerAsync(topic, conf, callback);
}

Result Client::subscribe(const std::string& topic, const std::string& consumerName,
                         Consumer& consumer) {
    return subscribe(topic, consumerName, ConsumerConfiguration(), consumer);
}

Result Client::subscribe(const std::string& topic, const std::string& consumerName,
                         const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeAsync(topic, consumerName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeAsync(const std::string& topic, const std::string& consumerName,
                            SubscribeCallback callback) {
    subscribeAsync(topic, consumerName, ConsumerConfiguration(), callback);
}

void Client::subscribeAsync(const std::string& topic, const std::string& consumerName,
                            const ConsumerConfiguration& conf, SubscribeCallback callback) {
    LOG_DEBUG("Topic is :" << topic);
    impl_->subscribeAsync(topic, consumerName, conf, callback);
}

Result Client::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Client::closeAsync(CloseCallback callback) {
    impl_->closeAsync(callback);
}

void Client::shutdown() {
    impl_->shutdown();
}

}
