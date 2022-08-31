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
#include "BinaryProtoLookupService.h"
#include "SharedBuffer.h"

#include <lib/TopicName.h>

#include "ConnectionPool.h"

#include <string>

DECLARE_LOG_OBJECT()

namespace pulsar {

auto BinaryProtoLookupService::getBroker(const TopicName& topicName) -> LookupResultFuture {
    return findBroker(serviceNameResolver_.resolveHost(), false, topicName.toString());
}

auto BinaryProtoLookupService::findBroker(const std::string& address, bool authoritative,
                                          const std::string& topic) -> LookupResultFuture {
    LOG_DEBUG("find broker from " << address << ", authoritative: " << authoritative << ", topic: " << topic);
    auto promise = std::make_shared<Promise<Result, LookupResult>>();
    // NOTE: we can use move capture for topic since C++14
    cnxPool_.getConnectionAsync(address).addListener([this, promise, topic, address](
                                                         Result result,
                                                         const ClientConnectionWeakPtr& weakCnx) {
        if (result != ResultOk) {
            promise->setFailed(result);
            return;
        }
        auto cnx = weakCnx.lock();
        if (!cnx) {
            LOG_ERROR("Connection to " << address << " is expired before lookup");
            promise->setFailed(ResultNotConnected);
            return;
        }
        auto lookupPromise = std::make_shared<LookupDataResultPromise>();
        cnx->newTopicLookup(topic, false, listenerName_, newRequestId(), lookupPromise);
        lookupPromise->getFuture().addListener([this, cnx, promise, topic, address](
                                                   Result result, const LookupDataResultPtr& data) {
            if (result != ResultOk || !data) {
                LOG_ERROR("Lookup failed for " << topic << ", result " << result);
                promise->setFailed(result);
                return;
            }

            const auto responseBrokerAddress =
                (serviceNameResolver_.useTls() ? data->getBrokerUrlTls() : data->getBrokerUrl());
            if (data->isRedirect()) {
                LOG_DEBUG("Lookup request is for " << topic << " redirected to " << responseBrokerAddress);
                findBroker(responseBrokerAddress, data->isAuthoritative(), topic)
                    .addListener([promise](Result result, const LookupResult& value) {
                        if (result == ResultOk) {
                            promise->setValue(value);
                        } else {
                            promise->setFailed(result);
                        }
                    });
            } else {
                LOG_DEBUG("Lookup response for " << topic << ", lookup-broker-url " << data->getBrokerUrl());
                if (data->shouldProxyThroughServiceUrl()) {
                    // logicalAddress is the proxy's address, we should still connect through proxy
                    promise->setValue({responseBrokerAddress, address});
                } else {
                    promise->setValue({responseBrokerAddress, responseBrokerAddress});
                }
            }
        });
    });
    return promise->getFuture();
}

/*
 * @param    topicName topic to get number of partitions.
 *
 */
Future<Result, LookupDataResultPtr> BinaryProtoLookupService::getPartitionMetadataAsync(
    const TopicNamePtr& topicName) {
    LookupDataResultPromisePtr promise = std::make_shared<LookupDataResultPromise>();
    if (!topicName) {
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    std::string lookupName = topicName->toString();
    const auto address = serviceNameResolver_.resolveHost();
    cnxPool_.getConnectionAsync(address, address)
        .addListener(std::bind(&BinaryProtoLookupService::sendPartitionMetadataLookupRequest, this,
                               lookupName, std::placeholders::_1, std::placeholders::_2, promise));
    return promise->getFuture();
}

void BinaryProtoLookupService::sendPartitionMetadataLookupRequest(const std::string& topicName, Result result,
                                                                  const ClientConnectionWeakPtr& clientCnx,
                                                                  LookupDataResultPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(result);
        Future<Result, LookupDataResultPtr> future = promise->getFuture();
        return;
    }
    LookupDataResultPromisePtr lookupPromise = std::make_shared<LookupDataResultPromise>();
    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    conn->newPartitionedMetadataLookup(topicName, requestId, lookupPromise);
    lookupPromise->getFuture().addListener(std::bind(&BinaryProtoLookupService::handlePartitionMetadataLookup,
                                                     this, topicName, std::placeholders::_1,
                                                     std::placeholders::_2, clientCnx, promise));
}

void BinaryProtoLookupService::handlePartitionMetadataLookup(const std::string& topicName, Result result,
                                                             LookupDataResultPtr data,
                                                             const ClientConnectionWeakPtr& clientCnx,
                                                             LookupDataResultPromisePtr promise) {
    if (data) {
        LOG_DEBUG("PartitionMetadataLookup response for " << topicName << ", lookup-broker-url "
                                                          << data->getBrokerUrl());
        promise->setValue(data);
    } else {
        LOG_DEBUG("PartitionMetadataLookup failed for " << topicName << ", result " << result);
        promise->setFailed(result);
    }
}

uint64_t BinaryProtoLookupService::newRequestId() {
    Lock lock(mutex_);
    return ++requestIdGenerator_;
}

Future<Result, NamespaceTopicsPtr> BinaryProtoLookupService::getTopicsOfNamespaceAsync(
    const NamespaceNamePtr& nsName) {
    NamespaceTopicsPromisePtr promise = std::make_shared<Promise<Result, NamespaceTopicsPtr>>();
    if (!nsName) {
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    std::string namespaceName = nsName->toString();
    cnxPool_.getConnectionAsync(serviceNameResolver_.resolveHost())
        .addListener(std::bind(&BinaryProtoLookupService::sendGetTopicsOfNamespaceRequest, this,
                               namespaceName, std::placeholders::_1, std::placeholders::_2, promise));
    return promise->getFuture();
}

void BinaryProtoLookupService::sendGetTopicsOfNamespaceRequest(const std::string& nsName, Result result,
                                                               const ClientConnectionWeakPtr& clientCnx,
                                                               NamespaceTopicsPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(ResultConnectError);
        return;
    }

    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    LOG_DEBUG("sendGetTopicsOfNamespaceRequest. requestId: " << requestId << " nsName: " << nsName);

    conn->newGetTopicsOfNamespace(nsName, requestId)
        .addListener(std::bind(&BinaryProtoLookupService::getTopicsOfNamespaceListener, this,
                               std::placeholders::_1, std::placeholders::_2, promise));
}

void BinaryProtoLookupService::getTopicsOfNamespaceListener(Result result, NamespaceTopicsPtr topicsPtr,
                                                            NamespaceTopicsPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(ResultLookupError);
        return;
    }

    promise->setValue(topicsPtr);
}

}  // namespace pulsar
