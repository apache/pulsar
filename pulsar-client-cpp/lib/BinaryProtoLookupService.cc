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

/*
 * @param lookupUrl service url to do lookup
 * Constructor
 */
BinaryProtoLookupService::BinaryProtoLookupService(ConnectionPool& cnxPool, const std::string& lookupUrl)
    : cnxPool_(cnxPool), serviceUrl_(lookupUrl), mutex_(), requestIdGenerator_(0) {}

/*
 * @param topicName topic name to get broker for
 *
 * Looks up the owner broker for the given topic name
 */
Future<Result, LookupDataResultPtr> BinaryProtoLookupService::lookupAsync(const std::string& topic) {
    TopicNamePtr topicName = TopicName::get(topic);
    if (!topicName) {
        LOG_ERROR("Unable to parse topic - " << topic);
        LookupDataResultPromisePtr promise = std::make_shared<LookupDataResultPromise>();
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    std::string lookupName = topicName->toString();
    LookupDataResultPromisePtr promise = std::make_shared<LookupDataResultPromise>();
    Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(serviceUrl_, serviceUrl_);
    future.addListener(std::bind(&BinaryProtoLookupService::sendTopicLookupRequest, this, lookupName, false,
                                 std::placeholders::_1, std::placeholders::_2, promise));
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
    Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(serviceUrl_, serviceUrl_);
    future.addListener(std::bind(&BinaryProtoLookupService::sendPartitionMetadataLookupRequest, this,
                                 lookupName, std::placeholders::_1, std::placeholders::_2, promise));
    return promise->getFuture();
}

void BinaryProtoLookupService::sendTopicLookupRequest(const std::string& topicName, bool authoritative,
                                                      Result result, const ClientConnectionWeakPtr& clientCnx,
                                                      LookupDataResultPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(ResultConnectError);
        return;
    }
    LookupDataResultPromisePtr lookupPromise = std::make_shared<LookupDataResultPromise>();
    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    conn->newTopicLookup(topicName, authoritative, requestId, lookupPromise);
    lookupPromise->getFuture().addListener(std::bind(&BinaryProtoLookupService::handleLookup, this, topicName,
                                                     std::placeholders::_1, std::placeholders::_2, clientCnx,
                                                     promise));
}

void BinaryProtoLookupService::handleLookup(const std::string& topicName, Result result,
                                            LookupDataResultPtr data,
                                            const ClientConnectionWeakPtr& clientCnx,
                                            LookupDataResultPromisePtr promise) {
    if (data) {
        if (data->isRedirect()) {
            LOG_DEBUG("Lookup request is for " << topicName << " redirected to " << data->getBrokerUrl());

            const std::string& logicalAddress = data->getBrokerUrl();
            const std::string& physicalAddress =
                data->shouldProxyThroughServiceUrl() ? serviceUrl_ : logicalAddress;
            Future<Result, ClientConnectionWeakPtr> future =
                cnxPool_.getConnectionAsync(logicalAddress, physicalAddress);
            future.addListener(std::bind(&BinaryProtoLookupService::sendTopicLookupRequest, this, topicName,
                                         data->isAuthoritative(), std::placeholders::_1,
                                         std::placeholders::_2, promise));
        } else {
            LOG_DEBUG("Lookup response for " << topicName << ", lookup-broker-url " << data->getBrokerUrl());
            promise->setValue(data);
        }
    } else {
        LOG_DEBUG("Lookup failed for " << topicName << ", result " << result);
        promise->setFailed(result);
    }
}

void BinaryProtoLookupService::sendPartitionMetadataLookupRequest(const std::string& topicName, Result result,
                                                                  const ClientConnectionWeakPtr& clientCnx,
                                                                  LookupDataResultPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(ResultConnectError);
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
    Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(serviceUrl_, serviceUrl_);
    future.addListener(std::bind(&BinaryProtoLookupService::sendGetTopicsOfNamespaceRequest, this,
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
