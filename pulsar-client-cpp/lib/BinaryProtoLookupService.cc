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

#include "DestinationName.h"
#include "BinaryProtoLookupService.h"
#include "LogUtils.h"
#include "SharedBuffer.h"

#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include "ConnectionPool.h"

#include <string>

DECLARE_LOG_OBJECT()

namespace pulsar {

    /*
     * @param lookupUrl service url to do lookup
     * Constructor
     */
    BinaryProtoLookupService::BinaryProtoLookupService(ConnectionPool& cnxPool, const std::string& lookupUrl)
    :
    cnxPool_(cnxPool),
    serviceUrl_(lookupUrl),
    mutex_(),
    requestIdGenerator_(0) {}

    /*
     * @param destination_name topic name to get broker for
     *
     * Looks up the owner broker for the given destination name
     */
    Future<Result, LookupDataResultPtr> BinaryProtoLookupService::lookupAsync(const std::string& destinationName) {
        DestinationNamePtr dn = DestinationName::get(destinationName);
        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            LookupDataResultPromisePtr promise = boost::make_shared<LookupDataResultPromise>();
            promise->setFailed(ResultInvalidTopicName);
            return promise->getFuture();
        }
        std::string lookupName = dn->toString();
        LookupDataResultPromisePtr promise = boost::make_shared<LookupDataResultPromise>();
        Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(serviceUrl_);
        future.addListener(boost::bind(&BinaryProtoLookupService::sendTopicLookupRequest, this, lookupName, false, _1, _2, promise));
        return promise->getFuture();
    }

    /*
     * @param    destination_name topic to get number of partitions.
     *
     */
    Future<Result, LookupDataResultPtr> BinaryProtoLookupService::getPartitionMetadataAsync(const DestinationNamePtr& dn) {
        LookupDataResultPromisePtr promise = boost::make_shared<LookupDataResultPromise>();
        if (!dn) {
            promise->setFailed(ResultInvalidTopicName);
            return promise->getFuture();
        }
        std::string lookupName = dn->toString();
        Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(serviceUrl_);
        future.addListener(boost::bind(&BinaryProtoLookupService::sendPartitionMetadataLookupRequest, this, lookupName, _1, _2, promise));
        return promise->getFuture();
    }


    void BinaryProtoLookupService::sendTopicLookupRequest(const std::string& destinationName, bool authoritative, Result result, const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise) {
        if (result != ResultOk) {
            promise->setFailed(ResultConnectError);
            return;
        }
        LookupDataResultPromisePtr lookupPromise = boost::make_shared<LookupDataResultPromise>();
        ClientConnectionPtr conn = clientCnx.lock();
        uint64_t requestId = newRequestId();
        conn->newTopicLookup(destinationName, authoritative, requestId, lookupPromise);
        lookupPromise->getFuture().addListener(boost::bind(&BinaryProtoLookupService::handleLookup, this, destinationName, _1, _2, clientCnx, promise));
    }

    void BinaryProtoLookupService::handleLookup(const std::string& destinationName,
            Result result, LookupDataResultPtr data, const ClientConnectionWeakPtr& clientCnx,
            LookupDataResultPromisePtr promise) {
        if (data) {
            if(data ->isRedirect()) {
                LOG_DEBUG("Lookup request is for " << destinationName << " redirected to " << data->getBrokerUrl());
                Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(data->getBrokerUrl());
                future.addListener(boost::bind(&BinaryProtoLookupService::sendTopicLookupRequest, this, destinationName, data->isAuthoritative(), _1, _2, promise));
            } else {
                LOG_DEBUG("Lookup response for " << destinationName << ", lookup-broker-url " << data->getBrokerUrl());
                promise->setValue(data);
            }
        } else {
            LOG_DEBUG("Lookup failed for " << destinationName << ", result " << result);
            promise->setFailed(result);
        }
    }

    void BinaryProtoLookupService::sendPartitionMetadataLookupRequest(const std::string& destinationName, Result result, const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise) {
        if (result != ResultOk) {
            promise->setFailed(ResultConnectError);
            Future<Result, LookupDataResultPtr> future = promise->getFuture();
            return;
        }
        LookupDataResultPromisePtr lookupPromise = boost::make_shared<LookupDataResultPromise>();
        ClientConnectionPtr conn = clientCnx.lock();
        uint64_t requestId = newRequestId();
        conn->newPartitionedMetadataLookup(destinationName, requestId, lookupPromise);
        lookupPromise->getFuture().addListener(boost::bind(&BinaryProtoLookupService::handleLookup, this, destinationName, _1, _2, clientCnx, promise));
    }

    void BinaryProtoLookupService::handlePartitionMetadataLookup(const std::string& destinationName,
            Result result, LookupDataResultPtr data, const ClientConnectionWeakPtr& clientCnx,
            LookupDataResultPromisePtr promise) {
        if (data) {
            if(data->isRedirect()) {
                LOG_DEBUG("Lookup request is for " << destinationName << " redirected to " << data->getBrokerUrl());
                Future<Result, ClientConnectionWeakPtr> future = cnxPool_.getConnectionAsync(data->getBrokerUrl());
                future.addListener(boost::bind(&BinaryProtoLookupService::sendPartitionMetadataLookupRequest, this, destinationName, _1, _2, promise));
            } else {
                LOG_DEBUG("Lookup response for " << destinationName << ", lookup-broker-url " << data->getBrokerUrl());
                promise->setValue(data);
            }
        } else {
            LOG_DEBUG("Lookup failed for " << destinationName << ", result " << result);
            promise->setFailed(result);
        }
    }

    uint64_t BinaryProtoLookupService::newRequestId() {
        Lock lock(mutex_);
        return ++requestIdGenerator_;
    }

}
