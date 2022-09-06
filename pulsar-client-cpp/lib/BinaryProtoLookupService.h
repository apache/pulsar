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
#ifndef _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_
#define _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_

#include <iostream>
#include <pulsar/defines.h>
#include <pulsar/Authentication.h>
#include "ConnectionPool.h"
#include "Backoff.h"
#include <lib/LookupService.h>
#include <mutex>
#include "ServiceNameResolver.h"

namespace pulsar {
class LookupDataResult;

class PULSAR_PUBLIC BinaryProtoLookupService : public LookupService {
   public:
    BinaryProtoLookupService(ServiceNameResolver& serviceNameResolver, ConnectionPool& pool,
                             const std::string& listenerName)
        : serviceNameResolver_(serviceNameResolver), cnxPool_(pool), listenerName_(listenerName) {}

    LookupResultFuture getBroker(const TopicName& topicName) override;

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) override;

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName) override;

   private:
    std::mutex mutex_;
    uint64_t requestIdGenerator_ = 0;

    ServiceNameResolver& serviceNameResolver_;
    ConnectionPool& cnxPool_;
    std::string listenerName_;

    // TODO: limit the redirect count, see https://github.com/apache/pulsar/pull/7096
    LookupResultFuture findBroker(const std::string& address, bool authoritative, const std::string& topic);

    void sendPartitionMetadataLookupRequest(const std::string& topicName, Result result,
                                            const ClientConnectionWeakPtr& clientCnx,
                                            LookupDataResultPromisePtr promise);

    void handlePartitionMetadataLookup(const std::string& topicName, Result result, LookupDataResultPtr data,
                                       const ClientConnectionWeakPtr& clientCnx,
                                       LookupDataResultPromisePtr promise);

    void sendGetTopicsOfNamespaceRequest(const std::string& nsName, Result result,
                                         const ClientConnectionWeakPtr& clientCnx,
                                         NamespaceTopicsPromisePtr promise);

    void getTopicsOfNamespaceListener(Result result, NamespaceTopicsPtr topicsPtr,
                                      NamespaceTopicsPromisePtr promise);

    uint64_t newRequestId();
};
typedef std::shared_ptr<BinaryProtoLookupService> BinaryProtoLookupServicePtr;
}  // namespace pulsar

#endif  //_PULSAR_BINARY_LOOKUP_SERVICE_HEADER_
