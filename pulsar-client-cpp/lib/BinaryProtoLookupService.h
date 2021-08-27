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

namespace pulsar {
class LookupDataResult;

class PULSAR_PUBLIC BinaryProtoLookupService : public LookupService {
   public:
    /*
     * constructor
     */
    BinaryProtoLookupService(ConnectionPool& cnxPool, const std::string& serviceUrl);

    BinaryProtoLookupService(ConnectionPool& cnxPool, const std::string& serviceUrl,
                             const std::string& listenerName);

    Future<Result, LookupDataResultPtr> lookupAsync(const std::string& topicName);

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName);

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName);

   private:
    std::mutex mutex_;
    uint64_t requestIdGenerator_ = 0;

    std::string serviceUrl_;
    std::string listenerName_;
    ConnectionPool& cnxPool_;

    void sendTopicLookupRequest(const std::string& topicName, bool authoritative,
                                const std::string& listenerName, Result result,
                                const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise);

    void handleLookup(const std::string& topicName, Result result, LookupDataResultPtr data,
                      const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise);

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
