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

#ifndef _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_
#define _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_

#include <iostream>

#include <boost/shared_ptr.hpp>
#include <pulsar/Auth.h>
#include <pulsar/Result.h>
#include "ConnectionPool.h"
#include "DestinationName.h"
#include "Future.h"
#include "LookupDataResult.h"
#include "Backoff.h"

#pragma GCC visibility push(default)

namespace pulsar {
class LookupDataResult;

class BinaryProtoLookupService {
 public:
    /*
     * constructor
     */
    BinaryProtoLookupService(ConnectionPool& cnxPool, const std::string& serviceUrl);

    Future<Result, LookupDataResultPtr> lookupAsync(const std::string& destinationName);

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr& dn);

 private:

    boost::mutex mutex_;
    uint64_t requestIdGenerator_;

    std::string serviceUrl_;
    ConnectionPool& cnxPool_;

    void sendTopicLookupRequest(const std::string& destinationName, bool authoritative, Result result,
                     const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise);

    void handleLookup(const std::string& destinationName, Result result, LookupDataResultPtr data,
                      const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise);


    void sendPartitionMetadataLookupRequest(const std::string& destinationName, Result result,
                                   const ClientConnectionWeakPtr& clientCnx,
                                   LookupDataResultPromisePtr promise);

    void handlePartitionMetadataLookup(const std::string& destinationName, Result result, LookupDataResultPtr data,
                          const ClientConnectionWeakPtr& clientCnx, LookupDataResultPromisePtr promise);


    uint64_t newRequestId();

};

}

#pragma GCC visibility pop

#endif //_PULSAR_BINARY_LOOKUP_SERVICE_HEADER_
