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
#ifndef PULSAR_CPP_LOOKUPSERVICE_H
#define PULSAR_CPP_LOOKUPSERVICE_H

#include <lib/LookupDataResult.h>
#include <pulsar/Result.h>
#include <lib/Future.h>
#include <lib/DestinationName.h>
#include <lib/LogUtils.h>

namespace pulsar {
class LookupService {
public:
    /*
     * @param    destinationName - topic name
     *
     * Looks up the owner broker for the given destination name
     */
    virtual Future<Result, LookupDataResultPtr> lookupAsync(const std::string& destinationName) = 0;

    /*
     * @param    dn - pointer to destination (topic) name
     *
     * Gets Partition metadata
     */
    virtual Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr& dn) = 0;
};
typedef boost::shared_ptr<LookupService> LookupServicePtr;
}
#endif //PULSAR_CPP_LOOKUPSERVICE_H
