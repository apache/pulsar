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
#include <lib/LogUtils.h>
#include <lib/TopicName.h>

#include <vector>

namespace pulsar {
typedef std::shared_ptr<std::vector<std::string>> NamespaceTopicsPtr;
typedef Promise<Result, NamespaceTopicsPtr> NamespaceTopicsPromise;
typedef std::shared_ptr<Promise<Result, NamespaceTopicsPtr>> NamespaceTopicsPromisePtr;

class LookupService {
   public:
    /*
     * @param    topicName - topic name
     *
     * Looks up the owner broker for the given topic name
     */
    virtual Future<Result, LookupDataResultPtr> lookupAsync(const std::string& topicName) = 0;

    /*
     * @param    topicName - pointer to topic name
     *
     * Gets Partition metadata
     */
    virtual Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) = 0;

    /**
     * @param   namespace - namespace-name
     *
     * Returns all the topics name for a given namespace.
     */
    virtual Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName) = 0;

    virtual ~LookupService() {}
};

typedef std::shared_ptr<LookupService> LookupServicePtr;

}  // namespace pulsar
#endif  // PULSAR_CPP_LOOKUPSERVICE_H
