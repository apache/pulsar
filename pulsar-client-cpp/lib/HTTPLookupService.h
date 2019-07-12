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
#ifndef PULSAR_CPP_HTTPLOOKUPSERVICE_H
#define PULSAR_CPP_HTTPLOOKUPSERVICE_H

#include <lib/LookupService.h>
#include <lib/ClientImpl.h>
#include <lib/Url.h>
#include <lib/Version.h>

namespace pulsar {
class HTTPLookupService : public LookupService, public std::enable_shared_from_this<HTTPLookupService> {
    class CurlInitializer {
       public:
        CurlInitializer();
        ~CurlInitializer();
    };
    static CurlInitializer curlInitializer;
    enum RequestType
    {
        Lookup,
        PartitionMetaData
    };
    typedef Promise<Result, LookupDataResultPtr> LookupPromise;
    ExecutorServiceProviderPtr executorProvider_;
    std::string adminUrl_;
    AuthenticationPtr authenticationPtr_;
    int lookupTimeoutInSeconds_;
    bool tlsAllowInsecure_;
    bool isUseTls_;
    std::string tlsTrustCertsFilePath_;
    bool tlsValidateHostname_;

    static LookupDataResultPtr parsePartitionData(const std::string&);
    static LookupDataResultPtr parseLookupData(const std::string&);
    static NamespaceTopicsPtr parseNamespaceTopicsData(const std::string&);

    void handleLookupHTTPRequest(LookupPromise, const std::string, RequestType);
    void handleNamespaceTopicsHTTPRequest(NamespaceTopicsPromise promise, const std::string completeUrl);

    Result sendHTTPRequest(const std::string completeUrl, std::string& responseData);

   public:
    HTTPLookupService(const std::string&, const ClientConfiguration&, const AuthenticationPtr&);

    Future<Result, LookupDataResultPtr> lookupAsync(const std::string&);

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr&);

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName);
};
}  // namespace pulsar

#endif  // PULSAR_CPP_HTTPLOOKUPSERVICE_H
