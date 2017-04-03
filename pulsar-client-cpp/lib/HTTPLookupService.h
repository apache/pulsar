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

#ifndef PULSAR_CPP_HTTPLOOKUPSERVICE_H
#define PULSAR_CPP_HTTPLOOKUPSERVICE_H

#include <lib/LookupService.h>
#include <lib/ClientImpl.h>
#include <lib/Url.h>
#include <json/value.h>
#include <json/reader.h>
#include <boost/bind.hpp>

namespace pulsar {
    class HTTPLookupService : public LookupService {
        enum RequestType {Lookup, PartitionMetaData};
        typedef Promise<Result, LookupDataResultPtr> LookupPromise;
        ExecutorServiceProviderPtr executorProvider_;
        std::string adminUrl_;
        AuthenticationPtr authenticationPtr_;
        int lookupTimeoutInSeconds_;

        static LookupDataResultPtr parsePartitionData(const std::string&);
        static LookupDataResultPtr parseLookupData(const std::string&);
        void sendHTTPRequest(LookupPromise, const std::string, RequestType);
     public:
        ~HTTPLookupService();

        HTTPLookupService(const std::string&, const ClientConfiguration&, const AuthenticationPtr&);

        Future<Result, LookupDataResultPtr> lookupAsync(const std::string&);

        Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr&);
    };

}

#endif //PULSAR_CPP_HTTPLOOKUPSERVICE_H
