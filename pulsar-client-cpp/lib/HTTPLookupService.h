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
#include <lib/HTTPWrapper.h>

namespace pulsar {
    class HTTPLookupService : public LookupService {
        enum RequestType {Lookup, PartitionMetaData};
        typedef Promise<Result, LookupDataResultPtr> LookupPromise;
        ExecutorServiceProviderPtr executorProvider_;
        Url adminUrl_;
        AuthenticationPtr authenticationPtr_;
        TimeDuration lookupTimeout_;

        static LookupDataResultPtr parsePartitionData(const std::string&);
        static LookupDataResultPtr parseLookupData(const std::string&);
        void sendHTTPRequest(LookupPromise, std::stringstream&, RequestType);
        DeadlineTimerPtr startTimer(LookupPromise);
    public:
        HTTPLookupService(const std::string&, const ClientConfiguration&,
                      ExecutorServiceProviderPtr, const AuthenticationPtr&);

        Future<Result, LookupDataResultPtr> lookupAsync(const std::string&);

        Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr&);

        static void callback(HTTPWrapperPtr, Promise<Result, LookupDataResultPtr>, RequestType, DeadlineTimerPtr);
        void handleTimeout(const boost::system::error_code&, LookupPromise);
    };

}

#endif //PULSAR_CPP_HTTPLOOKUPSERVICE_H
