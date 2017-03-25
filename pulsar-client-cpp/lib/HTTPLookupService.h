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
    public:
        HTTPLookupService(const std::string& lookupUrl, const ClientConfiguration& clientConfiguration,
                      ExecutorServiceProviderPtr executorProvider, const AuthenticationPtr& authData);

        Future<Result, LookupDataResultPtr> lookupAsync(const std::string& destinationName);

        Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr& dn);

        static void callback(const boost::system::error_code& er, const HTTPWrapperResponse& response,
                                        Promise<Result, LookupDataResultPtr> promise);
    private:
        typedef Promise<Result, LookupDataResultPtr> LookupPromise;
        ExecutorServiceProviderPtr executorProvider_;
        Url adminUrl_;
        AuthenticationPtr authenticationPtr_;
        TimeDuration lookupTimeout_;

        static LookupDataResultPtr parsePartitionData(std::string& json);
        static LookupDataResultPtr parseLookupData(std::string& json);
    };

}

#endif //PULSAR_CPP_HTTPLOOKUPSERVICE_H
