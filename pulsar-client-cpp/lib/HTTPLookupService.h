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
#include <boost/bind.hpp>
#include <json/value.h>
#include <json/reader.h>

namespace pulsar {

    class HTTPLookupService : public LookupService {
    public:
        HTTPLookupService(const std::string& lookupUrl, const ClientConfiguration& clientConfiguration,
                      ExecutorServiceProviderPtr executorProvider, const AuthenticationPtr& authData);

        Future<Result, LookupDataResultPtr> lookupAsync(const std::string& destinationName);

        Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr& dn);

    private:
        typedef Promise<Result, LookupDataResultPtr> LookupPromise;
        ExecutorServiceProviderPtr executorProvider_;
        Url adminUrl_;
        AuthenticationPtr authenticationPtr_;
        TimeDuration lookupTimeout_;
        void handle_resolve(ReadStreamPtr requestStreamPtr, LookupPromise promise, TcpResolverPtr resolverPtr,
                            const boost::system::error_code& err,
                            boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

        void handle_connect(ReadStreamPtr requestStreamPtr, LookupPromise promise, SocketPtr socketPtr,
                            const boost::system::error_code& err,
                            boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

        void handle_write_request(LookupPromise promise, SocketPtr socketPtr,
                                  const boost::system::error_code& err);

        void handle_read_status_line(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                     SocketPtr socketPtr, const boost::system::error_code& err);

        void handle_read_headers(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                 SocketPtr socketPtr, const boost::system::error_code& err);

        void handle_read_content(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                 SocketPtr socketPtr, const boost::system::error_code& err);

        static LookupDataResultPtr parsePartitionData(ReadStreamPtr streamPtr);

        static LookupDataResultPtr parseLookupData(ReadStreamPtr streamPtr);
    };

}

#endif //PULSAR_CPP_HTTPLOOKUPSERVICE_H
