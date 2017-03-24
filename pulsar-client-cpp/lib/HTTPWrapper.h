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

#ifndef PULSAR_CPP_HTTPWRAPPER_H
#define PULSAR_CPP_HTTPWRAPPER_H
#include <lib/LookupService.h>
#include <lib/ClientImpl.h>
#include <lib/Url.h>
#include <boost/bind.hpp>
#include <vector>
#include <string>

namespace pulsar {
class HTTPWrapper;

enum HTTPMethod {HTTP_GET, HTTP_POST, HTTP_HEAD, HTTP_PUT, HTTP_DELETE, HTTP_OPTIONS, HTTP_CONNECTION};

struct HTTPWrapperResponse {
    std::vector<std::string> headers;
    std::string response;
    std::string statusLine;
};

typedef boost::shared_ptr<HTTPWrapper> HTTPWrapperPtr;
typedef boost::function<void(boost::system::error_code er)> HTTPWrapperCallback;

class HTTPWrapper : public boost::enable_shared_from_this<HTTPWrapper> {
public:
    HTTPWrapper(ExecutorServiceProviderPtr executorService);
    void createRequest(Url serverUrl ,HTTPMethod method, std::string HTTPVersion, std::string path,
                       std::vector<std::string> headers, std::string content, HTTPWrapperCallback callback);
    HTTPWrapperResponse getResponse();
    static std::string getHTTPMethodName(HTTPMethod method);
private:
    TcpResolverPtr resolverPtr_;
    ReadStreamPtr requestStreamPtr_;
    ReadStreamPtr responseHeaderStreamPtr_;
    ReadStreamPtr responseContentStreamPtr_;
    HTTPWrapperCallback callback_;
    SocketPtr socketPtr_;

    void handle_resolve(const boost::system::error_code &err,
                        boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

    void handle_connect(const boost::system::error_code &err,
                        boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

    void handle_write_request(const boost::system::error_code &err);

    void handle_read_status_line(const boost::system::error_code &err);

    void handle_read_headers(const boost::system::error_code &err);

    void handle_read_content(const boost::system::error_code &err);
};
}

#endif //PULSAR_CPP_HTTPWRAPPER_H
