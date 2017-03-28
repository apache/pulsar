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

typedef boost::shared_ptr<HTTPWrapper> HTTPWrapperPtr;
class HTTPWrapper : public boost::enable_shared_from_this<HTTPWrapper> {
public:
    struct Response {
        enum RetCode { Success, SendFailure, ResponseFailure, ResolveError, ConnectError};
        std::string HTTPVersion;
        uint64_t statusCode;
        std::string statusMessage;
        std::vector<std::string> headers;
        std::string content;
        RetCode retCode;
        boost::system::error_code errCode;
        friend std::ostream & operator<<(std::ostream&, const Response&);
    };
    typedef boost::function<void(HTTPWrapperPtr)> HTTPWrapperCallback;

    struct Request {
        enum Method {GET, POST, HEAD, PUT, DELETE, OPTIONS, CONNECTION};
        Url serverUrl;
        Method method;
        std::string version;
        std::string path;
        std::vector<std::string> headers;
        std::string content;
        friend std::ostream & operator<<(std::ostream&, const Request&);
    };

    static void createRequest(ExecutorServiceProviderPtr, Request&, HTTPWrapperCallback);
    static std::string getHTTPMethodName(const Request::Method&);
    inline const Response& getResponse() const {
        return response_;
    }
    inline Request& getMutableRequest() {
        return request_;
    }
    void createRequest(Request& request, HTTPWrapperCallback);
protected:
    HTTPWrapper(ExecutorServiceProviderPtr);
private:
    TcpResolverPtr resolverPtr_;
    ReadStreamPtr requestStreamPtr_;
    ReadStreamPtr responseStreamPtr_;
    HTTPWrapperCallback callback_;
    SocketPtr socketPtr_;
    Response response_;
    Request request_;

    void handle_resolve(const boost::system::error_code&,
                        boost::asio::ip::tcp::resolver::iterator);

    void handle_connect(const boost::system::error_code&,
                        boost::asio::ip::tcp::resolver::iterator);

    void handle_write_request(const boost::system::error_code&);

    void handle_read_status_line(const boost::system::error_code&);

    void handle_read_headers(const boost::system::error_code&);

    void handle_read_content(const boost::system::error_code&);
};
}

#endif //PULSAR_CPP_HTTPWRAPPER_H
