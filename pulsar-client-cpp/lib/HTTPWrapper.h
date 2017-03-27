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
    
struct HTTPWrapperResponse {
    enum RetCode { Success, SendFailure, Timeout, ResponseFailure, UnknownError};
    HTTPWrapperResponse();
    std::string HTTPVersion;
    uint64_t statusCode;
    std::string statusMessage;
    std::vector<std::string> headers;
    std::string content;

    RetCode retCode;
    std::string retMessage;
    boost::system::error_code errCode;

    bool failed() const {
        return (retCode != Success);
    }
    friend std::ostream & operator<<(std::ostream&, const HTTPWrapperResponse&);
};

typedef boost::shared_ptr<HTTPWrapper> HTTPWrapperPtr;
typedef boost::function<void(const boost::system::error_code&, const HTTPWrapperResponse&)> HTTPWrapperCallback;

class HTTPWrapper : public boost::enable_shared_from_this<HTTPWrapper> {
public:
    enum Method {GET, POST, HEAD, PUT, DELETE, OPTIONS, CONNECTION};
    static void createRequest(ExecutorServiceProviderPtr, Url&, HTTPWrapper::Method&, std::string&, std::string&,
                       std::vector<std::string>&, std::string&, HTTPWrapperCallback);
    static std::string getHTTPMethodName(HTTPWrapper::Method&);
protected:
    HTTPWrapper(ExecutorServiceProviderPtr, HTTPWrapperCallback);
private:
    TcpResolverPtr resolverPtr_;
    ReadStreamPtr requestStreamPtr_;
    ReadStreamPtr responseStreamPtr_;
    HTTPWrapperCallback callback_;
    SocketPtr socketPtr_;
    HTTPWrapperResponse response_;

    void handle_resolve(const boost::system::error_code&,
                        boost::asio::ip::tcp::resolver::iterator);

    void handle_connect(const boost::system::error_code&,
                        boost::asio::ip::tcp::resolver::iterator);

    void handle_write_request(const boost::system::error_code&);

    void handle_read_status_line(const boost::system::error_code&);

    void handle_read_headers(const boost::system::error_code&);

    void handle_read_content(const boost::system::error_code&);
    void createRequest(Url&, HTTPWrapper::Method&, std::string&, std::string&,
                              std::vector<std::string>&, std::string&);
};
}

#endif //PULSAR_CPP_HTTPWRAPPER_H
