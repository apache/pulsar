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

#include "HTTPWrapper.h"

DECLARE_LOG_OBJECT()

namespace pulsar {
    using boost::asio::ip::tcp;

    HTTPWrapper::HTTPWrapper(ExecutorServiceProviderPtr executorServiceProviderPtr) :
        resolverPtr_(executorServiceProviderPtr->get()->createTcpResolver()),
        requestStreamPtr_(executorServiceProviderPtr->get()->createReadStream()),
        responseHeaderStreamPtr_(executorServiceProviderPtr->get()->createReadStream()),
        responseContentStreamPtr_(executorServiceProviderPtr->get()->createReadStream()),
        socketPtr_(executorServiceProviderPtr->get()->createSocket()) {
    }

    std::string HTTPWrapper::getHTTPMethodName(HTTPMethod method) {
        switch(method) {
            case HTTP_GET:
                return "GET";
            case HTTP_POST:
                return "POST";
            case HTTP_HEAD:
                return "HEAD";
            case HTTP_PUT:
                return "PUT";
            case HTTP_DELETE:
                return "DELETE";
            case HTTP_OPTIONS:
                return "OPTIONS";
            case HTTP_CONNECTION:
                return "CONNECTION";
        }
    }


    void HTTPWrapper::createRequest(Url serverUrl,
                                    HTTPMethod method, std::string HTTPVersion, std::string path,
                                    std::vector<std::string> headers, std::string content,
                                    HTTPWrapperCallback callback) {
        callback_ = callback;
        std::ostream request_stream(requestStreamPtr_.get());
        request_stream << getHTTPMethodName(method) << " " << path << " HTTP/" << HTTPVersion << "\r\n";
        std::vector<std::string>::iterator iter = headers.begin();
        while (iter != headers.end()) {
            request_stream << *iter << "\r\n";
            iter++;
        }
        request_stream << content << "\r\n";
        tcp::resolver::query query(serverUrl.host(), boost::lexical_cast<std::string>(serverUrl.port()));
        LOG_DEBUG("JAI 2");
        resolverPtr_->async_resolve(query,
                                   boost::bind(&HTTPWrapper::handle_resolve, this,
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::iterator));
        LOG_DEBUG("JAI 3");
    }


    void HTTPWrapper::handle_resolve(const boost::system::error_code &err,
                                     tcp::resolver::iterator endpoint_iterator) {
        if (!err) {
            // Attempt a connection to the first endpoint in the list. Each endpoint
            // will be tried until we successfully establish a connection.
            tcp::endpoint endpoint = *endpoint_iterator;
            LOG_DEBUG("JAI 4");
            socketPtr_->async_connect(endpoint,
                                     boost::bind(&HTTPWrapper::handle_connect, this,
                                                 boost::asio::placeholders::error, ++endpoint_iterator));
            LOG_DEBUG("JAI 5");
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    void HTTPWrapper::handle_connect(const boost::system::error_code &err, tcp::resolver::iterator endpoint_iterator) {
        if (!err) {
            // The connection was successful. Send the request.
            boost::asio::async_write(*socketPtr_, *requestStreamPtr_,
                                     boost::bind(&HTTPWrapper::handle_write_request, this,
                                                 boost::asio::placeholders::error));
        } else if (endpoint_iterator != tcp::resolver::iterator()) {
            // The connection failed. Try the next endpoint in the list.
            LOG_DEBUG("JAI 7");
            socketPtr_->close();
            tcp::endpoint endpoint = *endpoint_iterator;
            socketPtr_->async_connect(endpoint,
                                     boost::bind(&HTTPWrapper::handle_connect, this,
                                                 boost::asio::placeholders::error, ++endpoint_iterator));
            LOG_DEBUG("JAI 8");
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    void HTTPWrapper::handle_write_request(const boost::system::error_code &err) {
        if (!err) {
            // Read the response status line.
            boost::asio::async_read_until(*socketPtr_, *responseHeaderStreamPtr_, "\r\n",
                                          boost::bind(&HTTPWrapper::handle_read_status_line, this,
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    void HTTPWrapper::handle_read_status_line(const boost::system::error_code &err) {
//        // Check that response is OK.
//        std::istream response_stream(responseHeaderStreamPtr_.get());
//        std::string http_version;
//        response_stream >> http_version;
//        unsigned int status_code;
//        response_stream >> status_code;
//        std::string status_message;
//        std::getline(response_stream, status_message);
//
//        if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
//            LOG_ERROR("Invalid response");
//            callback_(err);
//            return;
//        }
//        if (status_code != 200) {
//            LOG_ERROR("Response returned with status code " << status_code);
//            callback_(err);
//            return;
//        }
        if (!err) {
            // Read the response headers, which are terminated by a blank line.
            boost::asio::async_read_until(*socketPtr_, *responseHeaderStreamPtr_, "\r\n\r\n",
                                          boost::bind(&HTTPWrapper::handle_read_headers, this,
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    void HTTPWrapper::handle_read_headers(const boost::system::error_code &err) {
        if (!err) {
            // Process the response headers.
//            std::istream response_stream(responseHeaderStreamPtr_.get());
//            std::string header;
//            while (std::getline(response_stream, header) && header != "\r") {
//                LOG_DEBUG(header);
//            }

//            // Discard the remaining junk characters
//            if (responseHeaderStreamPtr_.get()->size() > 0) {
//                // std::cout<<"\'"<<responseHeaderStreamPtr_.get()<<"\'"<<std::endl;
//                responseHeaderStreamPtr_.get()->consume(responseHeaderStreamPtr_.get()->size());
//            }

            // Start reading remaining data until EOF.
            boost::asio::async_read(*socketPtr_, *responseContentStreamPtr_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPWrapper::handle_read_content, this,
                                                boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    void HTTPWrapper::handle_read_content(const boost::system::error_code &err) {
        if (!err) {
            boost::asio::async_read(*socketPtr_, *responseContentStreamPtr_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPWrapper::handle_read_content, this,
                                                boost::asio::placeholders::error));
        } else if (err == boost::asio::error::eof) {
            callback_(err);
        } else {
            LOG_ERROR(err.message());
            callback_(err);
        }
    }

    HTTPWrapperResponse HTTPWrapper::getResponse() {
        std::string data;
        std::istream header(responseHeaderStreamPtr_.get());
        header >> data;
        LOG_ERROR("Header is "<<data);
        std::istream aheader(responseHeaderStreamPtr_.get());
        aheader >> data;
        LOG_ERROR("Again Header is "<<data);

        while (std::getline(header, data)) {
            LOG_DEBUG(data);
        }

        data = "";
        std::istream content(responseContentStreamPtr_.get());
        content >> data;

        LOG_ERROR("COnetent is "<<data);

        std::istream acontent(responseContentStreamPtr_.get());
        acontent >> data;
        LOG_ERROR("Again COnetent is "<<data);


        while (std::getline(content, data)) {
            LOG_DEBUG(data);
        }


    }


}