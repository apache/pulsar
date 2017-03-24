#include <lib/HTTPLookupService.h>

DECLARE_LOG_OBJECT()

namespace pulsar {
    using boost::posix_time::seconds;
    using boost::asio::ip::tcp;

    const static std::string V2_PATH = "/lookup/v2/destination/";
    const static std::string PARTITION_PATH = "/admin/persistent/";
    const static int MAX_HTTP_REDIRECTS = 20;
    const static char SEPARATOR = '/';
    const static std::string PARTITION_METHOD_NAME = "partitions";

    HTTPLookupService::HTTPLookupService(const std::string &lookupUrl,
                                         const ClientConfiguration &clientConfiguration,
                                         ExecutorServiceProviderPtr executorProvider,
                                         const AuthenticationPtr &authData)
            : executorProvider_(executorProvider),
              authenticationPtr_(authData),
              lookupTimeout_(seconds(clientConfiguration.getOperationTimeoutSeconds())) {
        if (!Url::parse(lookupUrl, adminUrl_)) {
            throw "Exception: Invalid service url";
        }
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::lookupAsync(const std::string &destinationName) {
        boost::shared_ptr<DestinationName> dn = DestinationName::get(destinationName);

        LookupPromise promise;
        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            promise.setFailed(ResultInvalidTopicName);
            return promise.getFuture();
        }

        std::string requestUrl = "";
        // TODO - remove this - adminUrl_ + V2_PATH + dn->getLookupName();
        LOG_DEBUG("Doing topic lookup on: " << requestUrl);

        // LookupDataPromisePtr promise = boost::make_shared<LookupDataPromise>();
        // cmsAdminGetAsync(requestUrl, promise, PartitionMetadataPromisePtr());
        // return promise->getFuture();

        AuthenticationDataPtr authDataContent;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication method should have AuthenticationData");
            promise.setFailed(authResult);
            return promise.getFuture();
        }

        std::string authorizationData;
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }


        return promise.getFuture();
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(const DestinationNamePtr &dn) {
        // PartitionMetadataPromisePtr promise = boost::make_shared<PartitionMetadataPromise>();
        // cmsAdminGetAsync(requestUrl.str(), LookupDataPromisePtr(), promise);
        // return promise->getFuture();

        AuthenticationDataPtr authDataContent;
        LookupPromise promise;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication methods should have AuthenticationData");
            promise.setFailed(authResult);
            return promise.getFuture();
        }

        // Need to think more about other Authorization methods
        std::string authorizationData;
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }

        // TODO - validate if the url is well formed
        // TODO - setup deadline timer
        ReadStreamPtr requestStreamPtr = executorProvider_->get()->createReadStream();
        std::ostream request_stream(requestStreamPtr.get());
        request_stream << "GET " << V2_PATH << "persistent" << SEPARATOR << dn->getProperty() << SEPARATOR << dn->getCluster()
                       << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << " HTTP/1.1\r\n";

        /*
        request_stream << "GET " << PARTITION_PATH << dn->getProperty() << SEPARATOR << dn->getCluster()
                       << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << SEPARATOR
                       << PARTITION_METHOD_NAME << " HTTP/1.1\r\n";
                       */
        request_stream << "Host: " << adminUrl_.host() << "\r\n";
        request_stream << "Accept: */*\r\n";
        request_stream << "Connection: close\r\n\r\n";

        LOG_DEBUG("Getting Partition Metadata from : " << request_stream);
        LOG_DEBUG("AdminUrl = " << adminUrl_);
        // TODO - put this resolver as a part of the class
        tcp::resolver::query query(adminUrl_.host(), boost::lexical_cast<std::string>(adminUrl_.port()));
        TcpResolverPtr resolverPtr = executorProvider_->get()->createTcpResolver();
        resolverPtr->async_resolve(query,
                                   boost::bind(&HTTPLookupService::handle_resolve, this, requestStreamPtr, promise,
                                               resolverPtr,
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::iterator));

        return promise.getFuture();
    }

    void HTTPLookupService::handle_resolve(ReadStreamPtr requestStreamPtr, LookupPromise promise,
                                           TcpResolverPtr resolverPtr,
                                           const boost::system::error_code &err,
                                           tcp::resolver::iterator endpoint_iterator) {
        SocketPtr socketPtr = executorProvider_->get()->createSocket();
        if (!err) {
            // Attempt a connection to the first endpoint in the list. Each endpoint
            // will be tried until we successfully establish a connection.
            tcp::endpoint endpoint = *endpoint_iterator;
            socketPtr->async_connect(endpoint,
                                     boost::bind(&HTTPLookupService::handle_connect, this, requestStreamPtr, promise,
                                                 socketPtr, boost::asio::placeholders::error, ++endpoint_iterator));
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    void HTTPLookupService::handle_connect(ReadStreamPtr requestStreamPtr, LookupPromise promise, SocketPtr socketPtr,
                                           const boost::system::error_code &err,
                                           tcp::resolver::iterator endpoint_iterator) {
        if (!err) {
            // The connection was successful. Send the request.
            boost::asio::async_write(*socketPtr, *requestStreamPtr,
                                     boost::bind(&HTTPLookupService::handle_write_request, this, promise, socketPtr,
                                                 boost::asio::placeholders::error));
        } else if (endpoint_iterator != tcp::resolver::iterator()) {
            // The connection failed. Try the next endpoint in the list.
            socketPtr->close();
            tcp::endpoint endpoint = *endpoint_iterator;
            socketPtr->async_connect(endpoint,
                                     boost::bind(&HTTPLookupService::handle_connect, this, requestStreamPtr, promise,
                                                 socketPtr, boost::asio::placeholders::error, ++endpoint_iterator));
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    void HTTPLookupService::handle_write_request(LookupPromise promise, SocketPtr socketPtr,
                                                 const boost::system::error_code &err) {
        if (!err) {
            ReadStreamPtr responseStreamPtr = executorProvider_->get()->createReadStream();
            // Read the response status line.
            boost::asio::async_read_until(*socketPtr, *responseStreamPtr, "\r\n",
                                          boost::bind(&HTTPLookupService::handle_read_status_line, this,
                                                      responseStreamPtr, promise, socketPtr,
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    void HTTPLookupService::handle_read_status_line(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                                    SocketPtr socketPtr, const boost::system::error_code &err) {
        if (!err) {
            // Check that response is OK.
            std::istream response_stream(responseStreamPtr.get());
            std::string http_version;
            response_stream >> http_version;
            unsigned int status_code;
            response_stream >> status_code;
            std::string status_message;
            std::getline(response_stream, status_message);
            if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
                LOG_ERROR("Invalid response");
                promise.setFailed(ResultLookupError);
                return;
            }
            if (status_code != 200) {
                LOG_ERROR("Response returned with status code " << status_code);
                promise.setFailed(ResultLookupError);;
                return;
            }

            // Read the response headers, which are terminated by a blank line.
            boost::asio::async_read_until(*socketPtr, *responseStreamPtr, "\r\n\r\n",
                                          boost::bind(&HTTPLookupService::handle_read_headers, this,
                                                      responseStreamPtr, promise, socketPtr,
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    void HTTPLookupService::handle_read_headers(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                                SocketPtr socketPtr, const boost::system::error_code &err) {
        if (!err) {
            // Process the response headers.
            std::istream response_stream(responseStreamPtr.get());
            std::string header;
            while (std::getline(response_stream, header) && header != "\r") {
                LOG_DEBUG(header);
            }

            // Discard the remaining junk characters
            if (responseStreamPtr.get()->size() > 0) {
                std::cout<<"\'"<<responseStreamPtr.get()<<"\'"<<std::endl;
                responseStreamPtr.get()->consume(responseStreamPtr.get()->size());
            }

            // Start reading remaining data until EOF.
            boost::asio::async_read(*socketPtr, *responseStreamPtr,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPLookupService::handle_read_content, this,
                                                responseStreamPtr, promise, socketPtr,
                                                boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    void HTTPLookupService::handle_read_content(ReadStreamPtr responseStreamPtr, LookupPromise promise,
                                                SocketPtr socketPtr,
                                                const boost::system::error_code &err) {
        if (!err) {
            boost::asio::async_read(*socketPtr, *responseStreamPtr,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPLookupService::handle_read_content, this,
                                                responseStreamPtr, promise, socketPtr,
                                                boost::asio::placeholders::error));
        } else if (err == boost::asio::error::eof) {
            // Reading data in the end together from responseStreamPtr since expecting only about 100 bytes in response
            LookupDataResultPtr lookupDataResultPtr = parsePartitionData(responseStreamPtr);
            if (lookupDataResultPtr) {
                promise.setValue(lookupDataResultPtr);
            } else {
                promise.setFailed(ResultLookupError);
            }
        } else {
            LOG_ERROR(err.message());
            promise.setFailed(ResultLookupError);
        }
    }

    LookupDataResultPtr HTTPLookupService::parsePartitionData(ReadStreamPtr streamPtr) {
        std::istream is(streamPtr.get());
        std::string json;
        is >> json;
        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(json, root, false)) {
            LOG_ERROR("Failed to parse json of Partition Metadata: " << reader.getFormatedErrorMessages());
            LOG_ERROR("JSON Response: " << json);
            return LookupDataResultPtr();
        }
        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setPartitions(root.get("partitions", 0).asInt());
        return lookupDataResultPtr;
    }

    static LookupDataResultPtr parseLookupData(ReadStreamPtr streamPtr) {
        std::istream is(streamPtr.get());
        std::string json;
        is >> json;

        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(json, root, false)) {
            LOG_ERROR("Failed to parse json : " << reader.getFormatedErrorMessages());
            return LookupDataResultPtr();
        }

        if (! root.isMember("brokerUrl") || root.isMember("defaultBrokerUrlSsl") ) {
            LOG_ERROR("malformed json! " << json);
            return LookupDataResultPtr();
        }

        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setBrokerUrl(root.get("brokerUrl", "").asString());
        lookupDataResultPtr->setBrokerUrlSsl(root.get("brokerUrlSsl", "").asString());
        return lookupDataResultPtr;
    }
}

////private
//    void HTTPLookupService::cmsAdminGetAsync(const std::string& requestUrl,
//                                         LookupDataPromisePtr lookupDataPromise,
//                                         PartitionMetadataPromisePtr partitionMetdataPromise) {
//        //check if the URL is valid or not if not return invalid address error
//        // asio::error_code err;
//        urdl::url service_url = urdl::url::from_string(requestUrl, err);
//        if (err) {
//            LOG_ERROR("Invalid Url, unable to parse - " << requestUrl << "error - " << err);
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultInvalidUrl);
//            }
//            if (partitionMetdataPromise) {
//                partitionMetdataPromise->setFailed(ResultInvalidUrl);
//            }
//            return;
//        }
//
//        AuthenticationDataPtr authDataContent;
//        Result authResult = authentication->getAuthData(authDataContent);
//        if (authResult != ResultOk) {
//            LOG_ERROR("All Authentication method should have AuthenticationData");
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(authResult);
//            }
//            if (partitionMetdataPromise) {
//                partitionMetdataPromise->setFailed(authResult);
//            }
//            return;
//        }
//
//        std::string authorizationData;
//        if (authDataContent->hasDataFromCommand()) {
//            authorizationData = authDataContent->getCommandData();
//        }
//
//        // Setup timer
//        DeadlineTimerPtr timer = executorProvider_->get()->createDeadlineTimer();
//        timer->expires_from_now(lookupTimeout_);
//        timer->async_wait(boost::bind(&HTTPLookupService::handleTimeout, this, _1, lookupDataPromise, partitionMetdataPromise));
//
//        ReadStreamPtr readStream = executorProvider_->get()->createReadStream();
//        urdl::option_set options;
//        options.set_option(urdl::http::max_redirects(MAX_HTTP_REDIRECTS));
//        options.set_option(urdl::http::yca_header(authorizationData));
//        readStream->set_options(options);
//        readStream->async_open(requestUrl,
//                               boost::bind(&HTTPLookupService::handleOpen,
//                                           this, _1, readStream, requestUrl,
//                                           lookupDataPromise, partitionMetdataPromise, timer));
//
//
//    }
//
///*
// * @param ec asio error code, non-zero means trouble
// * handler for async_open call
// */
//    void HTTPLookupService::handleOpen(const asio::error_code& ec, ReadStreamPtr readStream,
//                                   const std::string& requestUrl,
//                                   LookupDataPromisePtr lookupDataPromise,
//                                   PartitionMetadataPromisePtr partitionMetdataPromise, DeadlineTimerPtr timer) {
//        if (!ec) {
//            // composable call, only returns after reading content_length size data
//            size_t length = readStream->content_length();
//            SharedBuffer buffer = SharedBuffer::allocate(length);
//            asio::async_read(*readStream, buffer.asio_buffer(),
//                             boost::bind(&HTTPLookupService::handleRead,
//                                         this, _1, _2, buffer, lookupDataPromise, partitionMetdataPromise, timer));
//        } else {
//            LOG_ERROR("Unable to open URL - " << requestUrl << " : " << ec << " -- " << ec.message());
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultConnectError);
//            } else {
//                partitionMetdataPromise->setFailed(ResultConnectError);
//            }
//
//            timer->cancel();
//        }
//    }
//
///*
// * @param ec asio error_code, non-zero means trouble
// */
//    void HTTPLookupService::handleRead(const asio::error_code& ec, size_t bytesTransferred,
//                                   SharedBuffer buffer,
//                                   LookupDataPromisePtr lookupDataPromise,
//                                   PartitionMetadataPromisePtr partitionMetdataPromise, DeadlineTimerPtr timer) {
//        buffer.bytesWritten(bytesTransferred);
//
//        if (!ec) {
//            std::string response(buffer.data(), buffer.readableBytes());
//            if (lookupDataPromise) {
//                LOG_DEBUG("Lookup response : " << response);
//                lookupDataPromise->setValue(LookupData::parse(response));
//            } else {
//                LOG_DEBUG("Partition Metadata response : " << response);
//                partitionMetdataPromise->setValue(PartitionMetadata::parse(response));
//            }
//        } else {
//            LOG_ERROR("Error Reading data from the socket:  " << ec);
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultReadError);
//            } else {
//                partitionMetdataPromise->setFailed(ResultReadError);
//            }
//        }
//
//        timer->cancel();
//    }
//
//    void HTTPLookupService::handleTimeout(const asio::error_code& ec, LookupDataPromisePtr lookupDataPromise,
//                                      PartitionMetadataPromisePtr partitionMetdataPromise) {
//        if (!ec) {
//            // Timer has reached the given timeout
//            if (lookupDataPromise) {
//                if (lookupDataPromise->setFailed(ResultTimeout)) {
//                    LOG_WARN("Timeout during HTTP lookup operation");
//                }
//            } else {
//                if (partitionMetdataPromise->setFailed(ResultTimeout)) {
//                    LOG_WARN("Timeout while fetching partition metadata");
//                }
//            }
//        }
//    }
// }
