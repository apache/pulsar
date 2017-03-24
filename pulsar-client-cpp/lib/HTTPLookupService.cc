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
#include <lib/HTTPLookupService.h>

DECLARE_LOG_OBJECT()

namespace pulsar {
    using boost::posix_time::seconds;
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
//        std::ostream request_stream(requestStreamPtr.get());
//        request_stream << "GET " << V2_PATH << "persistent" << SEPARATOR << dn->getProperty() << SEPARATOR << dn->getCluster()
//                       << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << " HTTP/1.1\r\n";

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

        // TODO - setup deadline timer
        HTTPWrapperPtr wrapperPtr = boost::make_shared<HTTPWrapper>(executorProvider_);
        std::stringstream requestStream;
        requestStream << PARTITION_PATH << dn->getProperty() << SEPARATOR << dn->getCluster()
                      << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << SEPARATOR
                      << PARTITION_METHOD_NAME;

        std::vector<std::string> headers;
        headers.push_back("Host: " + adminUrl_.host());
        headers.push_back("Accept: */*");
        // TODO - set authentication headers
        headers.push_back("Connection: close");
        LOG_DEBUG("JAI 1");
        wrapperPtr->createRequest(adminUrl_, HTTP_GET, "1.1", requestStream.str(),
                headers, "",
                boost::bind(&HTTPLookupService::callback, _1, wrapperPtr, promise));
        usleep(10 * 1000 * 1000);
        LOG_DEBUG("AdminUrl = " << adminUrl_);
        return promise.getFuture();
    }

    LookupDataResultPtr HTTPLookupService::parsePartitionData(std::string& json) {
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

    LookupDataResultPtr HTTPLookupService::parseLookupData(std::string& json) {
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


    void HTTPLookupService::callback(boost::system::error_code er, HTTPWrapperPtr wrapperPtr,
                                     Promise<Result, LookupDataResultPtr> promise) {
        LOG_ERROR("Callback called with data");
        // LOG_ERROR(wrapper.getResponseContent());
        // wrapperPtr->getResponse();
        return;
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
