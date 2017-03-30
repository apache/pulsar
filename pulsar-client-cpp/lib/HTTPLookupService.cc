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
        LookupPromise promise;
        boost::shared_ptr<DestinationName> dn = DestinationName::get(destinationName);

        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            promise.setFailed(ResultInvalidTopicName);
            return promise.getFuture();
        }

        std::stringstream requestStream;
        requestStream << V2_PATH << "persistent/" << dn->getProperty() << '/' << dn->getCluster()
                      << '/' << dn->getNamespacePortion() << '/' << dn->getEncodedLocalName();
        sendHTTPRequest(promise, requestStream, Lookup);
        return promise.getFuture();

    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(const DestinationNamePtr &dn) {
        LookupPromise promise;
        std::stringstream requestStream;
        requestStream << PARTITION_PATH << dn->getProperty() << '/' << dn->getCluster()
                      << '/' << dn->getNamespacePortion() << '/' << dn->getEncodedLocalName() << '/'
                      << PARTITION_METHOD_NAME;
        sendHTTPRequest(promise, requestStream, PartitionMetaData);
        return promise.getFuture();
    }

    void HTTPLookupService::sendHTTPRequest(LookupPromise promise, std::stringstream& requestStream,
                                            RequestType requestType) {
        DeadlineTimerPtr timerPtr = startTimer(promise);

        HTTPWrapper::Request request;

        // Need to think more about other Authorization methods
        std::string authorizationData = "";
        AuthenticationDataPtr authDataContent;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication methods should have AuthenticationData");
            promise.setFailed(authResult);
        }
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }
        // TODO - remove YCA mention from OSS and understand how other auth methods will work with this
        if (authorizationData != "") {
            request.headers.push_back("Yahoo-App-Auth: " + authorizationData);
        }
        request.headers.push_back("Accept: */*");
        request.headers.push_back("Connection: close");
        request.method = HTTPWrapper::Request::GET;
        request.version = "1.1";
        request.content = "";
        request.path = requestStream.str();
        request.serverUrl = adminUrl_;
        HTTPWrapper::createRequest(executorProvider_, request,
                                   boost::bind(&HTTPLookupService::callback, _1, promise, requestType, timerPtr,
                                               request.headers, MAX_HTTP_REDIRECTS));
    }

    LookupDataResultPtr HTTPLookupService::parsePartitionData(const std::string &json) {
        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(json, root, false)) {
            LOG_ERROR("Failed to parse json of Partition Metadata: " << reader.getFormatedErrorMessages()
                                                                     << "\nInput Json = " << json);
            return LookupDataResultPtr();
        }
        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setPartitions(root.get("partitions", 0).asInt());
        return lookupDataResultPtr;
    }

    LookupDataResultPtr HTTPLookupService::parseLookupData(const std::string &json) {
        Json::Value root;
        Json::Reader reader;
        if (!reader.parse(json, root, false)) {
            LOG_ERROR("Failed to parse json : " << reader.getFormatedErrorMessages()
                                                << "\nInput Json = " << json);
            return LookupDataResultPtr();
        }

        static const char brokerUrlStr[] = "brokerUrl";
        const Json::Value* brokerUrl = root.find(brokerUrlStr, brokerUrlStr + sizeof(brokerUrlStr) - 1);
        if (!brokerUrl || !brokerUrl->isString()) {
            LOG_ERROR("malformed json! " << json);
            return LookupDataResultPtr();
        }

        static const char brokerUrlSslStr[] = "brokerUrlSsl";
        const Json::Value* brokerUrlSsl = root.find(brokerUrlSslStr, brokerUrlSslStr + sizeof(brokerUrlSslStr) - 1);
        if (!brokerUrlSsl || !brokerUrlSsl->isString()) {
            LOG_ERROR("malformed json! " << json);
            return LookupDataResultPtr();
        }

        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setBrokerUrl(brokerUrl->asString());
        lookupDataResultPtr->setBrokerUrlSsl(brokerUrlSsl->asString());
        return lookupDataResultPtr;
    }


    void HTTPLookupService::callback(HTTPWrapperPtr httpWrapperPtr,
                                     Promise<Result, LookupDataResultPtr> promise, RequestType requestType,
                                     DeadlineTimerPtr timerPtr, std::vector<std::string> headers,
                                     int numberOfRedirects) {
        if (promise.isComplete()) {
            // Timer expired
            LOG_DEBUG("Promise already fulfilled");
            timerPtr->cancel();
            return;
        }
        const HTTPWrapper::Response &response = httpWrapperPtr->getResponse();
        LOG_DEBUG("HTTPLookupService::callback response = " << response);
        if (response.retCode != HTTPWrapper::Response::Success) {
            if (response.statusCode == 401) {
                // 401 means unauthenticated
                LOG_ERROR("Authentication failed");
                promise.setFailed(ResultConnectError);
                return;
            }
            LOG_ERROR("HTTPLookupService::callback failed - asio errCode = " << response.errCode.message());
            promise.setFailed(ResultLookupError);
            timerPtr->cancel();
            return;
        }
        if (response.statusCode == 307 || response.statusCode == 308) {
            LOG_DEBUG("Redirect response received");
            if (numberOfRedirects <= 0) {
                LOG_DEBUG("Max redirect limit reached");
                promise.setFailed(ResultLookupError);
                timerPtr->cancel();
                return;
            }
            const std::vector<std::string>& responseHeaders = response.headers;
            std::vector<std::string>::const_iterator iter = responseHeaders.begin();
            while (iter != responseHeaders.end()) {
                if ((*iter).compare(0, 8, "Location") == 0) {
                    // Match found
                    Url url;
                    std::string redirectString = (*iter).substr(10);
                    if (!Url::parse(redirectString, url)) {
                        LOG_ERROR("Failed to parse url: " << redirectString);
                        promise.setFailed(ResultLookupError);
                        return;
                    }

                    HTTPWrapper::Request& request = httpWrapperPtr->getMutableRequest();
                    request.path = url.path() + url.file() + url.parameter();
                    request.serverUrl = url;
                    httpWrapperPtr->createRequest(request, boost::bind(&HTTPLookupService::callback, _1, promise, requestType, timerPtr,
                                                                       headers, numberOfRedirects - 1));
                    return;
                }
                iter++;
            }
            return;
        }
        timerPtr->cancel(); // Take care
        const std::string &content = response.content;
        promise.setValue((requestType == PartitionMetaData) ? parsePartitionData(content) : parseLookupData(content));

    }


    DeadlineTimerPtr HTTPLookupService::startTimer(LookupPromise promise) {
        DeadlineTimerPtr timer = executorProvider_->get()->createDeadlineTimer();
        timer->expires_from_now(lookupTimeout_);
        timer->async_wait(boost::bind(&HTTPLookupService::handleTimeout, this, _1, promise));
        return timer;
    }

    void HTTPLookupService::handleTimeout(const boost::system::error_code &ec,LookupPromise promise) {
        if (ec) {
            LOG_DEBUG(" Ignoring timer on cancelled event, code[" << ec << "]");
            return;
        }
        LOG_DEBUG("Timeout occured");
        // Timer has reached the given timeout
        promise.setFailed(ResultTimeout);
    }
}
