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
        LookupPromise promise;
        boost::shared_ptr<DestinationName> dn = DestinationName::get(destinationName);

        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            promise.setFailed(ResultInvalidTopicName);
            return promise.getFuture();
        }

        std::stringstream requestStream;
        requestStream << V2_PATH << "persistent" << SEPARATOR << dn->getProperty() << SEPARATOR << dn->getCluster()
                      << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName();
        sendHTTPRequest(promise, requestStream, Lookup);
        return promise.getFuture();

    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(const DestinationNamePtr &dn) {
        LookupPromise promise;
        std::stringstream requestStream;
        requestStream << PARTITION_PATH << dn->getProperty() << SEPARATOR << dn->getCluster()
                      << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << SEPARATOR
                      << PARTITION_METHOD_NAME;
        sendHTTPRequest(promise, requestStream, PartitionMetaData);
        return promise.getFuture();
    }

    void HTTPLookupService::sendHTTPRequest(LookupPromise promise, std::stringstream& requestStream,
                                            RequestType requestType) {
        AuthenticationDataPtr authDataContent;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication methods should have AuthenticationData");
            promise.setFailed(authResult);
        }

        // Need to think more about other Authorization methods
        std::string authorizationData = "";
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }

        DeadlineTimerPtr timerPtr = startTimer(promise);

        HTTPWrapper::Request request;
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
                                   boost::bind(&HTTPLookupService::callback, _1, promise, requestType, timerPtr));
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

        if (!root.isMember("brokerUrl") || root.isMember("defaultBrokerUrlSsl")) {
            LOG_ERROR("malformed json! " << json);
            return LookupDataResultPtr();
        }

        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setBrokerUrl(root.get("brokerUrl", "").asString());
        lookupDataResultPtr->setBrokerUrlSsl(root.get("brokerUrlSsl", "").asString());
        return lookupDataResultPtr;
    }


    void HTTPLookupService::callback(HTTPWrapperPtr httpWrapperPtr,
                                     Promise<Result, LookupDataResultPtr> promise, RequestType requestType,
                                     DeadlineTimerPtr timer) {
        const HTTPWrapper::Response& response = httpWrapperPtr->getResponse();
        LOG_DEBUG("HTTPLookupService::callback response = " << response);
        if (response.retCode != HTTPWrapper::Response::Success) {
            LOG_ERROR("HTTPLookupService::callback failed " << response.errCode.message());
            promise.setFailed(ResultLookupError);
            return;
        }
        if (response.retCode == HTTPWrapper::Response::Timeout) {
            LOG_ERROR("Ignoring HTTPLookupService::callback since timer expired");
            promise.setFailed(ResultTimeout);
            return;
        }
        // TODO - handle redirects
//        redirect = new URI(String.format("%s%s%s?authoritative=%s", redirectUrl, "/lookup/v2/destination/",
//                                         topic.getLookupName(), newAuthoritative));
        const std::string &content = response.content;
        promise.setValue((requestType == PartitionMetaData) ? parsePartitionData(content) : parseLookupData(content));
        timer->cancel();
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
