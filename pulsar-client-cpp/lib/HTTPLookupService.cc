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
#include <curl/curl.h>

DECLARE_LOG_OBJECT()

namespace pulsar {
    const static std::string V2_PATH = "/lookup/v2/destination/";
    const static std::string PARTITION_PATH = "/admin/persistent/";
    const static int MAX_HTTP_REDIRECTS = 20;
    const static std::string PARTITION_METHOD_NAME = "partitions";
    const static int NUMBER_OF_LOOKUP_THREADS = 4;

    HTTPLookupService::HTTPLookupService(const std::string &lookupUrl,
            const ClientConfiguration &clientConfiguration,
            const AuthenticationPtr &authData)
    : executorProvider_(boost::make_shared<ExecutorServiceProvider>(NUMBER_OF_LOOKUP_THREADS)),
    authenticationPtr_(authData),
    lookupTimeoutInSeconds_(clientConfiguration.getOperationTimeoutSeconds()) {
        if (lookupUrl[lookupUrl.length() - 1] == '/') {
            // Remove trailing '/'
            adminUrl_ = lookupUrl.substr(0, lookupUrl.length() - 1);
        } else {
            adminUrl_ = lookupUrl;
        }

        // Once per application - https://curl.haxx.se/mail/lib-2015-11/0052.html
        curl_global_init(CURL_GLOBAL_ALL);
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::lookupAsync(const std::string &destinationName) {
        LookupPromise promise;
        boost::shared_ptr<DestinationName> dn = DestinationName::get(destinationName);
        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            promise.setFailed(ResultInvalidTopicName);
            return promise.getFuture();
        }

        std::stringstream completeUrlStream;
        completeUrlStream << adminUrl_ << V2_PATH << "persistent/" << dn->getProperty() << '/' << dn->getCluster()
        << '/' << dn->getNamespacePortion() << '/' << dn->getEncodedLocalName();
        executorProvider_->get()->postWork(boost::bind(&HTTPLookupService::sendHTTPRequest, this, promise, completeUrlStream.str(), Lookup));
        return promise.getFuture();
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(const DestinationNamePtr &dn) {
        LookupPromise promise;
        std::stringstream completeUrlStream;
        completeUrlStream << adminUrl_ << PARTITION_PATH << dn->getProperty() << '/' << dn->getCluster()
        << '/' << dn->getNamespacePortion() << '/' << dn->getEncodedLocalName() << '/'
        << PARTITION_METHOD_NAME;
        executorProvider_->get()->postWork(boost::bind(&HTTPLookupService::sendHTTPRequest, this, promise, completeUrlStream.str(), PartitionMetaData));
        return promise.getFuture();
    }

    static size_t curlWriteCallback(void *contents, size_t size, size_t nmemb, void *responseDataPtr) {
        ((std::string*)responseDataPtr)->append((char*)contents, size * nmemb);
        return size * nmemb;
    }

    void HTTPLookupService::sendHTTPRequest(LookupPromise promise, const std::string completeUrl,
            RequestType requestType) {
        CURL *handle;
        CURLcode res;
        std::string responseData;

        handle = curl_easy_init();

        if(!handle) {
            LOG_ERROR("Unable to curl_easy_init for url " << completeUrl);
            promise.setFailed(ResultLookupError);
            // No curl_easy_cleanup required since handle not initialized
            return;
        }
        // set URL
        curl_easy_setopt(handle, CURLOPT_URL, completeUrl.c_str());

        // Write callback
        curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
        curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

        // New connection is made for each call
        curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
        curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

        // Timer
        curl_easy_setopt(handle, CURLOPT_TIMEOUT, lookupTimeoutInSeconds_);

        // Redirects
        curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(handle, CURLOPT_MAXREDIRS, MAX_HTTP_REDIRECTS);

        // Fail if HTTP return code >=400
        curl_easy_setopt(handle, CURLOPT_FAILONERROR, 1L);

        // Authorization data
        struct curl_slist *list = NULL;

        // TODO - Need to think more about other Authorization methods
        AuthenticationDataPtr authDataContent;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication methods should have AuthenticationData and return true on getAuthData for url " << completeUrl);
            promise.setFailed(authResult);
        }
        if (authDataContent->hasDataFromCommand()) {
            // TODO - remove YCA mention from OSS and understand how other auth methods will work with this
            const std::string authHeader = "Yahoo-App-Auth: " + authDataContent->getCommandData();
            list = curl_slist_append(list, authHeader.c_str());
        }

        // Make get call to server
        res = curl_easy_perform(handle);

        // Free header list
        curl_slist_free_all(list);

        // TODO - check other return condition
        switch(res) {
            case CURLE_OK:
                LOG_DEBUG("Response received successfully for url " << completeUrl);
                promise.setValue((requestType == PartitionMetaData) ? parsePartitionData(responseData) : parseLookupData(responseData));
                break;
            case CURLE_COULDNT_CONNECT:
            case CURLE_COULDNT_RESOLVE_PROXY:
            case CURLE_COULDNT_RESOLVE_HOST:
            case CURLE_HTTP_RETURNED_ERROR:
                LOG_ERROR("Response failed for url "<<completeUrl << ". Error Code "<<res);
                promise.setFailed(ResultConnectError);
                break;
            case CURLE_READ_ERROR:
                LOG_ERROR("Response failed for url "<<completeUrl << ". Error Code "<<res);
                promise.setFailed(ResultReadError);
                break;
            case CURLE_OPERATION_TIMEDOUT:
                LOG_ERROR("Response failed for url "<<completeUrl << ". Error Code "<<res);
                promise.setFailed(ResultTimeout);
                break;
            default:
                LOG_ERROR("Response failed for url "<<completeUrl << ". Error Code "<<res);
                promise.setFailed(ResultLookupError);
                break;
        }
        curl_easy_cleanup(handle);
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
        const std::string defaultNotFoundString = "Url Not found";
        const std::string brokerUrl = root.get("brokerUrl", defaultNotFoundString).asString();
        if (brokerUrl == defaultNotFoundString) {
            LOG_ERROR("malformed json! - brokerUrl not present" << json);
            return LookupDataResultPtr();
        }

        const std::string brokerUrlSsl = root.get("brokerUrlSsl", defaultNotFoundString).asString();
        if (brokerUrlSsl == defaultNotFoundString) {
            LOG_ERROR("malformed json! - brokerUrlSsl not present" << json);
            return LookupDataResultPtr();
        }

        LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
        lookupDataResultPtr->setBrokerUrl(brokerUrl);
        lookupDataResultPtr->setBrokerUrlSsl(brokerUrlSsl);
        return lookupDataResultPtr;
    }

    HTTPLookupService::~HTTPLookupService() {
        curl_global_cleanup();
    }
}
