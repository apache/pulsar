/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <lib/HTTPLookupService.h>

DECLARE_LOG_OBJECT()

namespace pulsar {

const static std::string V1_PATH = "/lookup/v2/destination/";
const static std::string V2_PATH = "/lookup/v2/topic/";

const static std::string ADMIN_PATH_V1 = "/admin/";
const static std::string ADMIN_PATH_V2 = "/admin/v2/";

const static int MAX_HTTP_REDIRECTS = 20;
const static std::string PARTITION_METHOD_NAME = "partitions";
const static int NUMBER_OF_LOOKUP_THREADS = 1;

HTTPLookupService::CurlInitializer HTTPLookupService::curlInitializer;

HTTPLookupService::HTTPLookupService(const std::string &lookupUrl,
                                     const ClientConfiguration &clientConfiguration,
                                     const AuthenticationPtr &authData)
    : executorProvider_(boost::make_shared<ExecutorServiceProvider>(NUMBER_OF_LOOKUP_THREADS)),
      authenticationPtr_(authData),
      lookupTimeoutInSeconds_(clientConfiguration.getOperationTimeoutSeconds()),
      isUseTls_(clientConfiguration.isUseTls()),
      tlsAllowInsecure_(clientConfiguration.isTlsAllowInsecureConnection()),
      tlsTrustCertsFilePath_(clientConfiguration.getTlsTrustCertsFilePath()) {
    if (lookupUrl[lookupUrl.length() - 1] == '/') {
        // Remove trailing '/'
        adminUrl_ = lookupUrl.substr(0, lookupUrl.length() - 1);
    } else {
        adminUrl_ = lookupUrl;
    }
}

Future<Result, LookupDataResultPtr> HTTPLookupService::lookupAsync(const std::string &topic) {
    LookupPromise promise;
    boost::shared_ptr<TopicName> topicName = TopicName::get(topic);
    if (!topicName) {
        LOG_ERROR("Unable to parse topic - " << topic);
        promise.setFailed(ResultInvalidTopicName);
        return promise.getFuture();
    }

    std::stringstream completeUrlStream;
    if (topicName->isV2Topic()) {
        completeUrlStream << adminUrl_ << V2_PATH << topicName->getDomain() << "/" << topicName->getProperty()
                          << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName();
    } else {
        completeUrlStream << adminUrl_ << V1_PATH << topicName->getDomain() << "/" << topicName->getProperty()
                          << '/' << topicName->getCluster() << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName();
    }

    executorProvider_->get()->postWork(boost::bind(&HTTPLookupService::handleLookupHTTPRequest,
                                                   shared_from_this(), promise, completeUrlStream.str(),
                                                   Lookup));
    return promise.getFuture();
}

Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(
    const TopicNamePtr &topicName) {
    LookupPromise promise;
    std::stringstream completeUrlStream;

    if (topicName->isV2Topic()) {
        completeUrlStream << adminUrl_ << ADMIN_PATH_V2 << topicName->getDomain() << '/'
                          << topicName->getProperty() << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName() << '/' << PARTITION_METHOD_NAME;
    } else {
        completeUrlStream << adminUrl_ << ADMIN_PATH_V1 << topicName->getDomain() << '/'
                          << topicName->getProperty() << '/' << topicName->getCluster() << '/'
                          << topicName->getNamespacePortion() << '/' << topicName->getEncodedLocalName()
                          << '/' << PARTITION_METHOD_NAME;
    }

    executorProvider_->get()->postWork(boost::bind(&HTTPLookupService::handleLookupHTTPRequest,
                                                   shared_from_this(), promise, completeUrlStream.str(),
                                                   PartitionMetaData));
    return promise.getFuture();
}

Future<Result, NamespaceTopicsPtr> HTTPLookupService::getTopicsOfNamespaceAsync(
    const NamespaceNamePtr &nsName) {
    NamespaceTopicsPromise promise;
    std::stringstream completeUrlStream;

    if (nsName->isV2()) {
        completeUrlStream << adminUrl_ << ADMIN_PATH_V2 << "namespaces" << '/' << nsName->toString() << '/'
                          << "topics";
    } else {
        completeUrlStream << adminUrl_ << ADMIN_PATH_V1 << "namespaces" << '/' << nsName->toString() << '/'
                          << "destinations";
    }

    executorProvider_->get()->postWork(boost::bind(&HTTPLookupService::handleNamespaceTopicsHTTPRequest,
                                                   shared_from_this(), promise, completeUrlStream.str()));
    return promise.getFuture();
}

static size_t curlWriteCallback(void *contents, size_t size, size_t nmemb, void *responseDataPtr) {
    ((std::string *)responseDataPtr)->append((char *)contents, size * nmemb);
    return size * nmemb;
}

void HTTPLookupService::handleNamespaceTopicsHTTPRequest(NamespaceTopicsPromise promise,
                                                         const std::string completeUrl) {
    std::string responseData;
    Result result = sendHTTPRequest(completeUrl, responseData);

    if (result != ResultOk) {
        promise.setFailed(result);
    } else {
        promise.setValue(parseNamespaceTopicsData(responseData));
    }
}

Result HTTPLookupService::sendHTTPRequest(const std::string completeUrl, std::string &responseData) {
    CURL *handle;
    CURLcode res;
    std::string version = std::string("Pulsar-CPP-v") + _PULSAR_VERSION_;
    handle = curl_easy_init();

    if (!handle) {
        LOG_ERROR("Unable to curl_easy_init for url " << completeUrl);
        // No curl_easy_cleanup required since handle not initialized
        return ResultLookupError;
    }
    // set URL
    curl_easy_setopt(handle, CURLOPT_URL, completeUrl.c_str());

    // Write callback
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

    // New connection is made for each call
    curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

    // Skipping signal handling - results in timeouts not honored during the DNS lookup
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);

    // Timer
    curl_easy_setopt(handle, CURLOPT_TIMEOUT, lookupTimeoutInSeconds_);

    // Set User Agent
    curl_easy_setopt(handle, CURLOPT_USERAGENT, version.c_str());

    // Redirects
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(handle, CURLOPT_MAXREDIRS, MAX_HTTP_REDIRECTS);

    // Fail if HTTP return code >=400
    curl_easy_setopt(handle, CURLOPT_FAILONERROR, 1L);

    // Authorization data
    AuthenticationDataPtr authDataContent;
    Result authResult = authenticationPtr_->getAuthData(authDataContent);
    if (authResult != ResultOk) {
        LOG_ERROR(
            "All Authentication methods should have AuthenticationData and return true on getAuthData for "
            "url "
            << completeUrl);
        curl_easy_cleanup(handle);
        return authResult;
    }
    struct curl_slist *list = NULL;
    if (authDataContent->hasDataForHttp()) {
        list = curl_slist_append(list, authDataContent->getHttpHeaders().c_str());
    }
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);

    // TLS
    if (isUseTls_) {
        if (curl_easy_setopt(handle, CURLOPT_SSLENGINE, NULL) != CURLE_OK) {
            LOG_ERROR("Unable to load SSL engine for url " << completeUrl);
            curl_easy_cleanup(handle);
            return ResultConnectError;
        }
        if (curl_easy_setopt(handle, CURLOPT_SSLENGINE_DEFAULT, 1L) != CURLE_OK) {
            LOG_ERROR("Unable to load SSL engine as default, for url " << completeUrl);
            curl_easy_cleanup(handle);
            return ResultConnectError;
        }
        curl_easy_setopt(handle, CURLOPT_SSLCERTTYPE, "PEM");

        if (tlsAllowInsecure_) {
            curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0L);
        } else {
            curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 1L);
        }

        if (!tlsTrustCertsFilePath_.empty()) {
            curl_easy_setopt(handle, CURLOPT_CAINFO, tlsTrustCertsFilePath_.c_str());
        }

        if (authDataContent->hasDataForTls()) {
            curl_easy_setopt(handle, CURLOPT_SSLCERT, authDataContent->getTlsCertificates().c_str());
            curl_easy_setopt(handle, CURLOPT_SSLKEY, authDataContent->getTlsPrivateKey().c_str());
        }
    }

    LOG_INFO("Curl Lookup Request sent for " << completeUrl);

    // Make get call to server
    res = curl_easy_perform(handle);

    // Free header list
    curl_slist_free_all(list);

    Result retResult = ResultOk;

    switch (res) {
        case CURLE_OK:
            long response_code;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
            LOG_INFO("Response received for url " << completeUrl << " code " << response_code);
            if (response_code == 200) {
                retResult = ResultOk;
            } else {
                retResult = ResultLookupError;
            }
            break;
        case CURLE_COULDNT_CONNECT:
        case CURLE_COULDNT_RESOLVE_PROXY:
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_HTTP_RETURNED_ERROR:
            LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
            retResult = ResultConnectError;
            break;
        case CURLE_READ_ERROR:
            LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
            retResult = ResultReadError;
            break;
        case CURLE_OPERATION_TIMEDOUT:
            LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
            retResult = ResultTimeout;
            break;
        default:
            LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
            retResult = ResultLookupError;
            break;
    }
    curl_easy_cleanup(handle);
    return retResult;
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
    LOG_INFO("parsePartitionData = " << *lookupDataResultPtr);
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

    const std::string brokerUrlTls = root.get("brokerUrlTls", defaultNotFoundString).asString();
    if (brokerUrlTls == defaultNotFoundString) {
        LOG_ERROR("malformed json! - brokerUrlTls not present" << json);
        return LookupDataResultPtr();
    }

    LookupDataResultPtr lookupDataResultPtr = boost::make_shared<LookupDataResult>();
    lookupDataResultPtr->setBrokerUrl(brokerUrl);
    lookupDataResultPtr->setBrokerUrlTls(brokerUrlTls);

    LOG_INFO("parseLookupData = " << *lookupDataResultPtr);
    return lookupDataResultPtr;
}

NamespaceTopicsPtr HTTPLookupService::parseNamespaceTopicsData(const std::string &json) {
    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(json, root, false)) {
        LOG_ERROR("Failed to parse json of Topics of Namespace: " << reader.getFormatedErrorMessages()
                                                                  << "\nInput Json = " << json);
        return NamespaceTopicsPtr();
    }

    Json::Value topicsArray = root["topics"];
    std::set<std::string> topicSet;
    // get all topics
    for (int i = 0; i < topicsArray.size(); i++) {
        // remove partition part
        const std::string &topicName = topicsArray[i].asString();
        int pos = topicName.find("-partition-");
        std::string filteredName = topicName.substr(0, pos);

        // filter duped topic name
        if (topicSet.find(filteredName) == topicSet.end()) {
            topicSet.insert(filteredName);
        }
    }

    NamespaceTopicsPtr topicsResultPtr =
        boost::make_shared<std::vector<std::string>>(topicSet.begin(), topicSet.end());

    return topicsResultPtr;
}

void HTTPLookupService::handleLookupHTTPRequest(LookupPromise promise, const std::string completeUrl,
                                                RequestType requestType) {
    std::string responseData;
    Result result = sendHTTPRequest(completeUrl, responseData);

    if (result != ResultOk) {
        promise.setFailed(result);
    } else {
        promise.setValue((requestType == PartitionMetaData) ? parsePartitionData(responseData)
                                                            : parseLookupData(responseData));
    }
}

}  // namespace pulsar
