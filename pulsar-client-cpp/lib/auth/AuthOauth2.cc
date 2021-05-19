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
#include <lib/auth/AuthOauth2.h>

#include <curl/curl.h>
#include <sstream>
#include <stdexcept>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <lib/LogUtils.h>
DECLARE_LOG_OBJECT()

namespace pulsar {

// AuthDataOauth2

AuthDataOauth2::AuthDataOauth2(const std::string& accessToken) { accessToken_ = accessToken; }

AuthDataOauth2::~AuthDataOauth2() {}

bool AuthDataOauth2::hasDataForHttp() { return true; }

std::string AuthDataOauth2::getHttpHeaders() { return "Authorization: Bearer " + accessToken_; }

bool AuthDataOauth2::hasDataFromCommand() { return true; }

std::string AuthDataOauth2::getCommandData() { return accessToken_; }

// Oauth2TokenResult

Oauth2TokenResult::Oauth2TokenResult() { expiresIn_ = undefined_expiration; }

Oauth2TokenResult::~Oauth2TokenResult() {}

Oauth2TokenResult& Oauth2TokenResult::setAccessToken(const std::string& accessToken) {
    accessToken_ = accessToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setIdToken(const std::string& idToken) {
    idToken_ = idToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setRefreshToken(const std::string& refreshToken) {
    refreshToken_ = refreshToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setExpiresIn(const int64_t expiresIn) {
    expiresIn_ = expiresIn;
    return *this;
}

const std::string& Oauth2TokenResult::getAccessToken() const { return accessToken_; }

const std::string& Oauth2TokenResult::getIdToken() const { return idToken_; }

const std::string& Oauth2TokenResult::getRefreshToken() const { return refreshToken_; }

int64_t Oauth2TokenResult::getExpiresIn() const { return expiresIn_; }

// CachedToken

CachedToken::CachedToken() {}

CachedToken::~CachedToken() {}

// Oauth2CachedToken

static int64_t currentTimeMillis() {
    using namespace boost::posix_time;
    using boost::posix_time::milliseconds;
    using boost::posix_time::seconds;
    static ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));

    time_duration diff = microsec_clock::universal_time() - time_t_epoch;
    return diff.total_milliseconds();
}

Oauth2CachedToken::Oauth2CachedToken(Oauth2TokenResultPtr token) {
    latest_ = token;

    int64_t expiredIn = token->getExpiresIn();
    if (expiredIn > 0) {
        expiresAt_ = expiredIn + currentTimeMillis();
    } else {
        throw std::runtime_error("ExpiresIn in Oauth2TokenResult invalid value: " + expiredIn);
    }
    authData_ = AuthenticationDataPtr(new AuthDataOauth2(token->getAccessToken()));
}

AuthenticationDataPtr Oauth2CachedToken::getAuthData() { return authData_; }

Oauth2CachedToken::~Oauth2CachedToken() {}

bool Oauth2CachedToken::isExpired() { return expiresAt_ < currentTimeMillis(); }

// OauthFlow

Oauth2Flow::Oauth2Flow() {}
Oauth2Flow::~Oauth2Flow() {}

// ClientCredentialFlow
static std::string readFromFile(const std::string& credentialsFilePath) {
    std::ifstream input(credentialsFilePath);
    std::stringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

ClientCredentialFlow::ClientCredentialFlow(const std::string& issuerUrl, const std::string& clientId,
                                           const std::string& clientSecret, const std::string& audience) {
    issuerUrl_ = issuerUrl;
    clientId_ = clientId;
    clientSecret_ = clientSecret;
    audience_ = audience;
    this->initialize();
}

// read clientId/clientSecret from passed in `credentialsFilePath`
ClientCredentialFlow::ClientCredentialFlow(const std::string& issuerUrl,
                                           const std::string& credentialsFilePath,
                                           const std::string& audience) {
    issuerUrl_ = issuerUrl;
    audience_ = audience;

    boost::property_tree::ptree loadPtreeRoot;
    try {
        boost::property_tree::read_json(credentialsFilePath, loadPtreeRoot);
    } catch (boost::property_tree::json_parser_error& e) {
        LOG_ERROR("Failed to parse json input file for credentialsFilePath: " << credentialsFilePath
                                                                              << "with error:" << e.what());
        return;
    }

    const std::string defaultNotFoundString = "Client Id / Secret Not Found";

    clientId_ = loadPtreeRoot.get<std::string>("client_id", defaultNotFoundString);
    clientSecret_ = loadPtreeRoot.get<std::string>("client_secret", defaultNotFoundString);

    if (clientId_ == defaultNotFoundString || clientSecret_ == defaultNotFoundString) {
        LOG_ERROR("Not get valid clientId / clientSecret: " << clientId_ << "/" << clientSecret_);
        return;
    }
    this->initialize();
}

static size_t curlWriteCallback(void* contents, size_t size, size_t nmemb, void* responseDataPtr) {
    ((std::string*)responseDataPtr)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

void ClientCredentialFlow::initialize() {
    CURL* handle = curl_easy_init();
    CURLcode res;
    std::string responseData;

    // set header: json, request type: post
    struct curl_slist* list = NULL;
    list = curl_slist_append(list, "Accept: application/json");
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "GET");

    // set URL: well-know endpoint
    issuerUrl_.append("/.well-known/openid-configuration");
    curl_easy_setopt(handle, CURLOPT_URL, issuerUrl_.c_str());

    // Write callback
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

    // New connection is made for each call
    curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0L);

    // Make get call to server
    res = curl_easy_perform(handle);

    switch (res) {
        case CURLE_OK:
            long response_code;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
            LOG_DEBUG("Received well-known configuration data " << issuerUrl_ << " code " << response_code);
            if (response_code == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse well-known configuration data response: "
                              << e.what() << "\nInput Json = " << responseData);
                    break;
                }

                this->tokenEndPoint_ = root.get<std::string>("token_endpoint");

                LOG_DEBUG("Get token endpoint: " << this->tokenEndPoint_);
            } else {
                LOG_ERROR("Response failed for getting the well-known configuration "
                          << issuerUrl_ << ". response Code " << response_code);
            }
            break;
        default:
            LOG_ERROR("Response failed for getting the well-known configuration " << issuerUrl_
                                                                                  << ". Error Code " << res);
            break;
    }
    // Free header list
    curl_slist_free_all(list);
    curl_easy_cleanup(handle);
}
void ClientCredentialFlow::close() {}

Oauth2TokenResultPtr ClientCredentialFlow::authenticate() {
    Oauth2TokenResultPtr resultPtr = Oauth2TokenResultPtr(new Oauth2TokenResult());

    CURL* handle = curl_easy_init();
    CURLcode res;
    std::string responseData;

    // set header: json, request type: post
    struct curl_slist* list = NULL;
    list = curl_slist_append(list, "Content-Type: application/json");
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "POST");

    // set URL: issuerUrl
    curl_easy_setopt(handle, CURLOPT_URL, tokenEndPoint_.c_str());

    // Write callback
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

    // New connection is made for each call
    curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0L);

    // fill in the request data
    boost::property_tree::ptree pt;
    pt.put("grant_type", "client_credentials");
    pt.put("client_id", clientId_);
    pt.put("client_secret", clientSecret_);
    pt.put("audience", audience_);

    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, pt);
    std::string ssString = ss.str();

    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, ssString.c_str());

    // Make get call to server
    res = curl_easy_perform(handle);

    LOG_DEBUG("issuerUrl_ " << issuerUrl_ << " clientid: " << clientId_ << " client_secret " << clientSecret_
                            << " audience " << audience_ << " ssstring " << ssString);

    switch (res) {
        case CURLE_OK:
            long response_code;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
            LOG_DEBUG("Response received for issuerurl " << issuerUrl_ << " code " << response_code);
            if (response_code == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse json of Oauth2 response: "
                              << e.what() << "\nInput Json = " << responseData << " passedin: " << ssString);
                    break;
                }

                resultPtr->setAccessToken(root.get<std::string>("access_token"));
                resultPtr->setExpiresIn(root.get<uint32_t>("expires_in"));

                LOG_DEBUG("access_token: " << resultPtr->getAccessToken()
                                           << " expires_in: " << resultPtr->getExpiresIn());
            } else {
                LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". response Code "
                                                           << response_code << " passedin: " << ssString);
            }
            break;
        default:
            LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". Error Code " << res
                                                       << " passedin: " << ssString);
            break;
    }
    // Free header list
    curl_slist_free_all(list);
    curl_easy_cleanup(handle);

    return resultPtr;
}

// AuthOauth2

AuthOauth2::AuthOauth2(ParamMap& params) {
    std::map<std::string, std::string>::iterator it;
    it = params.find("private_key");

    if (it != params.end()) {
        flowPtr_ = FlowPtr(
            new ClientCredentialFlow(params["issuer_url"], params["private_key"], params["audience"]));
    } else {
        flowPtr_ = FlowPtr(new ClientCredentialFlow(params["issuer_url"], params["client_id"],
                                                    params["client_secret"], params["audience"]));
    }
}

AuthOauth2::~AuthOauth2() {}

ParamMap parseJsonAuthParamsString(const std::string& authParamsString) {
    ParamMap params;
    if (!authParamsString.empty()) {
        boost::property_tree::ptree root;
        std::stringstream stream;
        stream << authParamsString;
        try {
            boost::property_tree::read_json(stream, root);
            for (const auto& item : root) {
                params[item.first] = item.second.get_value<std::string>();
            }
        } catch (boost::property_tree::json_parser_error& e) {
            LOG_ERROR("Invalid String Error: " << e.what());
        }
    }
    return params;
}

AuthenticationPtr AuthOauth2::create(const std::string& authParamsString) {
    ParamMap params = parseJsonAuthParamsString(authParamsString);

    return create(params);
}

AuthenticationPtr AuthOauth2::create(ParamMap& params) { return AuthenticationPtr(new AuthOauth2(params)); }

const std::string AuthOauth2::getAuthMethodName() const { return "token"; }

Result AuthOauth2::getAuthData(AuthenticationDataPtr& authDataContent) {
    if (cachedTokenPtr_ == nullptr || cachedTokenPtr_->isExpired()) {
        cachedTokenPtr_ = CachedTokenPtr(new Oauth2CachedToken(flowPtr_->authenticate()));
    }

    authDataContent = cachedTokenPtr_->getAuthData();
    return ResultOk;
}

}  // namespace pulsar
