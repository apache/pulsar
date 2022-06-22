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

Oauth2CachedToken::Oauth2CachedToken(Oauth2TokenResultPtr token) {
    latest_ = token;

    int64_t expiredIn = token->getExpiresIn();
    if (expiredIn > 0) {
        expiresAt_ = Clock::now() + std::chrono::seconds(expiredIn);
    } else {
        throw std::runtime_error("ExpiresIn in Oauth2TokenResult invalid value: " +
                                 std::to_string(expiredIn));
    }
    authData_ = AuthenticationDataPtr(new AuthDataOauth2(token->getAccessToken()));
}

AuthenticationDataPtr Oauth2CachedToken::getAuthData() { return authData_; }

Oauth2CachedToken::~Oauth2CachedToken() {}

bool Oauth2CachedToken::isExpired() { return expiresAt_ < Clock::now(); }

// OauthFlow

Oauth2Flow::Oauth2Flow() {}
Oauth2Flow::~Oauth2Flow() {}

KeyFile KeyFile::fromParamMap(ParamMap& params) {
    const auto it = params.find("private_key");
    if (it != params.cend()) {
        return fromFile(it->second);
    } else {
        return {params["client_id"], params["client_secret"]};
    }
}

// read clientId/clientSecret from passed in `credentialsFilePath`
KeyFile KeyFile::fromFile(const std::string& credentialsFilePath) {
    boost::property_tree::ptree loadPtreeRoot;
    try {
        boost::property_tree::read_json(credentialsFilePath, loadPtreeRoot);
    } catch (const boost::property_tree::json_parser_error& e) {
        LOG_ERROR("Failed to parse json input file for credentialsFilePath: " << credentialsFilePath << ": "
                                                                              << e.what());
        return {};
    }

    try {
        return {loadPtreeRoot.get<std::string>("client_id"), loadPtreeRoot.get<std::string>("client_secret")};
    } catch (const boost::property_tree::ptree_error& e) {
        LOG_ERROR("Failed to get client_id or client_secret in " << credentialsFilePath << ": " << e.what());
        return {};
    }
}

ClientCredentialFlow::ClientCredentialFlow(ParamMap& params)
    : issuerUrl_(params["issuer_url"]),
      keyFile_(KeyFile::fromParamMap(params)),
      audience_(params["audience"]),
      scope_(params["scope"]) {}

std::string ClientCredentialFlow::getTokenEndPoint() const { return tokenEndPoint_; }

static size_t curlWriteCallback(void* contents, size_t size, size_t nmemb, void* responseDataPtr) {
    ((std::string*)responseDataPtr)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

void ClientCredentialFlow::initialize() {
    if (issuerUrl_.empty()) {
        LOG_ERROR("Failed to initialize ClientCredentialFlow: issuer_url is not set");
        return;
    }
    if (!keyFile_.isValid()) {
        return;
    }

    CURL* handle = curl_easy_init();
    CURLcode res;
    std::string responseData;

    // set header: json, request type: post
    struct curl_slist* list = NULL;
    list = curl_slist_append(list, "Accept: application/json");
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "GET");

    // set URL: well-know endpoint
    std::string wellKnownUrl = issuerUrl_;
    if (wellKnownUrl.back() == '/') {
        wellKnownUrl.pop_back();
    }
    wellKnownUrl.append("/.well-known/openid-configuration");
    curl_easy_setopt(handle, CURLOPT_URL, wellKnownUrl.c_str());

    // Write callback
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

    // New connection is made for each call
    curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);

    char errorBuffer[CURL_ERROR_SIZE];
    curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, errorBuffer);

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
            LOG_ERROR("Response failed for getting the well-known configuration "
                      << issuerUrl_ << ". Error Code " << res << ": " << errorBuffer);
            break;
    }
    // Free header list
    curl_slist_free_all(list);
    curl_easy_cleanup(handle);
}
void ClientCredentialFlow::close() {}

ParamMap ClientCredentialFlow::generateParamMap() const {
    if (!keyFile_.isValid()) {
        return {};
    }

    ParamMap params;
    params.emplace("grant_type", "client_credentials");
    params.emplace("client_id", keyFile_.getClientId());
    params.emplace("client_secret", keyFile_.getClientSecret());
    params.emplace("audience", audience_);
    if (!scope_.empty()) {
        params.emplace("scope", scope_);
    }
    return params;
}

static std::string buildClientCredentialsBody(CURL* curl, const ParamMap& params) {
    std::ostringstream oss;
    bool addSeparater = false;

    for (const auto& kv : params) {
        if (addSeparater) {
            oss << "&";
        } else {
            addSeparater = true;
        }

        char* encodedKey = curl_easy_escape(curl, kv.first.c_str(), kv.first.length());
        if (!encodedKey) {
            LOG_ERROR("curl_easy_escape for " << kv.first << " failed");
            continue;
        }
        char* encodedValue = curl_easy_escape(curl, kv.second.c_str(), kv.second.length());
        if (!encodedValue) {
            LOG_ERROR("curl_easy_escape for " << kv.second << " failed");
            continue;
        }

        oss << encodedKey << "=" << encodedValue;
        curl_free(encodedKey);
        curl_free(encodedValue);
    }

    return oss.str();
}

Oauth2TokenResultPtr ClientCredentialFlow::authenticate() {
    std::call_once(initializeOnce_, &ClientCredentialFlow::initialize, this);
    Oauth2TokenResultPtr resultPtr = Oauth2TokenResultPtr(new Oauth2TokenResult());
    if (tokenEndPoint_.empty()) {
        return resultPtr;
    }

    CURL* handle = curl_easy_init();
    const auto postData = buildClientCredentialsBody(handle, generateParamMap());
    if (postData.empty()) {
        curl_easy_cleanup(handle);
        return resultPtr;
    }
    LOG_DEBUG("Generate URL encoded body for ClientCredentialFlow: " << postData);

    CURLcode res;
    std::string responseData;

    struct curl_slist* list = NULL;
    list = curl_slist_append(list, "Content-Type: application/x-www-form-urlencoded");
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

    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, postData.c_str());

    char errorBuffer[CURL_ERROR_SIZE];
    curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, errorBuffer);

    // Make get call to server
    res = curl_easy_perform(handle);

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
                              << e.what() << "\nInput Json = " << responseData << " passedin: " << postData);
                    break;
                }

                resultPtr->setAccessToken(root.get<std::string>("access_token", ""));
                resultPtr->setExpiresIn(
                    root.get<uint32_t>("expires_in", Oauth2TokenResult::undefined_expiration));
                resultPtr->setRefreshToken(root.get<std::string>("refresh_token", ""));
                resultPtr->setIdToken(root.get<std::string>("id_token", ""));

                if (!resultPtr->getAccessToken().empty()) {
                    LOG_DEBUG("access_token: " << resultPtr->getAccessToken()
                                               << " expires_in: " << resultPtr->getExpiresIn());
                } else {
                    LOG_ERROR("Response doesn't contain access_token, the response is: " << responseData);
                }
            } else {
                LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". response Code "
                                                           << response_code << " passedin: " << postData);
            }
            break;
        default:
            LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". ErrorCode " << res << ": "
                                                       << errorBuffer << " passedin: " << postData);
            break;
    }
    // Free header list
    curl_slist_free_all(list);
    curl_easy_cleanup(handle);

    return resultPtr;
}

// AuthOauth2

AuthOauth2::AuthOauth2(ParamMap& params) : flowPtr_(new ClientCredentialFlow(params)) {}

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
        try {
            cachedTokenPtr_ = CachedTokenPtr(new Oauth2CachedToken(flowPtr_->authenticate()));
        } catch (const std::runtime_error& e) {
            // The real error logs have already been printed in authenticate()
            return ResultAuthenticationError;
        }
    }

    authDataContent = cachedTokenPtr_->getAuthData();
    return ResultOk;
}

}  // namespace pulsar
