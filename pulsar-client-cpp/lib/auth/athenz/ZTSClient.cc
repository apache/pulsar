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
#include "ZTSClient.h"
#include <sstream>

#include <unistd.h>
#include <string.h>
#include <time.h>

#include <openssl/sha.h>
#include <openssl/rsa.h>
#include <openssl/ec.h>
#include <openssl/pem.h>

#include <curl/curl.h>

#include <json/value.h>
#include <json/reader.h>

#include <boost/lexical_cast.hpp>
#include <boost/xpressive/xpressive.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>


DECLARE_LOG_OBJECT()

namespace pulsar {

    const static std::string DEFAULT_PRINCIPAL_HEADER = "Athenz-Principal-Auth";
    const static std::string DEFAULT_ROLE_HEADER = "Athenz-Role-Auth";
    const static int REQUEST_TIMEOUT = 10000;
    const static int DEFAULT_TOKEN_EXPIRATION_TIME_SEC = 3600;
    const static int MIN_TOKEN_EXPIRATION_TIME_SEC = 900;
    const static int MAX_HTTP_REDIRECTS = 20;
    const static long long FETCH_EPSILON = 60; // if cache expires in 60 seconds, get it from ZTS
    const static std::string requiredParams[] = {"tenantDomain", "tenantService", "providerDomain", "privateKeyPath", "ztsUrl"};

    std::map<std::string, RoleToken> ZTSClient::roleTokenCache_;

    ZTSClient::ZTSClient (std::map<std::string, std::string>& params) {
        // required parameter check
        bool valid = true;
        for (int i = 0; i < sizeof(requiredParams) / sizeof(std::string) ; i++) {
            if (params.find(requiredParams[i]) == params.end()) {
                valid = false;
                LOG_ERROR(requiredParams[i] << " parameter is required");
            }
        }
        if (!valid) {
            LOG_ERROR("Some parameters are missing")
            return;
        }

        // set required value
        tenantDomain_ = params[requiredParams[0]];
        tenantService_ = params[requiredParams[1]];
        providerDomain_ = params[requiredParams[2]];
        privateKeyPath_ = params[requiredParams[3]];
        ztsUrl_ = params[requiredParams[4]];

        // set optional value
        keyId_ = params.find("keyId") == params.end() ? "0" : params["keyId"];
        principalHeader_ = params.find("principalHeader") == params.end() ? DEFAULT_PRINCIPAL_HEADER : params["principalHeader"];
        roleHeader_ = params.find("roleHeader") == params.end() ? DEFAULT_ROLE_HEADER : params["roleHeader"];
        tokenExpirationTime_ = DEFAULT_TOKEN_EXPIRATION_TIME_SEC;
        if (params.find("tokenExpirationTime") != params.end()) {
            tokenExpirationTime_ = boost::lexical_cast<int>(params["tokenExpirationTime"]);
            if (tokenExpirationTime_ < MIN_TOKEN_EXPIRATION_TIME_SEC) {
                LOG_WARN(tokenExpirationTime_ << " is too small as a token expiration time. " << MIN_TOKEN_EXPIRATION_TIME_SEC << " is set instead of it.");
                tokenExpirationTime_ = MIN_TOKEN_EXPIRATION_TIME_SEC;
            }
        }

        if (*(--ztsUrl_.end()) == '/') {
            ztsUrl_.erase(--ztsUrl_.end());
        }

        LOG_DEBUG("ZTSClient is constructed properly")
    }

    ZTSClient::~ZTSClient() {
        LOG_DEBUG("ZTSClient is destructed")
    }

    std::string ZTSClient::getSalt() {
        unsigned long long salt = 0;
        for (int i = 0; i < 8; i++) {
            salt += ( (unsigned long long) rand() % (1 << 8) ) << 8 * i;
        }
        std::stringstream ss;
        ss << std::hex << salt;
        return ss.str();
    }

    std::string ZTSClient::ybase64Encode(const unsigned char *input, int length) {
        // base64 encode
        typedef boost::archive::iterators::base64_from_binary<boost::archive::iterators::transform_width<const unsigned char*, 6, 8> > base64;
        std::string ret = std::string(base64(input), base64(input + length));

        // replace '+', '/' to '.', '_' for ybase64
        for(std::string::iterator itr = ret.begin(); itr != ret.end(); itr++) {
            switch (*itr) {
                case '+':
                    ret.replace(itr, itr+1, ".");
                    break;
                case '/':
                    ret.replace(itr, itr+1, "_");
                    break;
                default:
                    break;
            }
        }

        // padding by '-'
        for (int i = 4 - ret.size() % 4; i; i--) {
            ret.push_back('-');
        }

        return ret;
    }

    const std::string ZTSClient::getPrincipalToken() const {
        // construct unsigned principal token
        std::string unsignedTokenString = "v=S1";
        char host[BUFSIZ];
        long long t = (long long) time(NULL);

        gethostname(host, sizeof(host));

        unsignedTokenString += ";d=" + tenantDomain_;
        unsignedTokenString += ";n=" + tenantService_;
        unsignedTokenString += ";h=" + std::string(host);
        unsignedTokenString += ";a=" + getSalt();
        unsignedTokenString += ";t=" + boost::lexical_cast<std::string>(t);
        unsignedTokenString += ";e=" + boost::lexical_cast<std::string>(t + tokenExpirationTime_);
        unsignedTokenString += ";k=" + keyId_;

        LOG_DEBUG("Created unsigned principal token: " << unsignedTokenString);

        // signing
        const char* unsignedToken = unsignedTokenString.c_str();
        unsigned char signature[BUFSIZ];
        unsigned char hash[SHA256_DIGEST_LENGTH];
        unsigned int siglen;
        FILE *fp;
        RSA *privateKey;

        fp = fopen(privateKeyPath_.c_str(), "r");
        if (fp == NULL) {
            LOG_ERROR("Failed to open athenz private key file: " << privateKeyPath_);
            return "";
        }

        privateKey = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
        if (privateKey == NULL) {
            LOG_ERROR("Failed to read private key: " << privateKeyPath_);
            fclose(fp);
            return "";
        }

        fclose(fp);

        SHA256( (unsigned char *)unsignedToken, unsignedTokenString.length(), hash );
        RSA_sign(NID_sha256, hash, SHA256_DIGEST_LENGTH, signature, &siglen, privateKey);

        std::string principalToken = unsignedTokenString + ";s=" + ybase64Encode(signature, siglen);
        LOG_DEBUG("Created signed principal token: " << principalToken);

        return principalToken;
    }

    static size_t curlWriteCallback(void *contents, size_t size, size_t nmemb, void *responseDataPtr) {
        ((std::string*)responseDataPtr)->append((char*)contents, size * nmemb);
        return size * nmemb;
    }

    static boost::mutex cacheMtx_;
    const std::string ZTSClient::getRoleToken() const {
        RoleToken roleToken;
        std::string cacheKey = "p=" + tenantDomain_ + "." + tenantService_ + ";d=" + providerDomain_;

        // locked block
        {
            boost::lock_guard<boost::mutex> lock(cacheMtx_);
            roleToken = roleTokenCache_[cacheKey];
        }

        if (!roleToken.token.empty() && roleToken.expiryTime > (long long) time(NULL) + FETCH_EPSILON) {
            LOG_DEBUG("Got cached role token " << roleToken.token);
            return roleToken.token;
        }

        std::string completeUrl = ztsUrl_ + "/zts/v1/domain/" + providerDomain_ + "/token";

        CURL *handle;
        CURLcode res;
        std::string responseData;

        handle = curl_easy_init();

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
        curl_easy_setopt(handle, CURLOPT_TIMEOUT, REQUEST_TIMEOUT);

        // Redirects
        curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(handle, CURLOPT_MAXREDIRS, MAX_HTTP_REDIRECTS);

        // Fail if HTTP return code >= 400
        curl_easy_setopt(handle, CURLOPT_FAILONERROR, 1L);

        struct curl_slist *list = NULL;
        std::string httpHeader = principalHeader_ + ": " + getPrincipalToken();
        list = curl_slist_append(list, httpHeader.c_str());
        curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);

        // Make get call to server
        res = curl_easy_perform(handle);

        // Free header list
        curl_slist_free_all(list);

        switch(res) {
            case CURLE_OK:
                long response_code;
                curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
                LOG_DEBUG("Response received for url " << completeUrl << " code " << response_code);
                if (response_code == 200) {
                    Json::Reader reader;
                    Json::Value root;
                    if (!reader.parse(responseData, root)) {
                        LOG_ERROR("Failed to parse json of ZTS response: " << reader.getFormatedErrorMessages()
                                  << "\nInput Json = " << responseData);
                        break;
                    }
                    roleToken.token = root["token"].asString();
                    roleToken.expiryTime = root["expiryTime"].asInt64();
                    boost::lock_guard<boost::mutex> lock(cacheMtx_);
                    roleTokenCache_[cacheKey] = roleToken;
                    LOG_DEBUG("Got role token " << roleToken.token)
                } else {
                    LOG_ERROR("Response failed for url " << completeUrl << ". response Code " << response_code)
                }
                break;
            default:
                LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
                break;
        }
        curl_easy_cleanup(handle);

        return roleToken.token;
    }

    const std::string ZTSClient::getHeader() const {
        return roleHeader_;
    }

}
