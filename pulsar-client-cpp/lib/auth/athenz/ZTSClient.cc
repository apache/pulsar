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

#ifndef _MSC_VER
#include <unistd.h>
#else
#include <stdio.h>
#endif
#include <string.h>
#include <time.h>

#include <openssl/sha.h>
#include <openssl/rsa.h>
#include <openssl/ec.h>
#include <openssl/pem.h>

#include <curl/curl.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
namespace ptree = boost::property_tree;

#include <boost/regex.hpp>
#include <boost/xpressive/xpressive.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <mutex>

DECLARE_LOG_OBJECT()

namespace pulsar {

const static std::string DEFAULT_PRINCIPAL_HEADER = "Athenz-Principal-Auth";
const static std::string DEFAULT_ROLE_HEADER = "Athenz-Role-Auth";
const static int REQUEST_TIMEOUT = 30000;
const static int DEFAULT_TOKEN_EXPIRATION_TIME_SEC = 3600;
const static int MIN_TOKEN_EXPIRATION_TIME_SEC = 900;
const static int MAX_HTTP_REDIRECTS = 20;
const static long long FETCH_EPSILON = 60;  // if cache expires in 60 seconds, get it from ZTS
const static std::string requiredParams[] = {"tenantDomain", "tenantService", "providerDomain", "privateKey",
                                             "ztsUrl"};

std::map<std::string, RoleToken> ZTSClient::roleTokenCache_;

ZTSClient::ZTSClient(std::map<std::string, std::string> &params) {
    // required parameter check
    bool valid = true;
    for (int i = 0; i < sizeof(requiredParams) / sizeof(std::string); i++) {
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
    privateKeyUri_ = parseUri(params[requiredParams[3]].c_str());
    ztsUrl_ = params[requiredParams[4]];

    // set optional value
    keyId_ = params.find("keyId") == params.end() ? "0" : params["keyId"];
    principalHeader_ =
        params.find("principalHeader") == params.end() ? DEFAULT_PRINCIPAL_HEADER : params["principalHeader"];
    roleHeader_ = params.find("roleHeader") == params.end() ? DEFAULT_ROLE_HEADER : params["roleHeader"];
    tokenExpirationTime_ = DEFAULT_TOKEN_EXPIRATION_TIME_SEC;
    if (params.find("tokenExpirationTime") != params.end()) {
        tokenExpirationTime_ = std::stoi(params["tokenExpirationTime"]);
        if (tokenExpirationTime_ < MIN_TOKEN_EXPIRATION_TIME_SEC) {
            LOG_WARN(tokenExpirationTime_ << " is too small as a token expiration time. "
                                          << MIN_TOKEN_EXPIRATION_TIME_SEC << " is set instead of it.");
            tokenExpirationTime_ = MIN_TOKEN_EXPIRATION_TIME_SEC;
        }
    }

    if (*(--ztsUrl_.end()) == '/') {
        ztsUrl_.erase(--ztsUrl_.end());
    }

    LOG_DEBUG("ZTSClient is constructed properly")
}

ZTSClient::~ZTSClient(){LOG_DEBUG("ZTSClient is destructed")}

std::string ZTSClient::getSalt() {
    unsigned long long salt = 0;
    for (int i = 0; i < 8; i++) {
        salt += ((unsigned long long)rand() % (1 << 8)) << 8 * i;
    }
    std::stringstream ss;
    ss << std::hex << salt;
    return ss.str();
}

std::string ZTSClient::ybase64Encode(const unsigned char *input, int length) {
    // base64 encode
    typedef boost::archive::iterators::base64_from_binary<
        boost::archive::iterators::transform_width<const unsigned char *, 6, 8> >
        base64;
    std::string ret = std::string(base64(input), base64(input + length));

    // replace '+', '/' to '.', '_' for ybase64
    for (std::string::iterator itr = ret.begin(); itr != ret.end(); itr++) {
        switch (*itr) {
            case '+':
                ret.replace(itr, itr + 1, ".");
                break;
            case '/':
                ret.replace(itr, itr + 1, "_");
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

char *ZTSClient::base64Decode(const char *input) {
    if (input == NULL) {
        return NULL;
    }

    size_t length = strlen(input);
    if (length == 0) {
        return NULL;
    }

    BIO *bio, *b64;
    char *result = (char *)malloc(length);

    bio = BIO_new_mem_buf((void *)input, -1);
    b64 = BIO_new(BIO_f_base64());
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    int decodeStrLen = BIO_read(bio, result, length);
    BIO_free_all(bio);
    if (decodeStrLen > 0) {
        result[decodeStrLen] = '\0';
        return result;
    }
    free(result);

    return NULL;
}

const std::string ZTSClient::getPrincipalToken() const {
    // construct unsigned principal token
    std::string unsignedTokenString = "v=S1";
    char host[BUFSIZ] = {};
    long long t = (long long)time(NULL);

    gethostname(host, sizeof(host));

    unsignedTokenString += ";d=" + tenantDomain_;
    unsignedTokenString += ";n=" + tenantService_;
    unsignedTokenString += ";h=" + std::string(host);
    unsignedTokenString += ";a=" + getSalt();
    unsignedTokenString += ";t=" + std::to_string(t);
    unsignedTokenString += ";e=" + std::to_string(t + tokenExpirationTime_);
    unsignedTokenString += ";k=" + keyId_;

    LOG_DEBUG("Created unsigned principal token: " << unsignedTokenString);

    // signing
    const char *unsignedToken = unsignedTokenString.c_str();
    unsigned char signature[BUFSIZ] = {};
    unsigned char hash[SHA256_DIGEST_LENGTH] = {};
    unsigned int siglen;
    FILE *fp;
    RSA *privateKey;

    if (privateKeyUri_.scheme == "data") {
        if (privateKeyUri_.mediaTypeAndEncodingType != "application/x-pem-file;base64") {
            LOG_ERROR("Unsupported mediaType or encodingType: " << privateKeyUri_.mediaTypeAndEncodingType);
            return "";
        }
        char *decodeStr = base64Decode(privateKeyUri_.data.c_str());

        if (decodeStr == NULL) {
            LOG_ERROR("Failed to decode privateKey");
            return "";
        }

        BIO *bio = BIO_new_mem_buf((void *)decodeStr, -1);
        BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
        if (bio == NULL) {
            LOG_ERROR("Failed to create key BIO");
            free(decodeStr);
            return "";
        }
        privateKey = PEM_read_bio_RSAPrivateKey(bio, NULL, NULL, NULL);
        BIO_free(bio);
        free(decodeStr);
        if (privateKey == NULL) {
            LOG_ERROR("Failed to load privateKey");
            return "";
        }
    } else if (privateKeyUri_.scheme == "file") {
        fp = fopen(privateKeyUri_.path.c_str(), "r");
        if (fp == NULL) {
            LOG_ERROR("Failed to open athenz private key file: " << privateKeyUri_.path);
            return "";
        }

        privateKey = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
        fclose(fp);
        if (privateKey == NULL) {
            LOG_ERROR("Failed to read private key: " << privateKeyUri_.path);
            return "";
        }
    } else {
        LOG_ERROR("Unsupported URI Scheme: " << privateKeyUri_.scheme);
        return "";
    }

    SHA256((unsigned char *)unsignedToken, unsignedTokenString.length(), hash);
    RSA_sign(NID_sha256, hash, SHA256_DIGEST_LENGTH, signature, &siglen, privateKey);

    std::string principalToken = unsignedTokenString + ";s=" + ybase64Encode(signature, siglen);
    LOG_DEBUG("Created signed principal token: " << principalToken);

    RSA_free(privateKey);

    return principalToken;
}

static size_t curlWriteCallback(void *contents, size_t size, size_t nmemb, void *responseDataPtr) {
    ((std::string *)responseDataPtr)->append((char *)contents, size * nmemb);
    return size * nmemb;
}

static std::mutex cacheMtx_;
const std::string ZTSClient::getRoleToken() const {
    RoleToken roleToken;
    std::string cacheKey = "p=" + tenantDomain_ + "." + tenantService_ + ";d=" + providerDomain_;

    // locked block
    {
        std::lock_guard<std::mutex> lock(cacheMtx_);
        roleToken = roleTokenCache_[cacheKey];
    }

    if (!roleToken.token.empty() && roleToken.expiryTime > (long long)time(NULL) + FETCH_EPSILON) {
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
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, REQUEST_TIMEOUT);

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

    switch (res) {
        case CURLE_OK:
            long response_code;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
            LOG_DEBUG("Response received for url " << completeUrl << " code " << response_code);
            if (response_code == 200) {
                ptree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    ptree::read_json(stream, root);
                } catch (ptree::json_parser_error &e) {
                    LOG_ERROR("Failed to parse json of ZTS response: " << e.what()
                                                                       << "\nInput Json = " << responseData);
                    break;
                }

                roleToken.token = root.get<std::string>("token");
                roleToken.expiryTime = root.get<uint32_t>("expiryTime");
                std::lock_guard<std::mutex> lock(cacheMtx_);
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

const std::string ZTSClient::getHeader() const { return roleHeader_; }

PrivateKeyUri ZTSClient::parseUri(const char *uri) {
    PrivateKeyUri uriSt;
    // scheme mediatype[;base64] path file
    static const boost::regex expression(
        "^(\?:([^:/\?#]+):)(\?:([;/\\-\\w]*),)\?(/\?(\?:[^\?#/]*/)*)\?([^\?#]*)");
    boost::cmatch groups;
    if (boost::regex_match(uri, groups, expression)) {
        uriSt.scheme = groups.str(1);
        uriSt.mediaTypeAndEncodingType = groups.str(2);
        uriSt.data = groups.str(4);
        uriSt.path = groups.str(3) + groups.str(4);
    }
    return uriSt;
}
}  // namespace pulsar
