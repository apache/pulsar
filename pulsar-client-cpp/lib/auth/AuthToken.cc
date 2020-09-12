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
#include "AuthToken.h"

#include <boost/algorithm/string/predicate.hpp>
#include <functional>
#include <stdexcept>

#include <sstream>
#include <fstream>

namespace pulsar {

// AuthDataToken

AuthDataToken::AuthDataToken(const TokenSupplier &tokenSupplier) { tokenSupplier_ = tokenSupplier; }

AuthDataToken::~AuthDataToken() {}

bool AuthDataToken::hasDataForHttp() { return true; }

std::string AuthDataToken::getHttpHeaders() { return "Authorization: Bearer " + tokenSupplier_(); }

bool AuthDataToken::hasDataFromCommand() { return true; }

std::string AuthDataToken::getCommandData() { return tokenSupplier_(); }

static std::string readDirect(const std::string &token) { return token; }

static std::string readFromFile(const std::string &tokenFilePath) {
    std::ifstream input(tokenFilePath);
    std::stringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

static std::string readFromEnv(const std::string &envVarName) {
    char *value = getenv(envVarName.c_str());
    if (!value) {
        throw std::runtime_error("Failed to read environment variable " + envVarName);
    }
    return std::string(value);
}

// AuthToken

AuthToken::AuthToken(AuthenticationDataPtr &authDataToken) { authDataToken_ = authDataToken; }

AuthToken::~AuthToken() {}

AuthenticationPtr AuthToken::create(ParamMap &params) {
    if (params.find("token") != params.end()) {
        return create(std::bind(&readDirect, params["token"]));
    } else if (params.find("file") != params.end()) {
        // Read token from a file
        return create(std::bind(&readFromFile, params["file"]));
    } else if (params.find("env") != params.end()) {
        // Read token from environment variable
        std::string envVarName = params["env"];
        return create(std::bind(&readFromEnv, envVarName));
    } else {
        throw std::runtime_error("Invalid configuration for token provider");
    }
}

AuthenticationPtr AuthToken::create(const std::string &authParamsString) {
    ParamMap params;
    if (boost::starts_with(authParamsString, "token:")) {
        std::string token = authParamsString.substr(strlen("token:"));
        params["token"] = token;
    } else if (boost::starts_with(authParamsString, "file:")) {
        // Read token from a file
        std::string filePath = authParamsString.substr(strlen("file://"));
        params["file"] = filePath;
    } else if (boost::starts_with(authParamsString, "env:")) {
        std::string envVarName = authParamsString.substr(strlen("env:"));
        params["env"] = envVarName;
    } else {
        std::string token = authParamsString;
        params["token"] = token;
    }

    return create(params);
}

AuthenticationPtr AuthToken::createWithToken(const std::string &token) {
    return create(std::bind(&readDirect, token));
}

AuthenticationPtr AuthToken::create(const TokenSupplier &tokenSupplier) {
    AuthenticationDataPtr authDataToken = AuthenticationDataPtr(new AuthDataToken(tokenSupplier));
    return AuthenticationPtr(new AuthToken(authDataToken));
}

const std::string AuthToken::getAuthMethodName() const { return "token"; }

Result AuthToken::getAuthData(AuthenticationDataPtr &authDataContent) {
    authDataContent = authDataToken_;
    return ResultOk;
}

}  // namespace pulsar
