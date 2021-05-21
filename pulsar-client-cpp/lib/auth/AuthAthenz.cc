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
#include <lib/auth/AuthAthenz.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
namespace ptree = boost::property_tree;

#include <sstream>

#include <functional>

DECLARE_LOG_OBJECT()

namespace pulsar {
AuthDataAthenz::AuthDataAthenz(ParamMap& params) {
    ztsClient_ = std::make_shared<ZTSClient>(std::ref(params));
    LOG_DEBUG("AuthDataAthenz is construted.")
}

bool AuthDataAthenz::hasDataForHttp() { return true; }

std::string AuthDataAthenz::getHttpHeaders() {
    return ztsClient_->getHeader() + ": " + ztsClient_->getRoleToken();
}

bool AuthDataAthenz::hasDataFromCommand() { return true; }

std::string AuthDataAthenz::getCommandData() { return ztsClient_->getRoleToken(); }

AuthDataAthenz::~AuthDataAthenz() {}

AuthAthenz::AuthAthenz(AuthenticationDataPtr& authDataAthenz) { authDataAthenz_ = authDataAthenz; }

AuthAthenz::~AuthAthenz() {}

ParamMap parseAuthParamsString(const std::string& authParamsString) {
    ParamMap params;
    if (!authParamsString.empty()) {
        ptree::ptree root;
        std::stringstream stream;
        stream << authParamsString;
        try {
            ptree::read_json(stream, root);
            for (const auto& item : root) {
                params[item.first] = item.second.get_value<std::string>();
            }
        } catch (ptree::json_parser_error& e) {
            LOG_ERROR("Invalid String Error: " << e.what());
        }
    }
    return params;
}

AuthenticationPtr AuthAthenz::create(const std::string& authParamsString) {
    ParamMap params = parseAuthParamsString(authParamsString);
    AuthenticationDataPtr authDataAthenz = AuthenticationDataPtr(new AuthDataAthenz(params));
    return AuthenticationPtr(new AuthAthenz(authDataAthenz));
}

AuthenticationPtr AuthAthenz::create(ParamMap& params) {
    AuthenticationDataPtr authDataAthenz = AuthenticationDataPtr(new AuthDataAthenz(params));
    return AuthenticationPtr(new AuthAthenz(authDataAthenz));
}

const std::string AuthAthenz::getAuthMethodName() const { return "athenz"; }

Result AuthAthenz::getAuthData(AuthenticationDataPtr& authDataContent) {
    authDataContent = authDataAthenz_;
    return ResultOk;
}

extern "C" Authentication* create(const std::string& authParamsString) {
    ParamMap params = parseAuthParamsString(authParamsString);
    AuthenticationDataPtr authDataAthenz = AuthenticationDataPtr(new AuthDataAthenz(params));
    return new AuthAthenz(authDataAthenz);
}

}  // namespace pulsar
