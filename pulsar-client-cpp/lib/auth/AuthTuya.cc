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
#include "AuthTuya.h"
#include "md5.h"

#include <functional>

#include <sstream>
#include <fstream>

namespace pulsar {

// AuthTuya

AuthDataTuya::AuthDataTuya(const std::string& id, const std::string& key) {
    accessId_ = id;
    accessKey_ = key;
}

AuthDataTuya::~AuthDataTuya() {}

bool AuthDataTuya::hasDataForHttp() { return false; }
bool AuthDataTuya::hasDataForTuya() { return true; }
std::string AuthDataTuya::getTuyaAccessId() { return accessId_; }

std::string AuthDataTuya::getTuyaAccessKey() { return accessKey_; }

AuthTuya::AuthTuya(AuthenticationDataPtr& authDataTuya) { authDataTuya_ = authDataTuya;}

AuthTuya::~AuthTuya() {}

AuthenticationPtr AuthTuya::create(const std::string& authParamsString) {
    ParamMap params = parseDefaultFormatAuthParams(authParamsString);
    return create(params);
}

AuthenticationPtr AuthTuya::create(ParamMap& params) {
    return create(params["accessId"], params["accessKey"]);
}

AuthenticationPtr AuthTuya::create(const std::string& id, const std::string& key){
    AuthenticationDataPtr authDataTuya = AuthenticationDataPtr(new AuthDataTuya(id, key));
    return AuthenticationPtr(new AuthTuya(authDataTuya));
}

bool AuthDataTuya::hasDataFromCommand() { return true; }

std::string AuthDataTuya::getCommandData() { return "{\"username\":\"84dj9ppwvvdyrf4e4sxq\",\"password\":\"c87f8f12fa5a097b\"}"; }

const std::string AuthTuya::getAuthMethodName() const { return "auth1"; }

void AuthDataTuya::AuthenticationDataProvider(const std::string& id, const std::string& key){
//    commandData = md5(this->accessId_.append(this->accessId_.substr(8, 24)));
    this->commandData = "{\"username\":\"84dj9ppwvvdyrf4e4sxq\",\"password\":\"c87f8f12fa5a097b\"}";
}

Result AuthTuya::getAuthData(AuthenticationDataPtr& authDataContent) const {
    authDataContent = authDataTuya_;
    return ResultOk;
}



}  // namespace pulsar

