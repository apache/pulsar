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
#include <lib/auth/AuthTls.h>

namespace pulsar {
AuthDataTls::AuthDataTls(const std::string& certificatePath, const std::string& privateKeyPath) {
    tlsCertificate_ = certificatePath;
    tlsPrivateKey_ = privateKeyPath;
}

AuthDataTls::~AuthDataTls() {}

bool AuthDataTls::hasDataForTls() { return true; }

std::string AuthDataTls::getTlsCertificates() { return tlsCertificate_; }

std::string AuthDataTls::getTlsPrivateKey() { return tlsPrivateKey_; }

AuthTls::AuthTls(AuthenticationDataPtr& authDataTls) { authDataTls_ = authDataTls; }

AuthTls::~AuthTls() {}

AuthenticationPtr AuthTls::create(const std::string& authParamsString) {
    ParamMap params = parseDefaultFormatAuthParams(authParamsString);
    return create(params);
}

AuthenticationPtr AuthTls::create(ParamMap& params) {
    return create(params["tlsCertFile"], params["tlsKeyFile"]);
}

AuthenticationPtr AuthTls::create(const std::string& certificatePath, const std::string& privateKeyPath) {
    AuthenticationDataPtr authDataTls =
        AuthenticationDataPtr(new AuthDataTls(certificatePath, privateKeyPath));
    return AuthenticationPtr(new AuthTls(authDataTls));
}

const std::string AuthTls::getAuthMethodName() const { return "tls"; }

Result AuthTls::getAuthData(AuthenticationDataPtr& authDataContent) {
    authDataContent = authDataTls_;
    return ResultOk;
}

}  // namespace pulsar
