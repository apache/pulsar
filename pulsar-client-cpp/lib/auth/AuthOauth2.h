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

#pragma once

#include <pulsar/Authentication.h>
#include <string>
#include <boost/function.hpp>

namespace pulsar {

const std::string OAUTH2_TOKEN_PLUGIN_NAME = "oauth2token";
const std::string OAUTH2_TOKEN_JAVA_PLUGIN_NAME =
    "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2";

class ClientCredentialFlow : public Oauth2Flow {
   public:
    ClientCredentialFlow(const std::string& issuerUrl, const std::string& clientId,
                         const std::string& clientSecret, const std::string& audience);
    ClientCredentialFlow(const std::string& issuerUrl, const std::string& credentialsFilePath,
                         const std::string& audience);
    void initialize();
    Oauth2TokenResultPtr authenticate();
    void close();

   private:
    std::string tokenEndPoint_;
    std::string issuerUrl_;
    std::string clientId_;
    std::string clientSecret_;
    std::string audience_;
};

class Oauth2CachedToken : public CachedToken {
   public:
    Oauth2CachedToken(Oauth2TokenResultPtr token);
    ~Oauth2CachedToken();
    bool isExpired();
    AuthenticationDataPtr getAuthData();

   private:
    int64_t expiresAt_;
    Oauth2TokenResultPtr latest_;
    AuthenticationDataPtr authData_;
};

class AuthDataOauth2 : public AuthenticationDataProvider {
   public:
    AuthDataOauth2(const std::string& accessToken);
    ~AuthDataOauth2();

    bool hasDataForHttp();
    std::string getHttpHeaders();
    bool hasDataFromCommand();
    std::string getCommandData();

   private:
    std::string accessToken_;
};

}  // namespace pulsar
