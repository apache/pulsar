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

const std::string TOKEN_PLUGIN_NAME = "token";
const std::string TOKEN_JAVA_PLUGIN_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationToken";

class AuthDataToken : public AuthenticationDataProvider {
   public:
    AuthDataToken(const std::string& token);
    AuthDataToken(const TokenSupplier& tokenSupplier);
    ~AuthDataToken();

    bool hasDataForHttp();
    std::string getHttpHeaders();
    bool hasDataFromCommand();
    std::string getCommandData();

   private:
    TokenSupplier tokenSupplier_;
};

}  // namespace pulsar
