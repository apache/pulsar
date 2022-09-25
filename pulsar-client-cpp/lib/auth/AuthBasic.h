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

const std::string BASIC_PLUGIN_NAME = "basic";
const std::string BASIC_JAVA_PLUGIN_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationBasic";

class AuthDataBasic : public AuthenticationDataProvider {
   public:
    AuthDataBasic(const std::string& username, const std::string& password);
    ~AuthDataBasic();

    bool hasDataForHttp();
    std::string getHttpHeaders();
    bool hasDataFromCommand();
    std::string getCommandData();

   private:
    std::string commandAuthToken_;
    std::string httpAuthToken_;
};

}  // namespace pulsar
