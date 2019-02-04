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
#ifndef PULSAR_AUTH_ATHENZ_H_
#define PULSAR_AUTH_ATHENZ_H_

#include <pulsar/Authentication.h>
#include <lib/auth/athenz/ZTSClient.h>
#include <string>

namespace pulsar {

const std::string ATHENZ_PLUGIN_NAME = "athenz";
const std::string ATHENZ_JAVA_PLUGIN_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationAthenz";

class AuthDataAthenz : public AuthenticationDataProvider {
   public:
    AuthDataAthenz(ParamMap& params);
    bool hasDataForHttp();
    std::string getHttpHeaders();
    bool hasDataFromCommand();
    std::string getCommandData();
    ~AuthDataAthenz();

   private:
    std::shared_ptr<ZTSClient> ztsClient_;
};

}  // namespace pulsar
#endif /* PULSAR_AUTH_ATHENZ_H_ */
