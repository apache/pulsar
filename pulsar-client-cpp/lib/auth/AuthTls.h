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

namespace pulsar {

const std::string TLS_PLUGIN_NAME = "tls";
const std::string TLS_JAVA_PLUGIN_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationTls";

class AuthDataTls : public AuthenticationDataProvider {
   public:
    AuthDataTls(const std::string& certificatePath, const std::string& privateKeyPath);
    ~AuthDataTls();

    bool hasDataForTls();
    std::string getTlsCertificates();
    std::string getTlsPrivateKey();

   private:
    std::string tlsCertificate_;
    std::string tlsPrivateKey_;
};

}  // namespace pulsar
