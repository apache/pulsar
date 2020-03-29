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
#include <string>
#include <map>
#include <pulsar/defines.h>
#include <lib/LogUtils.h>

namespace pulsar {

struct RoleToken {
    std::string token;
    long long expiryTime;
};

struct PrivateKeyUri {
    std::string scheme;
    std::string mediaTypeAndEncodingType;
    std::string data;
    std::string path;
};

class PULSAR_PUBLIC ZTSClient {
   public:
    ZTSClient(std::map<std::string, std::string>& params);
    const std::string getRoleToken() const;
    const std::string getHeader() const;
    ~ZTSClient();

   private:
    std::string tenantDomain_;
    std::string tenantService_;
    std::string providerDomain_;
    PrivateKeyUri privateKeyUri_;
    std::string ztsUrl_;
    std::string keyId_;
    std::string principalHeader_;
    std::string roleHeader_;
    int tokenExpirationTime_;
    static std::map<std::string, RoleToken> roleTokenCache_;
    static std::string getSalt();
    static std::string ybase64Encode(const unsigned char* input, int length);
    static char* base64Decode(const char* input);
    const std::string getPrincipalToken() const;
    static PrivateKeyUri parseUri(const char* uri);

    friend class ZTSClientWrapper;
};
}  // namespace pulsar
