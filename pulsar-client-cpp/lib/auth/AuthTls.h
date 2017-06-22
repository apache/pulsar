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
#ifndef PULSAR_AUTH_TLS_H_
#define PULSAR_AUTH_TLS_H_

#include <pulsar/Authentication.h>
#include <lib/LogUtils.h>
#include <iostream>
#include <string>

namespace pulsar {
    
    class AuthDataTls : public AuthenticationDataProvider {
        
    public:
        AuthDataTls(ParamMap& params);
        ~AuthDataTls();
        bool hasDataForTls();
        std::string getTlsCertificates();
        std::string getTlsPrivateKey();
    private:
        std::string tlsCertificates_;
        std::string tlsPrivateKey_;
    };
    
    class AuthTls : public Authentication {
        
    public:
        AuthTls(AuthenticationDataPtr&);
        ~AuthTls();
        static AuthenticationPtr create(ParamMap& params);
        const std::string getAuthMethodName() const;
        Result getAuthData(AuthenticationDataPtr& authDataTls) const;
    private:
        AuthenticationDataPtr authDataTls_;
    };
    
}
#endif /* PULSAR_AUTH_TLS_H_ */
