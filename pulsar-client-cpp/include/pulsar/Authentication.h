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
#ifndef PULSAR_AUTHENTICATION_H_
#define PULSAR_AUTHENTICATION_H_

#include <vector>
#include <string>
#include <map>
#include <boost/shared_ptr.hpp>
#include <pulsar/Result.h>
#include <boost/make_shared.hpp>

#pragma GCC visibility push(default)

namespace pulsar {

    class ClientConfiguration;
    class Authentication;

    class AuthenticationDataProvider {
    public:
        virtual ~AuthenticationDataProvider();
        virtual bool hasDataForTls();
        virtual std::string getTlsCertificates();
        virtual std::string getTlsPrivateKey();
        virtual bool hasDataForHttp();
        virtual std::string getHttpAuthType();
        virtual std::string getHttpHeaders();
        virtual bool hasDataFromCommand();
        virtual std::string getCommandData();
    protected:
        AuthenticationDataProvider();
    };

    typedef boost::shared_ptr<AuthenticationDataProvider> AuthenticationDataPtr;
    typedef boost::shared_ptr<Authentication> AuthenticationPtr;
    typedef std::map<std::string, std::string> ParamMap;

    class Authentication {
    public:
        virtual ~Authentication();
        virtual const std::string getAuthMethodName() const = 0;
        virtual Result getAuthData(AuthenticationDataPtr& authDataContent) const {
            authDataContent = authData_;
            return ResultOk;
        }
    protected:
        Authentication();
        AuthenticationDataPtr authData_;
        friend class ClientConfiguration;
    };

    class AuthFactory {
    public:
        static AuthenticationPtr Disabled();
        static AuthenticationPtr create(const std::string& dynamicLibPath);
        static AuthenticationPtr create(const std::string& dynamicLibPath, const std::string& authParamsString);
        static AuthenticationPtr create(const std::string& dynamicLibPath, ParamMap& params);

    protected:
        static bool isShutdownHookRegistered_;
        static std::vector<void *> loadedLibrariesHandles_;
        static void release_handles();
    };
}
// namespace pulsar

#pragma GCC visibility pop

#endif /* PULSAR_AUTHENTICATION_H_ */
