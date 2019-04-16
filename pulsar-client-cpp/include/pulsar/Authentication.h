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

#include <pulsar/defines.h>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <pulsar/Result.h>
#include <functional>

namespace pulsar {

class ClientConfiguration;
class Authentication;

class PULSAR_PUBLIC AuthenticationDataProvider {
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

typedef std::shared_ptr<AuthenticationDataProvider> AuthenticationDataPtr;
typedef std::shared_ptr<Authentication> AuthenticationPtr;
typedef std::map<std::string, std::string> ParamMap;

class PULSAR_PUBLIC Authentication {
   public:
    virtual ~Authentication();
    virtual const std::string getAuthMethodName() const = 0;
    virtual Result getAuthData(AuthenticationDataPtr& authDataContent) const {
        authDataContent = authData_;
        return ResultOk;
    }
    static ParamMap parseDefaultFormatAuthParams(const std::string& authParamsString);

   protected:
    Authentication();
    AuthenticationDataPtr authData_;
    friend class ClientConfiguration;
};

/**
 * AuthFactory is used to create instances of Authentication class when
 * configuring a Client instance. It loads the authentication from an
 * external plugin.
 *
 * To use authentication methods that are internally supported, you should
 * use `AuthTls::create("my-cert.pem", "my-private.key")` or similar.
 */
class PULSAR_PUBLIC AuthFactory {
   public:
    static AuthenticationPtr Disabled();

    /**
     * Create
     * @param dynamicLibPath
     * @return
     */
    static AuthenticationPtr create(const std::string& pluginNameOrDynamicLibPath);
    static AuthenticationPtr create(const std::string& pluginNameOrDynamicLibPath,
                                    const std::string& authParamsString);
    static AuthenticationPtr create(const std::string& pluginNameOrDynamicLibPath, ParamMap& params);

   protected:
    static bool isShutdownHookRegistered_;
    static std::vector<void*> loadedLibrariesHandles_;
    static void release_handles();
};

/**
 * TLS implementation of Pulsar client authentication
 */
class PULSAR_PUBLIC AuthTls : public Authentication {
   public:
    AuthTls(AuthenticationDataPtr&);
    ~AuthTls();
    static AuthenticationPtr create(ParamMap& params);
    static AuthenticationPtr create(const std::string& authParamsString);
    static AuthenticationPtr create(const std::string& certificatePath, const std::string& privateKeyPath);
    const std::string getAuthMethodName() const;
    Result getAuthData(AuthenticationDataPtr& authDataTls) const;

   private:
    AuthenticationDataPtr authDataTls_;
};

typedef std::function<std::string()> TokenSupplier;

/**
 * Token based implementation of Pulsar client authentication
 */
class PULSAR_PUBLIC AuthToken : public Authentication {
   public:
    AuthToken(AuthenticationDataPtr&);
    ~AuthToken();

    static AuthenticationPtr create(ParamMap& params);

    static AuthenticationPtr create(const std::string& authParamsString);

    /**
     * Create an authentication provider for token based authentication.
     *
     * @param token
     *            a string containing the auth token
     */
    static AuthenticationPtr createWithToken(const std::string& token);

    /**
     * Create an authentication provider for token based authentication.
     *
     * @param tokenSupplier
     *            a supplier of the client auth token
     */
    static AuthenticationPtr create(const TokenSupplier& tokenSupplier);

    const std::string getAuthMethodName() const;
    Result getAuthData(AuthenticationDataPtr& authDataToken) const;

   private:
    AuthenticationDataPtr authDataToken_;
};

/**
 * Athenz implementation of Pulsar client authentication
 */
class PULSAR_PUBLIC AuthAthenz : public Authentication {
   public:
    AuthAthenz(AuthenticationDataPtr&);
    ~AuthAthenz();
    static AuthenticationPtr create(ParamMap& params);
    static AuthenticationPtr create(const std::string& authParamsString);
    const std::string getAuthMethodName() const;
    Result getAuthData(AuthenticationDataPtr& authDataAthenz) const;

   private:
    AuthenticationDataPtr authDataAthenz_;
};

}  // namespace pulsar

#endif /* PULSAR_AUTHENTICATION_H_ */
