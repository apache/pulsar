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

    /**
     * @return true if the authentication data contains data for TLS
     */
    virtual bool hasDataForTls();

    /**
     * @return a client certificate chain or “none” if the data is not available
     */
    virtual std::string getTlsCertificates();

    /**
     * @return a private key for the client certificate or “none” if the data is not available
     */
    virtual std::string getTlsPrivateKey();

    /**
     * @return true if this authentication data contains data for HTTP
     */
    virtual bool hasDataForHttp();

    /**
     * @return an authentication scheme or “none” if the request is not authenticated
     */
    virtual std::string getHttpAuthType();

    /**
     * @return the string of HTTP header or “none” if the request is not authenticated
     */
    virtual std::string getHttpHeaders();

    /**
     * @return true if authentication data contains data from Pulsar protocol
     */
    virtual bool hasDataFromCommand();

    /**
     * @return authentication data which is stored in a command
     */
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

    /**
     * @return the authentication method name supported by this provider
     */
    virtual const std::string getAuthMethodName() const = 0;

    /**
     * Get AuthenticationData from the current instance
     *
     * @param[out] authDataContent the shared pointer of AuthenticationData. The content of AuthenticationData
     * is changed to the internal data of the current instance.
     * @return ResultOk or ResultAuthenticationError if authentication failed
     */
    virtual Result getAuthData(AuthenticationDataPtr& authDataContent) {
        authDataContent = authData_;
        return ResultOk;
    }

    /**
     * Parse the authentication parameter string to a map whose key and value are both strings
     *
     * The parameter string can have multiple lines. The format of each line is a comma-separated “key:value”
     * string.
     *
     * For example, “k1:v1,k2:v2” is parsed to two key-value pairs `(k1, v1)` and `(k2, v2)`.
     *
     * @param authParamsString the authentication parameter string to be parsed
     * @return the parsed map whose key and value are both strings
     */
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
     * Create an AuthenticationPtr with an empty ParamMap
     *
     * @see create(const std::string&, const ParamMap&)
     */
    static AuthenticationPtr create(const std::string& pluginNameOrDynamicLibPath);

    /**
     * Create an AuthenticationPtr with a ParamMap that is converted from authParamsString
     *
     * @see Authentication::parseDefaultFormatAuthParams
     * @see create(const std::string&, const ParamMap&)
     */
    static AuthenticationPtr create(const std::string& pluginNameOrDynamicLibPath,
                                    const std::string& authParamsString);

    /**
     * Create an AuthenticationPtr
     *
     * When the first parameter represents the plugin name, the type of authentication can be one of the
     * following:
     * - AuthTls (if the plugin name is “tls”)
     * - AuthToken (if the plugin name is “token” or “org.apache.pulsar.client.impl.auth.AuthenticationToken”)
     * - AuthAthenz (if the plugin name is “athenz” or
     * “org.apache.pulsar.client.impl.auth.AuthenticationAthenz”)
     * - AuthOauth2 (if the plugin name is “oauth2token” or
     * “org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2”)
     *
     * @param pluginNameOrDynamicLibPath the plugin name or the path or a dynamic library that contains the
     * implementation of Authentication
     * @param params the ParamMap that is passed to Authentication::create method
     */
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

    /**
     * Create an AuthTls with a ParamMap
     *
     * It is equal to create(params[“tlsCertFile”], params[“tlsKeyFile”])
     * @see create(const std::string&, const std::string&)
     */
    static AuthenticationPtr create(ParamMap& params);

    /**
     * Create an AuthTls with an authentication parameter string
     *
     * @see Authentication::parseDefaultFormatAuthParams
     */
    static AuthenticationPtr create(const std::string& authParamsString);

    /**
     * Create an AuthTls with the required parameters
     *
     * @param certificatePath the file path for a client certificate
     * @param privateKeyPath the file path for a client private key
     */
    static AuthenticationPtr create(const std::string& certificatePath, const std::string& privateKeyPath);

    /**
     * @return “tls”
     */
    const std::string getAuthMethodName() const;

    /**
     * Get AuthenticationData from the current instance
     *
     * @param[out] authDataTls the shared pointer of AuthenticationData. The content of AuthenticationData is
     * changed to the internal data of the current instance.
     * @return ResultOk
     */
    Result getAuthData(AuthenticationDataPtr& authDataTls);

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

    /**
     * Create an AuthToken with a ParamMap
     *
     * @param parameters it must contain a key-value, where key means how to get the token and value means the
     * token source
     *
     * If the key is “token”, the value is the token
     *
     * If the key is “file”, the value is the file that contains the token
     *
     * If the key is “env”, the value is the environment variable whose value is the token
     *
     * Otherwise, a `std::runtime_error` error is thrown.
     * @see create(const std::string& authParamsString)
     */
    static AuthenticationPtr create(ParamMap& params);

    /**
     * Create an AuthToken with an authentication parameter string
     *
     * @see Authentication::parseDefaultFormatAuthParams
     */
    static AuthenticationPtr create(const std::string& authParamsString);

    /**
     * Create an authentication provider for token based authentication
     *
     * @param token
     *            a string containing the auth token
     */
    static AuthenticationPtr createWithToken(const std::string& token);

    /**
     * Create an authentication provider for token based authentication
     *
     * @param tokenSupplier
     *            a supplier of the client auth token
     */
    static AuthenticationPtr create(const TokenSupplier& tokenSupplier);

    /**
     * @return “token”
     */
    const std::string getAuthMethodName() const;

    /**
     * Get AuthenticationData from the current instance
     *
     * @param[out] authDataToken the shared pointer of AuthenticationData. The content of AuthenticationData
     * is changed to the internal data of the current instance.
     * @return ResultOk
     */
    Result getAuthData(AuthenticationDataPtr& authDataToken);

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

    /**
     * Create an AuthAthenz with a ParamMap
     *
     * The required parameter keys are “tenantDomain”, “tenantService”, “providerDomain”, “privateKey”, and
     * “ztsUrl”
     *
     * @param params the key-value to construct ZTS client
     * @see http://pulsar.apache.org/docs/en/security-athenz/
     */
    static AuthenticationPtr create(ParamMap& params);

    /**
     * Create an AuthAthenz with an authentication parameter string
     *
     * @see Authentication::parseDefaultFormatAuthParams
     */
    static AuthenticationPtr create(const std::string& authParamsString);

    /**
     * @return “athenz”
     */
    const std::string getAuthMethodName() const;

    /**
     * Get AuthenticationData from the current instance
     *
     * @param[out] authDataAthenz the shared pointer of AuthenticationData. The content of AuthenticationData
     * is changed to the internal data of the current instance.
     * @return ResultOk
     */
    Result getAuthData(AuthenticationDataPtr& authDataAthenz);

   private:
    AuthenticationDataPtr authDataAthenz_;
};

// OAuth 2.0 token and associated information.
// currently mainly works for access token
class Oauth2TokenResult {
   public:
    enum
    {
        undefined_expiration = -1
    };

    Oauth2TokenResult();
    ~Oauth2TokenResult();

    /**
     * Set the access token string
     *
     * @param accessToken the access token string
     */
    Oauth2TokenResult& setAccessToken(const std::string& accessToken);

    /**
     * Set the ID token
     *
     * @param idToken the ID token
     */
    Oauth2TokenResult& setIdToken(const std::string& idToken);

    /**
     * Set the refresh token which can be used to obtain new access tokens using the same authorization grant
     * or null for none
     *
     * @param refreshToken the refresh token
     */
    Oauth2TokenResult& setRefreshToken(const std::string& refreshToken);

    /**
     * Set the token lifetime
     *
     * @param expiresIn the token lifetime
     */
    Oauth2TokenResult& setExpiresIn(const int64_t expiresIn);

    /**
     * @return the access token string
     */
    const std::string& getAccessToken() const;

    /**
     * @return the ID token
     */
    const std::string& getIdToken() const;

    /**
     * @return the refresh token which can be used to obtain new access tokens using the same authorization
     * grant or null for none
     */
    const std::string& getRefreshToken() const;

    /**
     * @return the token lifetime in milliseconds
     */
    int64_t getExpiresIn() const;

   private:
    // map to json "access_token"
    std::string accessToken_;
    // map to json "id_token"
    std::string idToken_;
    // map to json "refresh_token"
    std::string refreshToken_;
    // map to json "expires_in"
    int64_t expiresIn_;
};

typedef std::shared_ptr<Oauth2TokenResult> Oauth2TokenResultPtr;

class Oauth2Flow {
   public:
    virtual ~Oauth2Flow();

    /**
     * Initializes the authorization flow.
     */
    virtual void initialize() = 0;

    /**
     * Acquires an access token from the OAuth 2.0 authorization server.
     * @return a token result including an access token.
     */
    virtual Oauth2TokenResultPtr authenticate() = 0;

    /**
     * Closes the authorization flow.
     */
    virtual void close() = 0;

   protected:
    Oauth2Flow();
};

typedef std::shared_ptr<Oauth2Flow> FlowPtr;

class CachedToken {
   public:
    virtual ~CachedToken();

    /**
     * @return true if the token has expired
     */
    virtual bool isExpired() = 0;

    /**
     * Get AuthenticationData from the current instance
     *
     * @return ResultOk or ResultAuthenticationError if authentication failed
     */
    virtual AuthenticationDataPtr getAuthData() = 0;

   protected:
    CachedToken();
};

typedef std::shared_ptr<CachedToken> CachedTokenPtr;

/**
 * Oauth2 based implementation of Pulsar client authentication.
 * Passed in parameter would be like:
 * ```
 *   "type": "client_credentials",
 *   "issuer_url": "https://accounts.google.com",
 *   "client_id": "d9ZyX97q1ef8Cr81WHVC4hFQ64vSlDK3",
 *   "client_secret": "on1uJ...k6F6R",
 *   "audience": "https://broker.example.com"
 *  ```
 *  If passed in as std::string, it should be in Json format.
 */
class PULSAR_PUBLIC AuthOauth2 : public Authentication {
   public:
    AuthOauth2(ParamMap& params);
    ~AuthOauth2();

    /**
     * Create an AuthOauth2 with a ParamMap
     *
     * The required parameter keys are “issuer_url”, “private_key”, and “audience”
     *
     * @param parameters the key-value to create OAuth 2.0 client credentials
     * @see http://pulsar.apache.org/docs/en/security-oauth2/#client-credentials
     */
    static AuthenticationPtr create(ParamMap& params);

    /**
     * Create an AuthOauth2 with an authentication parameter string
     *
     * @see Authentication::parseDefaultFormatAuthParams
     */
    static AuthenticationPtr create(const std::string& authParamsString);

    /**
     * @return “token”
     */
    const std::string getAuthMethodName() const;

    /**
     * Get AuthenticationData from the current instance
     *
     * @param[out] authDataOauth2 the shared pointer of AuthenticationData. The content of AuthenticationData
     * is changed to the internal data of the current instance.
     * @return ResultOk or ResultAuthenticationError if authentication failed
     */
    Result getAuthData(AuthenticationDataPtr& authDataOauth2);

   private:
    FlowPtr flowPtr_;
    CachedTokenPtr cachedTokenPtr_;
};

}  // namespace pulsar

#endif /* PULSAR_AUTHENTICATION_H_ */
