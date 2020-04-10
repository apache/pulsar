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
package org.apache.pulsar.broker.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.Key;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.api.AuthData;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.SignatureException;

public class AuthenticationProviderToken implements AuthenticationProvider {

    final static String HTTP_HEADER_NAME = "Authorization";
    final static String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // When symmetric key is configured
    final static String CONF_TOKEN_SECRET_KEY = "tokenSecretKey";

    // When public/private key pair is configured
    final static String CONF_TOKEN_PUBLIC_KEY = "tokenPublicKey";

    // The token's claim that corresponds to the "role" string
    final static String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";

    // When using public key's, the algorithm of the key
    final static String CONF_TOKEN_PUBLIC_ALG = "tokenPublicAlg";

    final static String TOKEN = "token";

    private Key validationKey;
    private String roleClaim;
    private SignatureAlgorithm publicKeyAlg;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException, IllegalArgumentException {
        // we need to fetch the algorithm before we fetch the key
        this.publicKeyAlg = getPublicKeyAlgType(config);
        this.validationKey = getValidationKey(config);
        this.roleClaim = getTokenRoleClaim(config);
    }

    @Override
    public String getAuthMethodName() {
        return TOKEN;
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        // Get Token
        String token = getToken(authData);

        // Parse Token by validating
        return getPrincipal(authenticateToken(token));
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
            throws AuthenticationException {
        return new TokenAuthenticationState(this, authData, remoteAddress, sslSession);
    }

    public static String getToken(AuthenticationDataSource authData) throws AuthenticationException {
        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            return validateToken(authData.getCommandData());
        } else if (authData.hasDataFromHttp()) {
            // Authentication HTTP request. The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue == null || !httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                throw new AuthenticationException("Invalid HTTP Authorization header");
            }

            // Remove prefix
            String token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
            return validateToken(token);
        } else {
            throw new AuthenticationException("No token credentials passed");
        }
    }

    private static String validateToken(final String token) throws AuthenticationException {
        if (StringUtils.isNotBlank(token)) {
            return token;
        } else {
            throw new AuthenticationException("Blank token found");
        }
    }

    @SuppressWarnings("unchecked")
    private Jwt<?, Claims> authenticateToken(final String token) throws AuthenticationException {
        try {
            return Jwts.parser()
                    .setSigningKey(validationKey)
                    .parse(token);
        } catch (JwtException e) {
            throw new AuthenticationException("Failed to authentication token: " + e.getMessage());
        }
    }

    private String getPrincipal(Jwt<?, Claims> jwt) {
        return jwt.getBody().get(roleClaim, String.class);
    }

    /**
     * Try to get the validation key for tokens from several possible config options.
     */
    private Key getValidationKey(ServiceConfiguration conf) throws IOException {
        if (conf.getProperty(CONF_TOKEN_SECRET_KEY) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_SECRET_KEY))) {
            final String validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_SECRET_KEY);
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(validationKeyConfig);
            return AuthTokenUtils.decodeSecretKey(validationKey);
        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY))) {
            final String validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY);
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(validationKeyConfig);
            return AuthTokenUtils.decodePublicKey(validationKey, publicKeyAlg);
        } else {
            throw new IOException("No secret key was provided for token authentication");
        }
    }

    private String getTokenRoleClaim(ServiceConfiguration conf) throws IOException {
        if (conf.getProperty(CONF_TOKEN_AUTH_CLAIM) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_AUTH_CLAIM))) {
            return (String) conf.getProperty(CONF_TOKEN_AUTH_CLAIM);
        } else {
            return Claims.SUBJECT;
        }
    }

    private SignatureAlgorithm getPublicKeyAlgType(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(CONF_TOKEN_PUBLIC_ALG) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_PUBLIC_ALG))) {
            String alg = (String) conf.getProperty(CONF_TOKEN_PUBLIC_ALG);
            try {
                return SignatureAlgorithm.forName(alg);
            } catch (SignatureException ex) {
                throw new IllegalArgumentException("invalid algorithm provided " + alg, ex);
            }
        } else {
            return SignatureAlgorithm.RS256;
        }
    }

    private static final class TokenAuthenticationState implements AuthenticationState {
        private final AuthenticationProviderToken provider;
        private AuthenticationDataSource authenticationDataSource;
        private Jwt<?, Claims> jwt;
        private final SocketAddress remoteAddress;
        private final SSLSession sslSession;
        private long expiration;

        TokenAuthenticationState(
                AuthenticationProviderToken provider,
                AuthData authData,
                SocketAddress remoteAddress,
                SSLSession sslSession) throws AuthenticationException {
            this.provider = provider;
            this.remoteAddress = remoteAddress;
            this.sslSession = sslSession;
            this.authenticate(authData);
        }

        @Override
        public String getAuthRole() throws AuthenticationException {
            return provider.getPrincipal(jwt);
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            String token = new String(authData.getBytes(), UTF_8);

            this.jwt = provider.authenticateToken(token);
            this.authenticationDataSource = new AuthenticationDataCommand(token, remoteAddress, sslSession);
            if (jwt.getBody().getExpiration() != null) {
                this.expiration = jwt.getBody().getExpiration().getTime();
            } else {
                // Disable expiration
                this.expiration = Long.MAX_VALUE;
            }

            // There's no additional auth stage required
            return null;
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return authenticationDataSource;
        }

        @Override
        public boolean isComplete() {
            // The authentication of tokens is always done in one single stage
            return true;
        }

        @Override
        public boolean isExpired() {
            return expiration < System.currentTimeMillis();
        }
    }
}
