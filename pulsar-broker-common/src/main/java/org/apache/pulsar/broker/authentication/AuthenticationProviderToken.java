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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;

import java.io.IOException;
import java.security.Key;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;

public class AuthenticationProviderToken implements AuthenticationProvider {

    public final static String HTTP_HEADER_NAME = "Authorization";
    final static String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // When simmetric key is configured
    final static String CONF_TOKEN_SECRET_KEY = "tokenSecretKey";

    // When public/private key pair is configured
    final static String CONF_TOKEN_PUBLIC_KEY = "tokenPublicKey";

    private Key validationKey;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        this.validationKey = getValidationKey(config);
    }

    @Override
    public String getAuthMethodName() {
        return "token";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        String token = null;

        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            token = authData.getCommandData();
        } else if (authData.hasDataFromHttp()) {
            // Authentication HTTP request. The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg:
            //
            // Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (!httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                throw new AuthenticationException("Invalid HTTP Authorization header");
            }

            // Remove prefix
            token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
        } else {
            throw new AuthenticationException("No token credentials passed");
        }

        // Validate the token
        try {
            @SuppressWarnings("unchecked")
            Jwt<?, Claims> jwt = Jwts.parser()
                    .setSigningKey(validationKey)
                    .parse(token);

            return jwt.getBody().getSubject();
        } catch (JwtException e) {
            throw new AuthenticationException("Failed to authentication token: " + e.getMessage());
        }
    }

    /**
     * Try to get the validation key for tokens from several possible config options.
     */
    private static Key getValidationKey(ServiceConfiguration conf) throws IOException {
        final boolean isPublicKey;
        final String validationKeyConfig;

        if (conf.getProperty(CONF_TOKEN_SECRET_KEY) != null) {
            isPublicKey = false;
            validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_SECRET_KEY);
        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY) != null) {
            isPublicKey = true;
            validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY);
        } else {
            throw new IOException("No secret key was provided for token authentication");
        }

        byte[] validationKey = AuthTokenUtils.readKeyFromUrl(validationKeyConfig);

        if (isPublicKey) {
            return AuthTokenUtils.decodePublicKey(validationKey);
        } else {
            return AuthTokenUtils.decodeSecretKey(validationKey);
        }
    }
}
