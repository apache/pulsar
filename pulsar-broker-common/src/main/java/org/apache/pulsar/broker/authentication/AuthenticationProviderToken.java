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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Charsets;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;

import javax.naming.AuthenticationException;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;

@Slf4j
public class AuthenticationProviderToken implements AuthenticationProvider {

    public final static String HTTP_HEADER_NAME = "X-Pulsar-Auth";

    // When simmetric key is configured
    final static String CONF_TOKEN_SECRET_KEY = "tokenSecretKey";
    final static String CONF_TOKEN_SECRET_KEY_FROM_ENV = "tokenSecretKeyFromEnv";
    final static String CONF_TOKEN_SECRET_KEY_FROM_FILE = "tokenSecretKeyFromFile";

    // When public/private key pair is configured
    final static String CONF_TOKEN_PUBLIC_KEY = "tokenPublicKey";
    final static String CONF_TOKEN_PUBLIC_KEY_FROM_ENV = "tokenPublicKeyFromEnv";
    final static String CONF_TOKEN_PUBLIC_KEY_FROM_FILE = "tokenPublicKeyFromFile";

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
            // Authentication HTTP request
            token = authData.getHttpHeader(HTTP_HEADER_NAME);
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

        // First check for secret key confs
        if (conf.getProperty(CONF_TOKEN_SECRET_KEY) != null) {
            // Secret key was specified directly in confg file
            Object secretKey = conf.getProperty(CONF_TOKEN_SECRET_KEY);
            checkNotNull(secretKey);
            checkArgument(secretKey instanceof String);
            log.info("Reading secret key from config file");
            return AuthTokenUtils.decodeSecretKey((String) secretKey);

        } else if (conf.getProperty(CONF_TOKEN_SECRET_KEY_FROM_ENV) != null) {
            // Secret key was specified in ENV variable
            String variableName = (String) conf.getProperty(CONF_TOKEN_SECRET_KEY_FROM_ENV);
            log.info("Reading secret key from env variable '{}'", variableName);
            return AuthTokenUtils.decodeSecretKey(System.getenv(variableName));

        } else if (conf.getProperty(CONF_TOKEN_SECRET_KEY_FROM_FILE) != null) {
            String filePath = (String) conf.getProperty(CONF_TOKEN_SECRET_KEY_FROM_FILE);
            log.info("Reading secret key from file '{}'", filePath);
            return AuthTokenUtils
                    .decodeSecretKey(new String(Files.readAllBytes(Paths.get(filePath)), Charsets.UTF_8));

            // Then check for public key conf
        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY) != null) {
            // Public key was specified directly in confg file
            Object publicKey = conf.getProperty(CONF_TOKEN_PUBLIC_KEY);
            checkNotNull(publicKey);
            checkArgument(publicKey instanceof String);
            log.info("Reading public key from config file");
            return AuthTokenUtils.decodePublicKey((String) publicKey);

        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY_FROM_ENV) != null) {
            // Public key was specified in ENV variable
            String variableName = (String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY_FROM_ENV);
            log.info("Reading public key from env variable '{}'", variableName);
            return AuthTokenUtils.decodePublicKey(System.getenv(variableName));

        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY_FROM_FILE) != null) {
            String filePath = (String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY_FROM_FILE);
            log.info("Reading public key from file '{}'", filePath);
            return AuthTokenUtils
                    .decodePublicKey(new String(Files.readAllBytes(Paths.get(filePath)), Charsets.UTF_8));

        } else {
            throw new IOException("No secret key was provided for token authentication");
        }
    }
}
