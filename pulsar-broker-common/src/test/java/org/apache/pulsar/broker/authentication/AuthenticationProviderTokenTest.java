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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.base.Charsets;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.sql.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;

import org.apache.commons.io.FileUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.testng.annotations.Test;

public class AuthenticationProviderTokenTest {

    @Test
    public void testInvalidInitialize() throws Exception {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        try {
            provider.initialize(new ServiceConfiguration());
            fail("should have failed");
        } catch (IOException e) {
            // Expected, secret key was not defined
        }

        provider.close();
    }

    @Test
    public void testAuthSecretKey() throws Exception {
        String secretKeyStr = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        SecretKey secretKey = AuthTokenUtils.deserializeSecretKey(secretKeyStr);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        assertEquals(provider.getAuthMethodName(), "token");

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, secretKeyStr);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        try {
            provider.authenticate(new AuthenticationDataSource() {
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, no credential passed
        }

        String token = AuthTokenUtils.createToken(secretKey, "my-test-subject", Optional.empty());

        // Pulsar protocol auth
        String subject = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(subject, "my-test-subject");

        // HTTP protocol auth
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("X-Pulsar-Auth")) {
                    return token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        });
        assertEquals(subject, "my-test-subject");

        // Expired token. This should be rejected by the authentication provider
        String expiredToken = AuthTokenUtils.createToken(secretKey, "my-test-subject",
                Optional.of(new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))));

        // Pulsar protocol auth
        try {
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return expiredToken;
                }
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, token was expired
        }

        provider.close();
    }

    @Test
    public void testAuthSecretKeyFromFile() throws Exception {
        String secretKeyStr = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        SecretKey secretKey = AuthTokenUtils.deserializeSecretKey(secretKeyStr);

        File secretKeyFile = File.createTempFile("pular-test-secret-key-", ".key");
        secretKeyFile.deleteOnExit();
        FileUtils.write(secretKeyFile, secretKeyStr, Charsets.UTF_8);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY_FROM_FILE, secretKeyFile.toString());

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String token = AuthTokenUtils.createToken(secretKey, "my-test-subject", Optional.empty());

        // Pulsar protocol auth
        String subject = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(subject, "my-test-subject");
        provider.close();
    }

    @Test
    public void testAuthSecretKeyPair() throws Exception {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        String privateKeyStr = AuthTokenUtils.encodeKey(keyPair.getPrivate());
        String publicKeyStr = AuthTokenUtils.encodeKey(keyPair.getPublic());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        // Use public key for validation
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, publicKeyStr);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        // Use private key to generate token
        SecretKey privateKey = AuthTokenUtils.deserializeSecretKey(privateKeyStr);
        String token = AuthTokenUtils.createToken(privateKey, "my-test-subject", Optional.empty());

        // Pulsar protocol auth
        String subject = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(subject, "my-test-subject");

        provider.close();
    }
}
