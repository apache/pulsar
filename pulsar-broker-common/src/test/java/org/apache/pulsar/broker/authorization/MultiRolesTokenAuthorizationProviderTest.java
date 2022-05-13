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
package org.apache.pulsar.broker.authorization;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Properties;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.util.concurrent.CompletableFuture;

public class MultiRolesTokenAuthorizationProviderTest {

    @Test
    public void testMultiRolesAuthz() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String userB = "user-b";
        String token = Jwts.builder().claim("sub", new String[]{userA, userB}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        assertTrue(provider.authorize(ads, role -> {
            if (role.equals(userB)) {
                return CompletableFuture.completedFuture(true); // only userB has permission
            }
            return CompletableFuture.completedFuture(false);
        }).get());

        assertTrue(provider.authorize(ads, role -> {
            return CompletableFuture.completedFuture(true); // all users has permission
        }).get());

        assertFalse(provider.authorize(ads, role -> {
            return CompletableFuture.completedFuture(false); // all users has no permission
        }).get());
    }

    @Test
    public void testMultiRolesAuthzWithEmptyRoles() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String token = Jwts.builder().claim("sub", new String[]{}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        assertFalse(provider.authorize(ads, role -> CompletableFuture.completedFuture(false)).get());
    }

    @Test
    public void testMultiRolesAuthzWithSingleRole() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testRole = "test-role";
        String token = Jwts.builder().claim("sub", testRole).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        assertTrue(provider.authorize(ads, role -> {
            if (role.equals(testRole)) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        }).get());
    }

    @Test
    public void testMultiRolesNotFailNonJWT() throws Exception {
        String token = "a-non-jwt-token";

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        assertFalse(provider.authorize(ads, role -> CompletableFuture.completedFuture(false)).get());
    }

    @Test
    public void testMultiRolesAuthzWithCustomRolesClaims() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testRole = "test-role";
        String customRolesClaims = "role";
        String token = Jwts.builder().claim(customRolesClaims, new String[]{testRole}).signWith(secretKey).compact();

        Properties properties = new Properties();
        properties.setProperty("tokenSettingPrefix", "prefix_");
        properties.setProperty("prefix_tokenAuthClaim", customRolesClaims);
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        provider.initialize(conf, mock(PulsarResources.class));

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        assertTrue(provider.authorize(ads, role -> {
            if (role.equals(testRole)) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        }).get());
    }
}
