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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.util.HashSet;
import java.util.Properties;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.testng.annotations.Test;
import javax.crypto.SecretKey;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class MultiRolesTokenAuthorizationProviderTest {

    @Test
    public void testMultiRolesAuthz() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String userB = "user-b";
        String token = Jwts.builder().claim("sub", new String[]{userA, userB}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = new ServiceConfiguration();
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

        assertTrue(provider.authorize("test", ads, role -> {
            if (role.equals(userB)) {
                return CompletableFuture.completedFuture(true); // only userB has permission
            }
            return CompletableFuture.completedFuture(false);
        }).get());

        assertTrue(provider.authorize("test", ads, role -> {
            return CompletableFuture.completedFuture(true); // all users has permission
        }).get());

        assertFalse(provider.authorize("test", ads, role -> {
            return CompletableFuture.completedFuture(false); // all users has no permission
        }).get());
    }

    @Test
    public void testMultiRolesAuthzWithEmptyRoles() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String token = Jwts.builder().claim("sub", new String[]{}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = new ServiceConfiguration();
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

        assertFalse(provider.authorize("test", ads, role -> CompletableFuture.completedFuture(false)).get());
    }

    @Test
    public void testMultiRolesAuthzWithSingleRole() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testRole = "test-role";
        String token = Jwts.builder().claim("sub", testRole).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = new ServiceConfiguration();
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

        assertTrue(provider.authorize("test", ads, role -> {
            if (role.equals(testRole)) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        }).get());
    }

    @Test
    public void testMultiRolesAuthzWithoutClaim() throws Exception {
        final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        final String testRole = "test-role";
        // broker will use "sub" as the claim by default.
        final String token = Jwts.builder()
                .claim("whatever", testRole).signWith(secretKey).compact();
        ServiceConfiguration conf = new ServiceConfiguration();
        final MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        provider.initialize(conf, mock(PulsarResources.class));
        final AuthenticationDataSource ads = new AuthenticationDataSource() {
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

        assertFalse(provider.authorize("test", ads, role -> {
            if (role == null) {
                throw new IllegalStateException("We should avoid pass null to sub providers");
            }
            return CompletableFuture.completedFuture(role.equals(testRole));
        }).get());
    }

    @Test
    public void testMultiRolesAuthzWithAnonymousUser() throws Exception {
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = new ServiceConfiguration();

        provider.initialize(conf, mock(PulsarResources.class));

        Function<String, CompletableFuture<Boolean>> authorizeFunc = (String role) -> {
            if (role.equals("test-role")) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        };
        assertTrue(provider.authorize("test-role", null, authorizeFunc).get());
        assertFalse(provider.authorize("test-role-x", null, authorizeFunc).get());
        assertTrue(provider.authorize("test-role", new AuthenticationDataSubscription(null, "test-sub"), authorizeFunc).get());
    }

    @Test
    public void testMultiRolesNotFailNonJWT() throws Exception {
        String token = "a-non-jwt-token";

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = new ServiceConfiguration();
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

        assertFalse(provider.authorize("test", ads, role -> CompletableFuture.completedFuture(false)).get());
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
        assertTrue(provider.authorize("test", ads, role -> {
            if (role.equals(testRole)) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        }).get());
    }

    @Test
    public void testMultiRolesAuthzWithSuperUser() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testAdminRole = "admin";
        String token = Jwts.builder().claim("sub", testAdminRole).signWith(secretKey).compact();

        ServiceConfiguration conf = new ServiceConfiguration();
        Set rols = new HashSet();
        rols.add(testAdminRole);
        conf.setSuperUserRoles(rols);

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

        assertTrue(provider.isSuperUser(testAdminRole, ads, conf).get());
        Function<String, CompletableFuture<Boolean>> authorizeFunc = (String role) -> {
            if (role.equals("admin1")) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        };
        assertTrue(provider.authorize(testAdminRole, ads, (String role) -> CompletableFuture.completedFuture(false)).get());
        assertTrue(provider.authorize("admin1", null, authorizeFunc).get());
        assertFalse(provider.authorize("admin2", null, authorizeFunc).get());
    }
}
