/*
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.TokenAuthenticationProvider;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.common.util.RestException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MultiRolesTokenAuthorizationProviderTest {

    private static ServiceConfiguration tokenConfiguration(SecretKey key) {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        conf.getProperties().setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(key));
        return conf;
    }

    private static void initializeProvider(MultiRolesTokenAuthorizationProvider provider, ServiceConfiguration conf,
                                           PulsarResources resources) throws IOException {
        AuthenticationProviderToken authenticationProvider = new AuthenticationProviderToken();
        authenticationProvider.initialize(AuthenticationProvider.Context.builder().config(conf).build());
        provider.initialize(new AuthorizationProvider.InitialContext(conf, resources,
                authenticationService(authenticationProvider)));
    }

    private static AuthenticationService authenticationService(AuthenticationProvider provider) {
        AuthenticationService service = mock(AuthenticationService.class);
        when(service.getAuthenticationProvider("token")).thenReturn(provider);
        return service;
    }

    @DataProvider
    public Object[][] validationSettings() {
        return new Object[][]{
                {""}, {"custom_"}
        };
    }

    @Test(dataProvider = "validationSettings")
    public void testTokenValidation(String prefix) throws Exception {
        SecretKey key = Jwts.SIG.HS256.key().build();
        ServiceConfiguration conf = tokenConfiguration(key);
        conf.setSuperUserRoles(Set.of("admin"));
        conf.getProperties().setProperty("tokenSettingPrefix", prefix);
        conf.getProperties().setProperty(prefix + "tokenSecretKey", AuthTokenUtils.encodeKeyBase64(key));
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        initializeProvider(provider, conf, mock(PulsarResources.class));

        String valid = Jwts.builder().claim("roles", List.of("user", "admin")).signWith(key).compact();
        String otherKey = Jwts.builder().claim("roles", List.of("user", "admin"))
                .signWith(Jwts.SIG.HS256.key().build()).compact();
        String unsigned = Jwts.builder().claim("roles", List.of("user", "admin")).compact();
        String expired = Jwts.builder().claim("roles", List.of("user", "admin"))
                .expiration(new Date(0)).signWith(key).compact();

        assertThat(provider.isSuperUser("user", new AuthenticationDataCommand(valid), conf).get()).isTrue();
        for (String token : List.of(otherKey, unsigned)) {
            AuthenticationDataSource data = new AuthenticationDataCommand(token);
            assertThat(provider.isSuperUser("user", data, conf).get()).isFalse();
            assertThat(provider.authorize("user", data, role -> CompletableFuture.completedFuture(true)).get())
                    .isFalse();
        }
        for (String token : List.of(expired, "invalid.token.value", "not-a-token")) {
            assertThat(provider.isSuperUser("user", new AuthenticationDataCommand(token), conf).get()).isFalse();
            assertThat(provider.authorize("user", new AuthenticationDataCommand(token),
                    role -> CompletableFuture.completedFuture(true)).get()).isFalse();
        }
    }

    @Test
    public void testRequiresSharedAuthenticationProvider() throws Exception {
        ServiceConfiguration conf = tokenConfiguration(Jwts.SIG.HS256.key().build());
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        assertThatThrownBy(() -> provider.initialize(new AuthorizationProvider.InitialContext(
                conf, mock(PulsarResources.class), null)))
                .isInstanceOf(IOException.class).hasMessageContaining("initialized token authentication provider");
        assertThatThrownBy(() -> provider.initialize(conf, mock(PulsarResources.class)))
                .isInstanceOf(IOException.class);
    }

    @DataProvider
    public Object[][] unsupportedProviders() {
        return new Object[][]{
                {null}, {mock(AuthenticationProvider.class)}, {mock(AuthenticationProviderTls.class)}
        };
    }

    public static class CustomTokenProvider extends AuthenticationProviderToken {
    }

    @Test
    public void testCustomTokenProvider() throws Exception {
        SecretKey key = Jwts.SIG.HS256.key().build();
        ServiceConfiguration conf = tokenConfiguration(key);
        conf.setAuthenticationProviders(Set.of(CustomTokenProvider.class.getName()));
        @Cleanup
        AuthenticationService service = new AuthenticationService(conf);
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        provider.initialize(new AuthorizationProvider.InitialContext(conf, mock(PulsarResources.class), service));
        String token = Jwts.builder().claim("roles", List.of("user", "other")).signWith(key).compact();
        assertThat(provider.authorize("user", new AuthenticationDataCommand(token),
                role -> CompletableFuture.completedFuture("other".equals(role))).get()).isTrue();
    }

    @Test(dataProvider = "unsupportedProviders")
    public void testRejectUnsupportedProviders(AuthenticationProvider authenticationProvider) throws IOException {
        ServiceConfiguration conf = tokenConfiguration(Jwts.SIG.HS256.key().build());
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        assertThatThrownBy(() -> provider.initialize(new AuthorizationProvider.InitialContext(
                conf, null, authenticationService(authenticationProvider))))
                .isInstanceOf(IOException.class).hasMessageContaining("initialized token authentication provider");
    }

    @Test
    public void testRejectDisabledAuthentication() throws IOException {
        ServiceConfiguration conf = tokenConfiguration(Jwts.SIG.HS256.key().build());
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        conf.setAuthenticationEnabled(false);
        assertThatThrownBy(() -> provider.initialize(new AuthorizationProvider.InitialContext(
                conf, null, authenticationService(mock(TokenAuthenticationProvider.class)))))
                .isInstanceOf(IOException.class).hasMessageContaining("authenticationEnabled=true");
    }

    @DataProvider
    public Object[][] validationOutcomes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "validationOutcomes")
    public void testSharedProviderValidationIsAsync(boolean succeeds) throws Exception {
        ServiceConfiguration conf = tokenConfiguration(Jwts.SIG.HS256.key().build());
        conf.setSuperUserRoles(Set.of("admin"));
        TokenAuthenticationProvider authenticationProvider = mock(TokenAuthenticationProvider.class);
        CompletableFuture<Set<String>> validation = new CompletableFuture<>();
        when(authenticationProvider.authenticateRolesAsync(any(), eq("roles"))).thenReturn(validation);
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        provider.initialize(new AuthorizationProvider.InitialContext(conf, mock(PulsarResources.class),
                authenticationService(authenticationProvider)));
        String token = Jwts.builder().claim("roles", List.of("user", "admin")).compact();
        CompletableFuture<Boolean> authorized = provider.authorize("user", new AuthenticationDataCommand(token),
                role -> CompletableFuture.completedFuture(false));
        assertThat(authorized).isNotDone();
        if (succeeds) {
            validation.complete(Set.of("user", "admin"));
        } else {
            validation.completeExceptionally(new IllegalArgumentException("Token validation failed"));
        }
        assertThat(authorized.get()).isEqualTo(succeeds);
        verify(authenticationProvider).authenticateRolesAsync(any(), eq("roles"));
        provider.close();
        verify(authenticationProvider, never()).initialize(any(AuthenticationProvider.Context.class));
        verify(authenticationProvider, never()).close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthz() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String userB = "user-b";
        String token = Jwts.builder().claim("roles", new String[]{userA, userB}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithEmptyRoles() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String token = Jwts.builder().claim("roles", new String[]{}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithSingleRole() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testRole = "test-role";
        String token = Jwts.builder().claim("roles", testRole).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithoutClaim() throws Exception {
        final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        final String testRole = "test-role";
        // broker will use "roles" as the claim by default.
        final String token = Jwts.builder()
                .claim("whatever", testRole).signWith(secretKey).compact();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        final MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        initializeProvider(provider, conf, mock(PulsarResources.class));
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
        SecretKey secretKey = Jwts.SIG.HS256.key().build();
        @Cleanup
        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);

        initializeProvider(provider, conf, mock(PulsarResources.class));

        Function<String, CompletableFuture<Boolean>> authorizeFunc = (String role) -> {
            if (role.equals("test-role")) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        };
        assertTrue(provider.authorize("test-role", null, authorizeFunc).get());
        assertFalse(provider.authorize("test-role-x", null, authorizeFunc).get());
        assertTrue(provider.authorize("test-role",
                new AuthenticationDataSubscription(null, "test-sub"), authorizeFunc).get());
    }

    @Test
    public void testMultiRolesNotFailNonJWT() throws Exception {
        SecretKey secretKey = Jwts.SIG.HS256.key().build();
        String token = "a-non-jwt-token";

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithCustomRolesClaims() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testRole = "test-role";
        String customRolesClaims = "role";
        String token = Jwts.builder().claim(customRolesClaims, new String[]{testRole}).signWith(secretKey).compact();

        Properties properties = new Properties();
        properties.setProperty("tokenSettingPrefix", "prefix_");
        properties.setProperty("prefix_tokenAuthClaim", customRolesClaims);
        properties.setProperty("prefix_tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        conf.setProperties(properties);

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithSuperUser() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String testAdminRole = "admin";
        String token = Jwts.builder().claim("roles", testAdminRole).signWith(secretKey).compact();

        ServiceConfiguration conf = tokenConfiguration(secretKey);
        conf.setSuperUserRoles(Set.of(testAdminRole));

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        initializeProvider(provider, conf, mock(PulsarResources.class));

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
        assertTrue(provider.authorize(testAdminRole, ads,
                (String role) -> CompletableFuture.completedFuture(false)).get());
        assertTrue(provider.authorize("admin1", null, authorizeFunc).get());
        assertFalse(provider.authorize("admin2", null, authorizeFunc).get());
    }

    /**
     * Test subscription prefix mismatch exception handling.
     * <p>
     * Scenario 1: One role authorization succeeds, another role throws subscription prefix mismatch exception
     * -> Returns true (exception is swallowed)
     * Scenario 2: All roles throw subscription prefix mismatch exception -> Returns false
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMultiRolesAuthzWithSubscriptionPrefixMismatchException() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String userB = "user-b";
        String token = Jwts.builder()
                .claim(MultiRolesTokenAuthorizationProvider.DEFAULT_ROLE_CLAIM, new String[]{userA, userB})
                .signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

        // userA throws subscription prefix mismatch exception, userB returns true -> result should be true
        assertTrue(provider.authorize("test", ads, role -> {
            if (role.equals(userA)) {
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                future.completeExceptionally(new PulsarServerException(
                        "The subscription name needs to be prefixed by the authentication role"));
                return future;
            }
            return CompletableFuture.completedFuture(true);
        }).get());

        // All roles throw subscription prefix mismatch exception -> result should be false
        assertFalse(provider.authorize("test", ads, role -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.completeExceptionally(new PulsarServerException(
                    "The subscription name needs to be prefixed by the authentication role"));
            return future;
        }).get());
    }

    /**
     * Test single role with subscription prefix mismatch exception.
     * <p>
     * Single role throws subscription prefix mismatch exception -> Should throw the original exception
     * (Single role keeps original behavior, does not swallow exception)
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSingleRoleAuthzWithSubscriptionPrefixMismatchException() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String token = Jwts.builder()
                .claim(MultiRolesTokenAuthorizationProvider.DEFAULT_ROLE_CLAIM, userA)
                .signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        ServiceConfiguration conf = tokenConfiguration(secretKey);
        initializeProvider(provider, conf, mock(PulsarResources.class));

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

        // Single role throws subscription prefix mismatch exception -> should propagate exception
        ExecutionException ex = expectThrows(ExecutionException.class, () -> {
            provider.authorize("test", ads, role -> {
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                future.completeExceptionally(new PulsarServerException(
                        "The subscription name needs to be prefixed by the authentication role"));
                return future;
            }).get();
        });
        assertTrue(ex.getCause() instanceof PulsarServerException);
        assertTrue(ex.getCause().getMessage().contains(
                "The subscription name needs to be prefixed by the authentication role"));
    }

    /**
     * A client asking about a tenant that does not exist is a client-side 404, not a broker fault, so
     * it must not be reported at ERROR. Any client can trigger it at will simply by mistyping a tenant.
     */
    @Test
    public void testMissingTenantIsNotLoggedAtError() throws Exception {
        String tenant = "non-existent-tenant";
        TenantResources tenantResources = mock(TenantResources.class);
        when(tenantResources.getTenantAsync(tenant))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getTenantResources()).thenReturn(tenantResources);

        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String token = Jwts.builder().claim("roles", new String[]{"user-a"}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();
        initializeProvider(provider, tokenConfiguration(secretKey), pulsarResources);

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

        try (LogCapture logs = LogCapture.attach(MultiRolesTokenAuthorizationProvider.class)) {
            CompletableFuture<Boolean> future = provider.validateTenantAdminAccess(tenant, "user-a", ads);

            ExecutionException ee = expectThrows(ExecutionException.class, future::get);
            assertEquals(((RestException) ee.getCause()).getResponse().getStatus(),
                    Response.Status.NOT_FOUND.getStatusCode());
            assertEquals(logs.messagesAt(Level.ERROR), List.of(),
                    "A tenant that does not exist must not be logged at ERROR");
        }
    }
}
