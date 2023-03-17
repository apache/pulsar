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
package org.apache.pulsar.broker.authentication.oidc;

import static org.testng.Assert.assertNull;
import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;
import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests to cover the AuthenticationProviderOpenID without any network calls.
 * <p>
 * This class only tests the verification of tokens. It does not test the integration to retrieve tokens
 * from an identity provider. See {@link AuthenticationProviderOpenIDIntegrationTest} for the authorization
 * server integration tests.
 * <p>
 * Note: this class uses the io.jsonwebtoken library here because it has more utilities than the auth0 library.
 * The jsonwebtoken library makes it easy to generate key pairs for many algorithms, and it also has an enum
 * that can be used to assert that unsupported algorithms properly fail validation.
 */
public class AuthenticationProviderOpenIDTest {

    // The set of algorithms we expect the AuthenticationProviderOpenID to support
    final Set<SignatureAlgorithm> supportedAlgorithms = Set.of(
            SignatureAlgorithm.RS256, SignatureAlgorithm.RS384, SignatureAlgorithm.RS512,
            SignatureAlgorithm.ES256, SignatureAlgorithm.ES384, SignatureAlgorithm.ES512);

    @Test
    public void testNullToken() {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Assert.assertThrows(AuthenticationException.class,
                () -> provider.authenticate(new AuthenticationDataCommand(null)));
    }

    @Test
    public void testThatNullAlgFails() {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Assert.assertThrows(AuthenticationException.class,
                () -> provider.verifyJWT(null, null, null));
    }

    @Test
    public void testThatUnsupportedAlgsThrowExceptions() {
        Set<SignatureAlgorithm> unsupportedAlgs = new HashSet<>(Set.of(SignatureAlgorithm.values()));
        unsupportedAlgs.removeAll(supportedAlgorithms);
        unsupportedAlgs.forEach(unsupportedAlg -> {
            AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
            // We don't create a public key because it's irrelevant
            Assert.assertThrows(AuthenticationException.class,
                    () -> provider.verifyJWT(null, unsupportedAlg.getValue(), null));
        });
    }

    @Test
    public void testThatSupportedAlgsWork() {
        supportedAlgorithms.forEach(supportedAlg -> {
            KeyPair keyPair = Keys.keyPairFor(supportedAlg);
            AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
            DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
            defaultJwtBuilder.setAudience("an-audience");
            defaultJwtBuilder.signWith(keyPair.getPrivate());

            // Convert to the right class
            DecodedJWT expectedValue = JWT.decode(defaultJwtBuilder.compact());
            DecodedJWT actualValue = null;
            try {
                actualValue = provider.verifyJWT(keyPair.getPublic(), supportedAlg.getValue(), expectedValue);
            } catch (Exception e) {
                Assert.fail("Token verification should not have thrown an exception.", e);
            }
            Assert.assertEquals(expectedValue, actualValue);
        });
    }

    @Test
    public void testThatSupportedAlgWithMismatchedPublicKeyFromDifferentAlgFamilyFails() {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setAudience("an-audience");
        defaultJwtBuilder.signWith(keyPair.getPrivate());
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());
        // Choose a different algorithm from a different alg family
        Assert.assertThrows(AuthenticationException.class,
                () -> provider.verifyJWT(keyPair.getPublic(), SignatureAlgorithm.ES512.getValue(), jwt));
    }

    @Test
    public void testThatSupportedAlgWithMismatchedPublicKeyFromSameAlgFamilyFails() {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setAudience("an-audience");
        defaultJwtBuilder.signWith(keyPair.getPrivate());
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());
        // Choose a different algorithm but within the same alg family as above
        Assert.assertThrows(AuthenticationException.class,
                () -> provider.verifyJWT(keyPair.getPublic(), SignatureAlgorithm.RS512.getValue(), jwt));
    }

    @Test
    public void ensureExpiredTokenFails() {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setExpiration(Date.from(Instant.EPOCH));
        defaultJwtBuilder.signWith(keyPair.getPrivate());
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());
        Assert.assertThrows(AuthenticationException.class,
                () -> provider.verifyJWT(keyPair.getPublic(), SignatureAlgorithm.RS256.getValue(), jwt));
    }

    @Test
    public void ensureRecentlyExpiredTokenWithinConfiguredLeewaySucceeds() throws Exception {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        // Set up the provider
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ACCEPTED_TIME_LEEWAY_SECONDS, "10");
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://localhost:8080");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build the JWT with an only recently expired token
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setExpiration(Date.from(Instant.ofEpochMilli(System.currentTimeMillis() - 5000L)));
        defaultJwtBuilder.signWith(keyPair.getPrivate());
        DecodedJWT expectedValue = JWT.decode(defaultJwtBuilder.compact());

        // Test the verification
        DecodedJWT actualValue = null;
        try {
            actualValue = provider.verifyJWT(keyPair.getPublic(), SignatureAlgorithm.RS256.getValue(), expectedValue);
        } catch (Exception e) {
            Assert.fail("Token verification should not have thrown an exception.", e);
        }
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void ensureEmptyIssuersFailsInitialization() {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        Assert.assertThrows(IllegalArgumentException.class, () -> provider.initialize(config));
    }

    @Test
    public void ensureNullIssuersFailsInitialization() {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        ServiceConfiguration config = new ServiceConfiguration();
        // Make sure this still defaults to null.
        assertNull(config.getProperties().get(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS));
        Assert.assertThrows(IllegalArgumentException.class, () -> provider.initialize(config));
    }

    @Test
    public void ensureInsecureIssuerFailsInitialization() {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com,http://myissuer.com");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        Assert.assertThrows(IllegalArgumentException.class, () -> provider.initialize(config));
    }

    @Test void ensureMissingRoleClaimReturnsNull() throws Exception {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com");
        props.setProperty(AuthenticationProviderOpenID.ROLE_CLAIM, "sub");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build an empty JWT
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setAudience("audience");
        DecodedJWT jwtWithoutSub = JWT.decode(defaultJwtBuilder.compact());

        // A JWT with an empty role claim must result in a null role
        assertNull(provider.getRole(jwtWithoutSub));
    }

    @Test void ensureRoleClaimForStringReturnsRole() throws Exception {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com");
        props.setProperty(AuthenticationProviderOpenID.ROLE_CLAIM, "sub");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build an empty JWT
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        defaultJwtBuilder.setSubject("my-role");
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());

        Assert.assertEquals("my-role", provider.getRole(jwt));
    }

    @Test void ensureRoleClaimForSingletonListReturnsRole() throws Exception {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com");
        props.setProperty(AuthenticationProviderOpenID.ROLE_CLAIM, "roles");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build an empty JWT
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        HashMap<String, List<String>> claims = new HashMap();
        claims.put("roles", Collections.singletonList("my-role"));
        defaultJwtBuilder.setClaims(claims);
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());

        Assert.assertEquals("my-role", provider.getRole(jwt));
    }

    @Test void ensureRoleClaimForMultiEntryListReturnsFirstRole() throws Exception {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com");
        props.setProperty(AuthenticationProviderOpenID.ROLE_CLAIM, "roles");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build an empty JWT
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        HashMap<String, List<String>> claims = new HashMap();
        claims.put("roles", Arrays.asList("my-role-1", "my-role-2"));
        defaultJwtBuilder.setClaims(claims);
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());

        Assert.assertEquals("my-role-1", provider.getRole(jwt));
    }

    @Test void ensureRoleClaimForEmptyListReturnsNull() throws Exception {
        AuthenticationProviderOpenID provider = new AuthenticationProviderOpenID();
        Properties props = new Properties();
        props.setProperty(AuthenticationProviderOpenID.ALLOWED_TOKEN_ISSUERS, "https://myissuer.com");
        props.setProperty(AuthenticationProviderOpenID.ROLE_CLAIM, "roles");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        provider.initialize(config);

        // Build an empty JWT
        DefaultJwtBuilder defaultJwtBuilder = new DefaultJwtBuilder();
        HashMap<String, List<String>> claims = new HashMap<>();
        claims.put("roles", Collections.emptyList());
        defaultJwtBuilder.setClaims(claims);
        DecodedJWT jwt = JWT.decode(defaultJwtBuilder.compact());

        // A JWT with an empty list role claim must result in a null role
        assertNull(provider.getRole(jwt));
    }
}
