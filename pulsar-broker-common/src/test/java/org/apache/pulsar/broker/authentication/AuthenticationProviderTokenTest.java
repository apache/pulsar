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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import java.security.Key;
import java.util.Arrays;
import java.util.List;
import lombok.Cleanup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.sql.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

public class AuthenticationProviderTokenTest {

    private static final String SUBJECT = "my-test-subject";

    @Test
    public void testInvalidInitialize() throws Exception {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        try {
            provider.initialize(new ServiceConfiguration());
            fail("should have failed");
        } catch (IOException e) {
            // Expected, secret key was not defined
        } finally {
            // currently, will not close any resource
            provider.close();
        }
    }

    @Test
    public void testSerializeSecretKey() {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        String token = Jwts.builder()
                .setSubject(SUBJECT)
                .signWith(secretKey)
                .compact();

        @SuppressWarnings("unchecked")
        Jwt<?, Claims> jwt = Jwts.parserBuilder()
                .setSigningKey(AuthTokenUtils.decodeSecretKey(secretKey.getEncoded()))
                .build()
                .parse(token);

        assertNotNull(jwt);
        assertNotNull(jwt.getBody());
        assertEquals(jwt.getBody().getSubject(), SUBJECT);
    }

    @Test
    public void testSerializeKeyPair() throws Exception {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        String privateKey = AuthTokenUtils.encodeKeyBase64(keyPair.getPrivate());
        String publicKey = AuthTokenUtils.encodeKeyBase64(keyPair.getPublic());

        String token = AuthTokenUtils.createToken(AuthTokenUtils.decodePrivateKey(Decoders.BASE64.decode(privateKey), SignatureAlgorithm.RS256),
                SUBJECT,
                Optional.empty());

        @SuppressWarnings("unchecked")
        Jwt<?, Claims> jwt = Jwts.parserBuilder()
                .setSigningKey(AuthTokenUtils.decodePublicKey(Decoders.BASE64.decode(publicKey), SignatureAlgorithm.RS256))
                .build()
                .parse(token);

        assertNotNull(jwt);
        assertNotNull(jwt.getBody());
        assertEquals(jwt.getBody().getSubject(), SUBJECT);
    }

    @Test
    public void testAuthSecretKey() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        assertEquals(provider.getAuthMethodName(), AuthenticationProviderToken.TOKEN);

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                AuthTokenUtils.encodeKeyBase64(secretKey));

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

        String token = AuthTokenUtils.createToken(secretKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);

        // HTTP protocol auth
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals(AuthenticationProviderToken.HTTP_HEADER_NAME)) {
                    return AuthenticationProviderToken.HTTP_HEADER_VALUE_PREFIX + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        });
        assertEquals(subject, SUBJECT);

        /**
         * HEADER:ALGORITHM & TOKEN TYPE
         * {
         *   "alg": "none"
         * }
         * PAYLOAD:DATA
         * {
         *   "sub": "test-user"
         * }
         */
        String tokenWithNoneAlg = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.";
        try {
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return tokenWithNoneAlg;
                }
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, Unsigned Claims JWTs are not supported.
        }

        // Expired token. This should be rejected by the authentication provider
        String expiredToken = AuthTokenUtils.createToken(secretKey, SUBJECT,
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
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        File secretKeyFile = File.createTempFile("pulsar-test-secret-key-", ".key");
        secretKeyFile.deleteOnExit();
        Files.write(Paths.get(secretKeyFile.toString()), secretKey.getEncoded());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, "file://" + secretKeyFile.toString());

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String token = AuthTokenUtils.createToken(secretKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);
        provider.close();
    }

    @Test
    public void testAuthSecretKeyFromValidFile() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        File secretKeyFile = File.createTempFile("pulsar-test-secret-key-valid", ".key");
        secretKeyFile.deleteOnExit();
        Files.write(Paths.get(secretKeyFile.toString()), secretKey.getEncoded());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, secretKeyFile.toString());

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String token = AuthTokenUtils.createToken(secretKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);
        provider.close();
    }

    @Test
    public void testAuthSecretKeyFromDataBase64() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                "data:;base64," + AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String token = AuthTokenUtils.createToken(secretKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);
        provider.close();
    }

    @Test
    public void testAuthSecretKeyPair() throws Exception {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        String privateKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPrivate());
        String publicKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPublic());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        // Use public key for validation
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY, publicKeyStr);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        // Use private key to generate token
        PrivateKey privateKey = AuthTokenUtils.decodePrivateKey(Decoders.BASE64.decode(privateKeyStr), SignatureAlgorithm.RS256);
        String token = AuthTokenUtils.createToken(privateKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);

        provider.close();
    }

    @Test
    public void testAuthSecretKeyPairWithCustomClaim() throws Exception {
        String authRoleClaim = "customClaim";
        String authRole = "my-test-role";

        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        String privateKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPrivate());
        String publicKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPublic());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        // Use public key for validation
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY, publicKeyStr);
        // Set custom claim field
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUTH_CLAIM, authRoleClaim);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);


        // Use private key to generate token
        PrivateKey privateKey = AuthTokenUtils.decodePrivateKey(Decoders.BASE64.decode(privateKeyStr), SignatureAlgorithm.RS256);
        String token = Jwts.builder()
                .setClaims(new HashMap<String, Object>() {{
                    put(authRoleClaim, authRole);
                }})
                .signWith(privateKey)
                .compact();


        // Pulsar protocol auth
        String role = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(role, authRole);

        provider.close();
    }

    @Test
    public void testAuthSecretKeyPairWithECDSA() throws Exception {
        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.ES256);

        String privateKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPrivate());
        String publicKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPublic());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        // Use public key for validation
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY, publicKeyStr);
        // Set that we are using EC keys
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_ALG, SignatureAlgorithm.ES256.getValue());

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        // Use private key to generate token
        PrivateKey privateKey = AuthTokenUtils.decodePrivateKey(Decoders.BASE64.decode(privateKeyStr), SignatureAlgorithm.ES256);
        String token = AuthTokenUtils.createToken(privateKey, SUBJECT, Optional.empty());

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
        assertEquals(subject, SUBJECT);

        provider.close();
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testAuthenticateWhenNoJwtPassed() throws AuthenticationException {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return false;
            }

            @Override
            public boolean hasDataFromHttp() {
                return false;
            }
        });
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testAuthenticateWhenAuthorizationHeaderNotExist() throws AuthenticationException {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public String getHttpHeader(String name) {
                return null;
            }

            @Override
            public boolean hasDataFromHttp() {
                return true;
            }
        });
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testAuthenticateWhenAuthHeaderValuePrefixIsInvalid() throws AuthenticationException {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public String getHttpHeader(String name) {
                return "MyBearer ";
            }

            @Override
            public boolean hasDataFromHttp() {
                return true;
            }
        });
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testAuthenticateWhenJwtIsBlank() throws AuthenticationException {
        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public String getHttpHeader(String name) {
                return AuthenticationProviderToken.HTTP_HEADER_VALUE_PREFIX + "      ";
            }

            @Override
            public boolean hasDataFromHttp() {
                return true;
            }
        });
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testAuthenticateWhenInvalidTokenIsPassed() throws AuthenticationException, IOException {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.initialize(conf);
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public String getHttpHeader(String name) {
                return AuthenticationProviderToken.HTTP_HEADER_VALUE_PREFIX + "invalid_token";
            }

            @Override
            public boolean hasDataFromHttp() {
                return true;
            }
        });
    }

    @Test(expectedExceptions = IOException.class)
    public void testValidationKeyWhenBlankSecretKeyIsPassed() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, "   ");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.initialize(conf);
    }

    @Test(expectedExceptions = IOException.class)
    public void testValidationKeyWhenBlankPublicKeyIsPassed() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY, "   ");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        provider.initialize(conf);
    }

    @Test(expectedExceptions = IOException.class)
    public void testInitializeWhenSecretKeyFilePathIsInvalid() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                "file://" + "invalid_secret_key_file");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        new AuthenticationProviderToken().initialize(conf);
    }

    @Test(expectedExceptions = IOException.class)
    public void testInitializeWhenSecretKeyIsValidPathOrBase64() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                "secret_key_file_not_exist");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        new AuthenticationProviderToken().initialize(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInitializeWhenSecretKeyFilePathIfNotExist() throws IOException {
        File secretKeyFile = File.createTempFile("secret_key_file_not_exist", ".key");
        assertTrue(secretKeyFile.delete());
        assertFalse(secretKeyFile.exists());

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, secretKeyFile.toString());

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        new AuthenticationProviderToken().initialize(conf);
    }

    @Test(expectedExceptions = IOException.class)
    public void testInitializeWhenPublicKeyFilePathIsInvalid() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY,
                "file://" + "invalid_public_key_file");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        new AuthenticationProviderToken().initialize(conf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidationWhenPublicKeyAlgIsInvalid() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_ALG,
                "invalid");

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);

        new AuthenticationProviderToken().initialize(conf);
    }


    @Test
    public void testExpiringToken() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        @Cleanup
        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY,
                AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        // Create a token that will expire in 3 seconds
        String expiringToken = AuthTokenUtils.createToken(secretKey, SUBJECT,
                Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3))));

        AuthenticationState authState = provider.newAuthState(AuthData.of(expiringToken.getBytes()), null, null);
        assertTrue(authState.isComplete());
        assertFalse(authState.isExpired());

        Thread.sleep(TimeUnit.SECONDS.toMillis(6));
        assertTrue(authState.isExpired());
        assertTrue(authState.isComplete());

        AuthData brokerData = authState.refreshAuthentication();
        assertEquals(brokerData, AuthData.REFRESH_AUTH_DATA);
    }

    // tests for Token Audience
    @Test
    public void testRightTokenAudienceClaim() throws Exception {
        String brokerAudience = "testBroker_" + System.currentTimeMillis();
        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, "aud");
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);

        testTokenAudienceWithDifferentConfig(properties, brokerAudience);
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testWrongTokenAudience() throws Exception {
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, "aud");
        // set wrong audience in token, should throw exception.
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience + "-wrong");
        testTokenAudienceWithDifferentConfig(properties, brokerAudience);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNoBrokerTokenAudience() throws Exception {
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        Properties properties = new Properties();
        // Not set broker audience, should throw exception.
        //properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, "aud");
        testTokenAudienceWithDifferentConfig(properties, brokerAudience);
    }

    @Test
    public void testSelfDefineTokenAudienceClaim() throws Exception {
        String audienceClaim = "audience_claim_" + System.currentTimeMillis();
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, audienceClaim);
        testTokenAudienceWithDifferentConfig(properties, audienceClaim, Lists.newArrayList(brokerAudience));
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testWrongSelfDefineTokenAudienceClaim() throws Exception {
        String audienceClaim = "audience_claim_" + System.currentTimeMillis();
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        Properties properties = new Properties();
        // Set wrong broker audience, should throw exception.
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, audienceClaim);
        testTokenAudienceWithDifferentConfig(properties,
                audienceClaim + "_wrong",
                Lists.newArrayList(brokerAudience));
    }

    @Test
    public void testMultiTokenAudience() throws Exception {
        String audienceClaim = "audience_claim_" + System.currentTimeMillis();
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        List<String> audiences = Lists.newArrayList("AnotherBrokerAudience", brokerAudience);

        Properties properties = new Properties();
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, audienceClaim);
        testTokenAudienceWithDifferentConfig(properties, audienceClaim, audiences);
    }

    @Test(expectedExceptions = AuthenticationException.class)
    public void testMultiTokenAudienceNotInclude() throws Exception {
        String audienceClaim = "audience_claim_" + System.currentTimeMillis();
        String brokerAudience = "testBroker_" + System.currentTimeMillis();

        List<String> audiences = Lists.newArrayList("AnotherBrokerAudience", brokerAudience + "_wrong");

        Properties properties = new Properties();
        // Broker audience not included in token's audiences, should throw exception.
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE, brokerAudience);
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUDIENCE_CLAIM, audienceClaim);
        testTokenAudienceWithDifferentConfig(properties, audienceClaim, audiences);
    }

    @Test
    public void testArrayTypeRoleClaim() throws Exception {
        String authRoleClaim = "customClaim";
        String authRole = "my-test-role";

        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);

        String privateKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPrivate());
        String publicKeyStr = AuthTokenUtils.encodeKeyBase64(keyPair.getPublic());

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        // Use public key for validation
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY, publicKeyStr);
        // Set custom claim field
        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_AUTH_CLAIM, authRoleClaim);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        // Use private key to generate token
        PrivateKey privateKey = AuthTokenUtils.decodePrivateKey(Decoders.BASE64.decode(privateKeyStr), SignatureAlgorithm.RS256);
        String token = Jwts.builder()
                .setClaims(new HashMap<String, Object>() {{
                    put(authRoleClaim, Arrays.asList(authRole, "other-role"));
                }})
                .signWith(privateKey)
                .compact();

        // Pulsar protocol auth
        String role = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(role, authRole);

        provider.close();
    }

    private static String createTokenWithAudience(Key signingKey, String audienceClaim, List<String> audience) {
        JwtBuilder builder = Jwts.builder()
                .setSubject(SUBJECT)
                .signWith(signingKey);

        builder.claim(audienceClaim, audience);
        return builder.compact();
    }

    private static void testTokenAudienceWithDifferentConfig(Properties properties,
                                                             String brokerAudience) throws Exception {
        testTokenAudienceWithDifferentConfig(properties,
                "aud",
                Lists.newArrayList(brokerAudience));
    }

    private static void testTokenAudienceWithDifferentConfig(Properties properties,
                                                             String audienceClaim, List<String> audiences) throws Exception {
        @Cleanup
        AuthenticationProviderToken provider = new AuthenticationProviderToken();
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        File secretKeyFile = File.createTempFile("pulsar-test-secret-key-valid", ".key");
        secretKeyFile.deleteOnExit();
        Files.write(Paths.get(secretKeyFile.toString()), secretKey.getEncoded());

        properties.setProperty(AuthenticationProviderToken.CONF_TOKEN_SECRET_KEY, secretKeyFile.toString());
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String token = createTokenWithAudience(secretKey, audienceClaim, audiences);

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
        assertEquals(subject, SUBJECT);
        provider.close();
    }
}
