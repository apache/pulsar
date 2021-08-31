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
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.net.URL;

import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.api.AuthData;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import com.auth0.jwt.*;
import com.auth0.jwt.interfaces.DecodedJWT;

public class AuthenticationProviderTokenJWK implements AuthenticationProvider {

    static final String HTTP_HEADER_NAME = "Authorization";
    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // When symmetric key is configured
    static final String CONF_TOKEN_SETTING_PREFIX = "";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";


    static final String CONF_JWK_URL = "jwkUrl";

    // When using public key's, the algorithm of the key
    static final String CONF_TOKEN_PUBLIC_ALG = "tokenPublicKeytokenPublicKey";

    // The token audience "claim" name, e.g. "aud", that will be used to get the audience from token.
    static final String CONF_TOKEN_AUDIENCE_CLAIM = "tokenAudienceClaim";

    // The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token, need contains this.
    static final String CONF_TOKEN_AUDIENCE = "tokenAudience";

    static final String TOKEN = "token";

    private static final Counter expiredTokenMetrics = Counter.build()
            .name("pulsar_expired_token_count")
            .help("Pulsar expired token")
            .register();

    private static final Histogram expiringTokenMinutesMetrics = Histogram.build()
            .name("pulsar_expiring_token_minutes")
            .help("The remaining time of expiring token in minutes")
            .buckets(5, 10, 60, 240)
            .register();

    private String roleClaim;
    private String audienceClaim;
    private String audience;
    private JwkProvider provider;
    private String jwkUrl;

    // config keys

    private String confTokenAuthClaimSettingName;
    private String confTokenAudienceClaimSettingName;
    private String confTokenAudienceSettingName;
    private String confJWkUrlSettingName;


    @Override
    public void close() throws IOException {
        // noop
    }

    @VisibleForTesting
    public static void resetMetrics() {
        expiredTokenMetrics.clear();
        expiringTokenMinutesMetrics.clear();
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException, IllegalArgumentException {
        String prefix = (String) config.getProperty(CONF_TOKEN_SETTING_PREFIX);
        if (null == prefix) {
            prefix = "";
        }
        this.confTokenAuthClaimSettingName = prefix + CONF_TOKEN_AUTH_CLAIM;
        this.confTokenAudienceClaimSettingName = prefix + CONF_TOKEN_AUDIENCE_CLAIM;
        this.confTokenAudienceSettingName = prefix + CONF_TOKEN_AUDIENCE;
        this.confJWkUrlSettingName = prefix + CONF_JWK_URL;


        // we need to fetch the algorithm before we fetch the key
        this.roleClaim = getTokenRoleClaim(config);
        this.audienceClaim = getTokenAudienceClaim(config);
        this.audience = getTokenAudience(config);
        this.jwkUrl = getJwkUrl(config);

        try {
            this.provider = new UrlJwkProvider(new URL(this.jwkUrl));
        } catch (MalformedURLException e){
            throw new MalformedURLException("Url is malformed");
        }




        if (audienceClaim != null && audience == null ) {
            throw new IllegalArgumentException("Token Audience Claim [" + audienceClaim
                    + "] configured, but Audience stands for this broker not.");
        }
    }

    @Override
    public String getAuthMethodName() {
        return TOKEN;
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        try {
            // Get Token
            String token;
            String role;
            token = getToken(authData);
            // Parse Token by validating


            role = getPrincipal(authenticateToken(token));


            AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
            return role;
        } catch (AuthenticationException exception) {
            AuthenticationMetrics.authenticateFailure(getClass().getSimpleName(), getAuthMethodName(), exception.getMessage());
            throw exception;
        }
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
    private DecodedJWT authenticateToken(final String token) throws AuthenticationException {

        DecodedJWT jwt = JWT.decode(token);

        Jwk jwk = null;
        Algorithm algorithm = null;
        try {
            jwk = provider.get(jwt.getKeyId());

            if(!(jwk instanceof RSAPublicKey)){
                throw new JwtException("key needs to be a RSA Publickey");
            }

            algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
            algorithm.verify(jwt); // if the token signature is invalid, the method will throw SignatureVerificationException

            if (audienceClaim != null) {
                Object object = jwt.getClaim(audienceClaim);

                if (object == null) {
                    throw new JwtException("Found null Audience in token, for claimed field: " + audienceClaim);
                }

                if (object instanceof List) {
                    List<String> audiences = (List<String>) object;
                    // audience not contains this broker, throw exception.
                    if (audiences.stream().noneMatch(audienceInToken -> audienceInToken.equals(audience))) {
                        throw new AuthenticationException("Audiences in token: [" + String.join(", ", audiences)
                                + "] not contains this broker: " + audience);
                    }
                } else if (object instanceof String) {
                    if (!object.equals(audience)) {
                        throw new AuthenticationException("Audiences in token: [" + object
                                + "] not contains this broker: " + audience);
                    }
                } else {
                    // should not reach here.
                    throw new AuthenticationException("Audiences in token is not in expected format: " + object);
                }
            }

            if(jwt.getExpiresAt() != null){
                long minToExpiration = (jwt.getExpiresAt().getTime() - new Date().getTime()) / (60 * 1000);
                if(minToExpiration <= 0){
                    throw new AuthenticationException("Token is expired");
                }
                expiringTokenMinutesMetrics.observe((double) minToExpiration / (60 * 1000));
            }

            return jwt;
        } catch (JwkException | SignatureVerificationException e) {
            throw new AuthenticationException("Failed to authentication token: " + e.getMessage());
        }

    }

    private String getPrincipal(DecodedJWT jwt) {

        return jwt.getClaim(roleClaim).asString();
    }

    private String getTokenRoleClaim(ServiceConfiguration conf){
        if (conf.getProperty(confTokenAuthClaimSettingName) != null
                && StringUtils.isNotBlank((String) conf.getProperty(confTokenAuthClaimSettingName))) {
            return (String) conf.getProperty(confTokenAuthClaimSettingName);
        } else {
            return Claims.SUBJECT;
        }
    }

    private String getJwkUrl(ServiceConfiguration conf){
        if (conf.getProperty(confJWkUrlSettingName) != null
                && StringUtils.isNotBlank((String) conf.getProperty(confJWkUrlSettingName))) {
            return (String) conf.getProperty(confJWkUrlSettingName);
        } else {
            return "";
        }
    }

    // get Token Audience Claim from configuration, if not configured return null.
    private String getTokenAudienceClaim(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(confTokenAudienceClaimSettingName) != null
                && StringUtils.isNotBlank((String) conf.getProperty(confTokenAudienceClaimSettingName))) {
            return (String) conf.getProperty(confTokenAudienceClaimSettingName);
        } else {
            return null;
        }
    }

    // get Token Audience that stands for this broker from configuration, if not configured return null.
    private String getTokenAudience(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(confTokenAudienceSettingName) != null
                && StringUtils.isNotBlank((String) conf.getProperty(confTokenAudienceSettingName))) {
            return (String) conf.getProperty(confTokenAudienceSettingName);
        } else {
            return null;
        }
    }

    private static final class TokenAuthenticationState implements AuthenticationState {
        private final AuthenticationProviderTokenJWK provider;
        private AuthenticationDataSource authenticationDataSource;
        private DecodedJWT jwt;
        private final SocketAddress remoteAddress;
        private final SSLSession sslSession;
        private long expiration;

        TokenAuthenticationState(
                AuthenticationProviderTokenJWK provider,
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
            if (jwt.getExpiresAt() != null) {
                this.expiration = jwt.getExpiresAt().getTime();
            } else {
                // Disable expiration
                this.expiration = Long.MAX_VALUE;
            }
            this.authenticationDataSource = new AuthenticationDataCommand(token, remoteAddress, sslSession);

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
