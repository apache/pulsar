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


import java.security.Key;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.jsonwebtoken.*;
import io.jsonwebtoken.security.InvalidKeyException;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.api.AuthData;
import com.auth0.jwt.interfaces.DecodedJWT;


public class AuthenticationProviderTokenOIDC implements AuthenticationProvider {

    static final String HTTP_HEADER_NAME = "Authorization";
    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";


    static final String CONF_ISSUER_URL = "tokenAuthenticationOIDCIssuerUrl";

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
    private String issuerUrl;
    private JwtParser parser;



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
        // we need to fetch the algorithm before we fetch the key
        this.roleClaim = getTokenRoleClaim(config);
        this.audienceClaim = getTokenAudienceClaim(config);
        this.audience = getTokenAudience(config);
        this.issuerUrl = getIssuerUrl(config);

        URL issuerUrl = new URL(this.issuerUrl);
        URL configUrl = new URL(issuerUrl,"/.well-known/openid-configuration");
        if(!configUrl.getProtocol().equals("https")){
            throw new MalformedURLException("protocol needs to be https");
        }
        //extracting the jwks_uri
        JsonObject json = new Gson().fromJson(IOUtils.toString(configUrl, UTF_8), JsonObject.class);
        URL jwksUrl = new URL(json.get("jwks_uri").getAsString());
        this.provider = new UrlJwkProvider(new URL(json.get("jwks_uri").getAsString()));

        this.parser = Jwts.parserBuilder().setSigningKeyResolver(new JwksResolver(jwksUrl, Clock.systemDefaultZone())).requireIssuer(this.issuerUrl).build();
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
            Jws<Claims> jwt;
            token = getToken(authData);

            // Parse Token by validating

            jwt = authenticateToken(token);
            role = getPrincipal(jwt);


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
    private Jws<Claims> authenticateToken(final String token) throws AuthenticationException {
        //DecodedJWT jwt = JWT.decode(token);

        Jwk jwk = null;
        Algorithm algorithm = null;
        try {
            // parse the token and verify the signature
            Jws<Claims> jwt = this.parser.parseClaimsJws(token);
            /*jwk = provider.get(jwt.getKeyId());
            if(!(jwk.getPublicKey() instanceof RSAPublicKey)){
                throw new JwtException("key needs to be a RSA Publickey");
            }

            algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
            algorithm.verify(jwt); // if the token signature is invalid, the method will throw SignatureVerificationException*/
            if (audience != null){
                Object object = audienceClaim != null ? jwt.getBody().get(audienceClaim): jwt.getBody().get("aud");
                if (object == null) {
                    throw new JwtException("Found null Audience in token, for claimed field: " + audienceClaim);
                }

                if (object instanceof List) {
                    List<String> audiences = (List<String>) object;
                    // audience not contains this broker, throw exception.
                    if (!audiences.contains(audience)) {
                        throw new AuthenticationException("Audiences in token doesn't contain :"+ audience);
                    }
                } else if (object instanceof String) {
                    if (!object.equals(audience)) {
                        throw new AuthenticationException("Audiences in token doesn't contain :"+ audience);
                    }
                } else {
                    // should not reach here.
                    throw new AuthenticationException("Audiences in token is not in expected format: " + object);
                }

            }

            return jwt;
        } catch (SignatureVerificationException e) {
            throw new AuthenticationException("Failed to authentication token: " + e.getMessage());
        }

    }

    private String getPrincipal(Jwt<?, Claims> jwt) {

        return jwt.getBody().getSubject();
    }

    private String getTokenRoleClaim(ServiceConfiguration conf){
        if (conf.getProperty(CONF_TOKEN_AUTH_CLAIM) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_AUTH_CLAIM))) {
            return (String) conf.getProperty(CONF_TOKEN_AUTH_CLAIM);
        } else {
            return Claims.SUBJECT;
        }
    }

    private String getIssuerUrl(ServiceConfiguration conf){
        if (conf.getProperty(CONF_ISSUER_URL) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_ISSUER_URL))) {
            return (String) conf.getProperty(CONF_ISSUER_URL);
        } else {
            return "";
        }
    }

    // get Token Audience Claim from configuration, if not configured return null.
    private String getTokenAudienceClaim(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(CONF_TOKEN_AUDIENCE_CLAIM) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_AUDIENCE_CLAIM))) {
            return (String) conf.getProperty(CONF_TOKEN_AUDIENCE_CLAIM);
        } else {
            return null;
        }
    }

    // get Token Audience that stands for this broker from configuration, if not configured return null.
    private String getTokenAudience(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(CONF_TOKEN_AUDIENCE) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_AUDIENCE))) {
            return (String) conf.getProperty(CONF_TOKEN_AUDIENCE);
        } else {
            return null;
        }
    }

    private static final class TokenAuthenticationState implements AuthenticationState {
        private final AuthenticationProviderTokenOIDC provider;
        private AuthenticationDataSource authenticationDataSource;
        private Jwt<?,Claims> jwt;
        private final SocketAddress remoteAddress;
        private final SSLSession sslSession;
        private long expiration;

        TokenAuthenticationState(
                AuthenticationProviderTokenOIDC provider,
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

class JwksResolver implements SigningKeyResolver {

    private final URL jwksUrl;
    private final Clock clock;
    private Duration connectTimeout;
    private Duration readTimeout;
    private Duration cacheTimeout;

    private transient Map<String, PublicKey> cache;
    private transient Instant cacheExpiry;

    public JwksResolver(URL jwksUrl, Clock clock) {
        this.jwksUrl = jwksUrl;
        this.clock = clock;
        this.cache = new LinkedHashMap<>(0);
        this.cacheTimeout = Duration.ofMinutes(10);
    }

    public JwksResolver withConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public JwksResolver withReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public JwksResolver withCacheTimeout(Duration cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
        return this;
    }

    @Override
    public synchronized Key resolveSigningKey(JwsHeader header, Claims claims) {
        return resolve(header);
    }

    @Override
    public synchronized Key resolveSigningKey(JwsHeader header, String plaintext) {
        return resolve(header);
    }

            /**
             * Returns the signing key that should be used to validate a digital signature for a JWS with
             * the specified header.
             * @param header the header of the JWS to validate.
             */
    Key resolve(JwsHeader header) {
        String keyId = header.getKeyId();
        // check cache
        PublicKey key = this.cache.get(keyId);
        if (key != null) {
            return key;
        }

        // update cache if necessary
        if (this.cacheExpiry == null || Instant.now(clock).isAfter(cacheExpiry)) {
            try {
                this.updateCache();
                this.cacheExpiry = Instant.now(clock).plus(this.cacheTimeout);
            } catch (Exception e) {
                throw new IllegalStateException("the JWKS is unavailable", e);
            }

            // check cache again
            key = this.cache.get(header.getKeyId());
        }

        if (key == null) {
            throw new InvalidKeyException("Unable to resolve key with id '" + keyId + "'");
        }
        return key;
    }

    /**
     * Downloads and updates the cache of JWK public keys.
     * @throws IOException if the JWKS could not be downloaded.
     */
    private void updateCache() throws IOException {

    }
}
