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

import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsBoolean;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsInt;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsSet;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsString;
import com.auth0.jwk.InvalidPublicKeyException;
import com.auth0.jwk.Jwk;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.AlgorithmMismatchException;
import com.auth0.jwt.exceptions.InvalidClaimException;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Verification;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import okhttp3.OkHttpClient;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.api.AuthData;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AuthenticationProvider} implementation that supports the usage of a JSON Web Token (JWT)
 * for client authentication. This implementation retrieves the PublicKey from the JWT issuer (assuming the
 * issuer is in the configured allowed list) and then uses that Public Key to verify the validity of the JWT's
 * signature.
 *
 * The Public Keys for a given provider are cached based on certain configured parameters to improve performance.
 * The tradeoff here is that the longer Public Keys are cached, the longer an invalidated token could be used. One way
 * to ensure caches are cleared is to restart all brokers.
 *
 * Class is called from multiple threads. The implementation must be thread safe. This class expects to be loaded once
 * and then called concurrently for each new connection. The cache is backed by a GuavaCachedJwkProvider, which is
 * thread-safe.
 *
 * Supported algorithms are: RS256, RS384, RS512, ES256, ES384, ES512 where the naming conventions follow
 * this RFC: https://datatracker.ietf.org/doc/html/rfc7518#section-3.1.
 */
public class AuthenticationProviderOpenID implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(AuthenticationProviderOpenID.class);

    // Must match the value used by the OAuth2 Client Plugin.
    private static final String AUTH_METHOD_NAME = "token";

    // This is backed by an ObjectMapper, which is thread safe. It is an optimization
    // to share this for decoding JWTs for all connections to this broker.
    private final JWT jwtLibrary = new JWT();

    private Set<String> issuers;

    // This caches the map from Issuer URL to the jwks_uri served at the /.well-known/openid-configuration endpoint
    private OpenIDProviderMetadataCache openIDProviderMetadataCache;

    // A cache used to store the results of getting the JWKS from the jwks_uri for an issuer.
    private JwksCache jwksCache;

    private volatile AsyncHttpClient httpClient;

    // A list of supported algorithms. This is the "alg" field on the JWT.
    // Source for strings: https://datatracker.ietf.org/doc/html/rfc7518#section-3.1.
    private static final String ALG_RS256 = "RS256";
    private static final String ALG_RS384 = "RS384";
    private static final String ALG_RS512 = "RS512";
    private static final String ALG_ES256 = "ES256";
    private static final String ALG_ES384 = "ES384";
    private static final String ALG_ES512 = "ES512";

    private long acceptedTimeLeewaySeconds;
    private FallbackDiscoveryMode fallbackDiscoveryMode;
    private String roleClaim = ROLE_CLAIM_DEFAULT;
    private boolean isRoleClaimNotSubject;

    static final String ALLOWED_TOKEN_ISSUERS = "openIDAllowedTokenIssuers";
    static final String ISSUER_TRUST_CERTS_FILE_PATH = "openIDTokenIssuerTrustCertsFilePath";
    static final String FALLBACK_DISCOVERY_MODE = "openIDFallbackDiscoveryMode";
    static final String ALLOWED_AUDIENCES = "openIDAllowedAudiences";
    static final String ROLE_CLAIM = "openIDRoleClaim";
    static final String ROLE_CLAIM_DEFAULT = "sub";
    static final String ACCEPTED_TIME_LEEWAY_SECONDS = "openIDAcceptedTimeLeewaySeconds";
    static final int ACCEPTED_TIME_LEEWAY_SECONDS_DEFAULT = 0;
    static final String CACHE_SIZE = "openIDCacheSize";
    static final int CACHE_SIZE_DEFAULT = 5;
    static final String CACHE_REFRESH_AFTER_WRITE_SECONDS = "openIDCacheRefreshAfterWriteSeconds";
    static final int CACHE_REFRESH_AFTER_WRITE_SECONDS_DEFAULT = 18 * 60 * 60;
    static final String CACHE_EXPIRATION_SECONDS = "openIDCacheExpirationSeconds";
    static final int CACHE_EXPIRATION_SECONDS_DEFAULT = 24 * 60 * 60;
    static final String KEY_ID_CACHE_MISS_REFRESH_SECONDS = "openIDKeyIdCacheMissRefreshSeconds";
    static final int KEY_ID_CACHE_MISS_REFRESH_SECONDS_DEFAULT = 5 * 60;
    static final String HTTP_CONNECTION_TIMEOUT_MILLIS = "openIDHttpConnectionTimeoutMillis";
    static final int HTTP_CONNECTION_TIMEOUT_MILLIS_DEFAULT = 10_000;
    static final String HTTP_READ_TIMEOUT_MILLIS = "openIDHttpReadTimeoutMillis";
    static final int HTTP_READ_TIMEOUT_MILLIS_DEFAULT = 10_000;
    static final String REQUIRE_HTTPS = "openIDRequireIssuersUseHttps";
    static final boolean REQUIRE_HTTPS_DEFAULT = true;

    // The list of audiences that are allowed to connect to this broker. A valid JWT must contain one of the audiences.
    private String[] allowedAudiences;
    private ApiClient k8sApiClient;

    private AuthenticationMetrics authenticationMetrics;

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        initialize(Context.builder().config(config).build());
    }

    @Override
    public void initialize(Context context) throws IOException {
        authenticationMetrics = new AuthenticationMetrics(context.getOpenTelemetry(),
                getClass().getSimpleName(), getAuthMethodName());
        var config = context.getConfig();
        this.allowedAudiences = validateAllowedAudiences(getConfigValueAsSet(config, ALLOWED_AUDIENCES));
        this.roleClaim = getConfigValueAsString(config, ROLE_CLAIM, ROLE_CLAIM_DEFAULT);
        this.isRoleClaimNotSubject = !ROLE_CLAIM_DEFAULT.equals(roleClaim);
        this.acceptedTimeLeewaySeconds = getConfigValueAsInt(config, ACCEPTED_TIME_LEEWAY_SECONDS,
                ACCEPTED_TIME_LEEWAY_SECONDS_DEFAULT);
        boolean requireHttps = getConfigValueAsBoolean(config, REQUIRE_HTTPS, REQUIRE_HTTPS_DEFAULT);
        this.fallbackDiscoveryMode = FallbackDiscoveryMode.valueOf(getConfigValueAsString(config,
                FALLBACK_DISCOVERY_MODE, FallbackDiscoveryMode.DISABLED.name()));
        this.issuers = validateIssuers(getConfigValueAsSet(config, ALLOWED_TOKEN_ISSUERS), requireHttps,
                fallbackDiscoveryMode != FallbackDiscoveryMode.DISABLED);

        int connectionTimeout = getConfigValueAsInt(config, HTTP_CONNECTION_TIMEOUT_MILLIS,
                HTTP_CONNECTION_TIMEOUT_MILLIS_DEFAULT);
        int readTimeout = getConfigValueAsInt(config, HTTP_READ_TIMEOUT_MILLIS, HTTP_READ_TIMEOUT_MILLIS_DEFAULT);
        String trustCertsFilePath = getConfigValueAsString(config, ISSUER_TRUST_CERTS_FILE_PATH, null);
        SslContext sslContext = null;
        // When config is in the conf file but is empty, it defaults to the empty string, which is not meaningful and
        // should be ignored.
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            // Use default settings for everything but the trust store.
            sslContext = SslContextBuilder.forClient()
                    .trustManager(new File(trustCertsFilePath))
                    .build();
        }
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
                .setConnectTimeout(connectionTimeout)
                .setReadTimeout(readTimeout)
                .setSslContext(sslContext)
                .build();
        httpClient = new DefaultAsyncHttpClient(clientConfig);
        k8sApiClient = fallbackDiscoveryMode != FallbackDiscoveryMode.DISABLED ? Config.defaultClient() : null;
        this.openIDProviderMetadataCache = new OpenIDProviderMetadataCache(this, config, httpClient, k8sApiClient);
        this.jwksCache = new JwksCache(this, config, httpClient, k8sApiClient);
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void incrementFailureMetric(Enum<?> errorCode) {
        authenticationMetrics.recordFailure(errorCode);
    }

    /**
     * Authenticate the parameterized {@link AuthenticationDataSource} by verifying the issuer is an allowed issuer,
     * then retrieving the JWKS URI from the issuer, then retrieving the Public key from the JWKS URI, and finally
     * verifying the JWT signature and claims.
     *
     * @param authData - the authData passed by the Pulsar Broker containing the token.
     * @return the role, if the JWT is authenticated, otherwise a failed future.
     */
    @Override
    public CompletableFuture<String> authenticateAsync(AuthenticationDataSource authData) {
        return authenticateTokenAsync(authData).thenApply(this::getRole);
    }

    /**
     * Authenticate the parameterized {@link AuthenticationDataSource} and return the decoded JWT.
     * @param authData - the authData containing the token.
     * @return a completed future with the decoded JWT, if the JWT is authenticated. Otherwise, a failed future.
     */
    CompletableFuture<DecodedJWT> authenticateTokenAsync(AuthenticationDataSource authData) {
        String token;
        try {
            token = AuthenticationProviderToken.getToken(authData);
        } catch (AuthenticationException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            return CompletableFuture.failedFuture(e);
        }
        return authenticateToken(token)
                .whenComplete((jwt, e) -> {
                    if (jwt != null) {
                        authenticationMetrics.recordSuccess();
                    }
                    // Failure metrics are incremented within methods above
                });
    }

    /**
     * Get the role from a JWT at the configured role claim field.
     * NOTE: does not do any verification of the JWT
     * @param jwt - token to get the role from
     * @return the role, or null, if it is not set on the JWT
     */
    String getRole(DecodedJWT jwt) {
        try {
            Claim roleClaim = jwt.getClaim(this.roleClaim);
            if (roleClaim.isNull()) {
                // The claim was not present in the JWT
                return null;
            }

            String role = roleClaim.asString();
            if (role != null) {
                // The role is non null only if the JSON node is a text field
                return role;
            }

            List<String> roles = jwt.getClaim(this.roleClaim).asList(String.class);
            if (roles == null || roles.size() == 0) {
                return null;
            } else if (roles.size() == 1) {
                return roles.get(0);
            } else {
                log.debug("JWT for subject [{}] has multiple roles; using the first one.", jwt.getSubject());
                return roles.get(0);
            }
        } catch (JWTDecodeException e) {
            log.error("Exception while retrieving role from JWT", e);
            return null;
        }
    }

    /**
     * Convert a JWT string into a {@link DecodedJWT}
     * The benefit of using this method is that it utilizes the already instantiated {@link JWT} parser.
     * WARNING: this method does not verify the authenticity of the token. It only decodes it.
     *
     * @param token - string JWT to be decoded
     * @return a decoded JWT
     * @throws AuthenticationException if the token string is null or if any part of the token contains
     *         an invalid jwt or JSON format of each of the jwt parts.
     */
    DecodedJWT decodeJWT(String token) throws AuthenticationException {
        if (token == null) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            throw new AuthenticationException("Invalid token: cannot be null");
        }
        try {
            return jwtLibrary.decodeJwt(token);
        } catch (JWTDecodeException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            throw new AuthenticationException("Unable to decode JWT: " + e.getMessage());
        }
    }

    /**
     * Authenticate the parameterized JWT.
     *
     * @param token - a nonnull JWT to authenticate
     * @return a fully authenticated JWT, or AuthenticationException if the JWT is proven to be invalid in any way
     */
    private CompletableFuture<DecodedJWT> authenticateToken(String token) {
        if (token == null) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            return CompletableFuture.failedFuture(new AuthenticationException("JWT cannot be null"));
        }
        final DecodedJWT jwt;
        try {
            jwt = decodeJWT(token);
        } catch (AuthenticationException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            return CompletableFuture.failedFuture(e);
        }
        return verifyIssuerAndGetJwk(jwt)
                .thenCompose(jwk -> {
                    try {
                        // verify the algorithm, if it is set ("alg" is optional in the JWK spec)
                        if (jwk.getAlgorithm() != null && !jwt.getAlgorithm().equals(jwk.getAlgorithm())) {
                            incrementFailureMetric(AuthenticationExceptionCode.ALGORITHM_MISMATCH);
                            return CompletableFuture.failedFuture(
                                    new AuthenticationException("JWK's alg [" + jwk.getAlgorithm()
                                            + "] does not match JWT's alg [" + jwt.getAlgorithm() + "]"));
                        }
                        // Verify the JWT signature
                        // Throws exception if any verification check fails
                        return CompletableFuture
                                .completedFuture(verifyJWT(jwk.getPublicKey(), jwt.getAlgorithm(), jwt));
                    } catch (InvalidPublicKeyException e) {
                        incrementFailureMetric(AuthenticationExceptionCode.INVALID_PUBLIC_KEY);
                        return CompletableFuture.failedFuture(
                                new AuthenticationException("Invalid public key: " + e.getMessage()));
                    } catch (AuthenticationException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });
    }

    /**
     * Verify the JWT's issuer (iss) claim is one of the allowed issuers and then retrieve the JWK from the issuer. If
     * not, see {@link FallbackDiscoveryMode} for the fallback behavior.
     * @param jwt - the token to use to discover the issuer's JWKS URI, which is then used to retrieve the issuer's
     *            current public keys.
     * @return a JWK that can be used to verify the JWT's signature
     */
    private CompletableFuture<Jwk> verifyIssuerAndGetJwk(DecodedJWT jwt) {
        if (jwt.getIssuer() == null) {
            incrementFailureMetric(AuthenticationExceptionCode.UNSUPPORTED_ISSUER);
            return CompletableFuture.failedFuture(new AuthenticationException("Issuer cannot be null"));
        } else if (this.issuers.contains(jwt.getIssuer())) {
            // Retrieve the metadata: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
            return openIDProviderMetadataCache.getOpenIDProviderMetadataForIssuer(jwt.getIssuer())
                    .thenCompose(metadata -> jwksCache.getJwk(metadata.getJwksUri(), jwt.getKeyId()));
        } else if (fallbackDiscoveryMode == FallbackDiscoveryMode.KUBERNETES_DISCOVER_TRUSTED_ISSUER) {
            return openIDProviderMetadataCache.getOpenIDProviderMetadataForKubernetesApiServer(jwt.getIssuer())
                    .thenCompose(metadata ->
                            openIDProviderMetadataCache.getOpenIDProviderMetadataForIssuer(metadata.getIssuer()))
                    .thenCompose(metadata -> jwksCache.getJwk(metadata.getJwksUri(), jwt.getKeyId()));
        } else if (fallbackDiscoveryMode == FallbackDiscoveryMode.KUBERNETES_DISCOVER_PUBLIC_KEYS) {
            return openIDProviderMetadataCache.getOpenIDProviderMetadataForKubernetesApiServer(jwt.getIssuer())
                    .thenCompose(__ -> jwksCache.getJwkFromKubernetesApiServer(jwt.getKeyId()));
        } else {
            incrementFailureMetric(AuthenticationExceptionCode.UNSUPPORTED_ISSUER);
            return CompletableFuture
                    .failedFuture(new AuthenticationException("Issuer not allowed: " + jwt.getIssuer()));
        }
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
            throws AuthenticationException {
        return new AuthenticationStateOpenID(this, remoteAddress, sslSession);
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
        if (k8sApiClient != null) {
            OkHttpClient okHttpClient = k8sApiClient.getHttpClient();
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
            if (okHttpClient.cache() != null) {
                okHttpClient.cache().close();
            }
        }
    }

    /**
     * Build and return a validator for the parameters.
     *
     * @param publicKey - the public key to use when configuring the validator
     * @param publicKeyAlg - the algorithm for the parameterized public key
     * @param jwt - jwt to be verified and returned (only if verified)
     * @return a validator to use for validating a JWT associated with the parameterized public key.
     * @throws AuthenticationException if the Public Key's algorithm is not supported or if the algorithm param does not
     * match the Public Key's actual algorithm.
     */
    DecodedJWT verifyJWT(PublicKey publicKey,
                                String publicKeyAlg,
                                DecodedJWT jwt) throws AuthenticationException {
        if (publicKeyAlg == null) {
            incrementFailureMetric(AuthenticationExceptionCode.UNSUPPORTED_ALGORITHM);
            throw new AuthenticationException("PublicKey algorithm cannot be null");
        }

        Algorithm alg;
        try {
            switch (publicKeyAlg) {
                case ALG_RS256:
                    alg = Algorithm.RSA256((RSAPublicKey) publicKey, null);
                    break;
                case ALG_RS384:
                    alg = Algorithm.RSA384((RSAPublicKey) publicKey, null);
                    break;
                case ALG_RS512:
                    alg = Algorithm.RSA512((RSAPublicKey) publicKey, null);
                    break;
                case ALG_ES256:
                    alg = Algorithm.ECDSA256((ECPublicKey) publicKey, null);
                    break;
                case ALG_ES384:
                    alg = Algorithm.ECDSA384((ECPublicKey) publicKey, null);
                    break;
                case ALG_ES512:
                    alg = Algorithm.ECDSA512((ECPublicKey) publicKey, null);
                    break;
                default:
                    incrementFailureMetric(AuthenticationExceptionCode.UNSUPPORTED_ALGORITHM);
                    throw new AuthenticationException("Unsupported algorithm: " + publicKeyAlg);
            }
        } catch (ClassCastException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ALGORITHM_MISMATCH);
            throw new AuthenticationException("Expected PublicKey alg [" + publicKeyAlg + "] does match actual alg.");
        }

        // We verify issuer when retrieving the PublicKey, so it is not verified here.
        // The claim presence requirements are based on https://openid.net/specs/openid-connect-basic-1_0.html#IDToken
         Verification verifierBuilder = JWT.require(alg)
                .acceptLeeway(acceptedTimeLeewaySeconds)
                .withAnyOfAudience(allowedAudiences)
                .withClaimPresence(RegisteredClaims.ISSUED_AT)
                .withClaimPresence(RegisteredClaims.EXPIRES_AT)
                .withClaimPresence(RegisteredClaims.NOT_BEFORE)
                .withClaimPresence(RegisteredClaims.SUBJECT);

        if (isRoleClaimNotSubject) {
            verifierBuilder = verifierBuilder.withClaimPresence(roleClaim);
        }

        JWTVerifier verifier = verifierBuilder.build();

        try {
            return verifier.verify(jwt);
        } catch (TokenExpiredException e) {
            incrementFailureMetric(AuthenticationExceptionCode.EXPIRED_JWT);
            throw new AuthenticationException("JWT expired: " + e.getMessage());
        } catch (SignatureVerificationException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_VERIFYING_JWT_SIGNATURE);
            throw new AuthenticationException("JWT signature verification exception: " + e.getMessage());
        } catch (InvalidClaimException e) {
            incrementFailureMetric(AuthenticationExceptionCode.INVALID_JWT_CLAIM);
            throw new AuthenticationException("JWT contains invalid claim: " + e.getMessage());
        } catch (AlgorithmMismatchException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ALGORITHM_MISMATCH);
            throw new AuthenticationException("JWT algorithm does not match Public Key algorithm: " + e.getMessage());
        } catch (JWTDecodeException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_DECODING_JWT);
            throw new AuthenticationException("Error while decoding JWT: " + e.getMessage());
        } catch (JWTVerificationException | IllegalArgumentException e) {
            incrementFailureMetric(AuthenticationExceptionCode.ERROR_VERIFYING_JWT);
            throw new AuthenticationException("JWT verification failed: " + e.getMessage());
        }
    }

    /**
     * Validate the configured allow list of allowedIssuers. The allowedIssuers set must be nonempty in order for
     * the plugin to authenticate any token. Thus, it fails initialization if the configuration is
     * missing. Each issuer URL should use the HTTPS scheme. The plugin fails initialization if any
     * issuer url is insecure, unless requireHttps is false.
     * @param allowedIssuers - issuers to validate
     * @param requireHttps - whether to require https for issuers.
     * @param allowEmptyIssuers - whether to allow empty issuers. This setting only makes sense when kubernetes is used
     *                   as a fallback issuer.
     * @return the validated issuers
     * @throws IllegalArgumentException if the allowedIssuers is empty, or contains insecure issuers when required
     */
    private Set<String> validateIssuers(Set<String> allowedIssuers, boolean requireHttps, boolean allowEmptyIssuers) {
        if (allowedIssuers == null || (allowedIssuers.isEmpty() && !allowEmptyIssuers)) {
            throw new IllegalArgumentException("Missing configured value for: " + ALLOWED_TOKEN_ISSUERS);
        }
        for (String issuer : allowedIssuers) {
            if (!issuer.toLowerCase().startsWith("https://")) {
                log.warn("Allowed issuer is not using https scheme: {}", issuer);
                if (requireHttps) {
                    throw new IllegalArgumentException("Issuer URL does not use https, but must: " + issuer);
                }
            }
        }
        return allowedIssuers;
    }

    /**
     * Validate the configured allow list of allowedAudiences. The allowedAudiences must be set because
     * JWT must have an audience claim.
     * See https://openid.net/specs/openid-connect-basic-1_0.html#IDTokenValidation.
     * @param allowedAudiences
     * @return the validated audiences
     */
    String[] validateAllowedAudiences(Set<String> allowedAudiences) {
        if (allowedAudiences == null || allowedAudiences.isEmpty()) {
            throw new IllegalArgumentException("Missing configured value for: " + ALLOWED_AUDIENCES);
        }
        return allowedAudiences.toArray(new String[0]);
    }
}
