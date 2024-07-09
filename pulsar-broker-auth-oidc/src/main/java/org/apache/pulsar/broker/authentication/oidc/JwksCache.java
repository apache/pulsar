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

import static org.apache.pulsar.broker.authentication.oidc.AuthenticationExceptionCode.ERROR_RETRIEVING_PUBLIC_KEY;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_REFRESH_AFTER_WRITE_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_REFRESH_AFTER_WRITE_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.KEY_ID_CACHE_MISS_REFRESH_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.KEY_ID_CACHE_MISS_REFRESH_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsInt;
import com.auth0.jwk.Jwk;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.OpenidApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.asynchttpclient.AsyncHttpClient;

public class JwksCache {

    // Map from an issuer's JWKS URI to its JWKS. When the Issuer is not empty, use the fallback client.
    private final AsyncLoadingCache<Optional<String>, List<Jwk>> cache;
    private final ConcurrentHashMap<Optional<String>, Long> jwksLastRefreshTime = new ConcurrentHashMap<>();
    private final long keyIdCacheMissRefreshNanos;
    private final ObjectReader reader = new ObjectMapper().readerFor(HashMap.class);
    private final AsyncHttpClient httpClient;
    private final OpenidApi openidApi;
    private final AuthenticationProvider authenticationProvider;

    JwksCache(AuthenticationProvider authenticationProvider, ServiceConfiguration config,
              AsyncHttpClient httpClient, ApiClient apiClient) throws IOException {
        this.authenticationProvider = authenticationProvider;
        // Store the clients
        this.httpClient = httpClient;
        this.openidApi = apiClient != null ? new OpenidApi(apiClient) : null;
        keyIdCacheMissRefreshNanos = TimeUnit.SECONDS.toNanos(getConfigValueAsInt(config,
                KEY_ID_CACHE_MISS_REFRESH_SECONDS, KEY_ID_CACHE_MISS_REFRESH_SECONDS_DEFAULT));
        // Configure the cache
        int maxSize = getConfigValueAsInt(config, CACHE_SIZE, CACHE_SIZE_DEFAULT);
        int refreshAfterWriteSeconds = getConfigValueAsInt(config, CACHE_REFRESH_AFTER_WRITE_SECONDS,
                CACHE_REFRESH_AFTER_WRITE_SECONDS_DEFAULT);
        int expireAfterSeconds = getConfigValueAsInt(config, CACHE_EXPIRATION_SECONDS,
                CACHE_EXPIRATION_SECONDS_DEFAULT);
        AsyncCacheLoader<Optional<String>, List<Jwk>> loader = (jwksUri, executor) -> {
            // Store the time of the retrieval, even though it might be a little early or the call might fail.
            jwksLastRefreshTime.put(jwksUri, System.nanoTime());
            if (jwksUri.isPresent()) {
                return getJwksFromJwksUri(jwksUri.get());
            } else {
                return getJwksFromKubernetesApiServer();
            }
        };
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshAfterWriteSeconds, TimeUnit.SECONDS)
                .expireAfterWrite(expireAfterSeconds, TimeUnit.SECONDS)
                .buildAsync(loader);
    }

    CompletableFuture<Jwk> getJwk(String jwksUri, String keyId) {
        if (jwksUri == null) {
            authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
            return CompletableFuture.failedFuture(new IllegalArgumentException("jwksUri must not be null."));
        }
        return getJwkAndMaybeReload(Optional.of(jwksUri), keyId, false);
    }

    /**
     * Retrieve the JWK for the given key ID from the given JWKS URI. If the key ID is not found, and failOnMissingKeyId
     * is false, then the JWK will be reloaded from the JWKS URI and the key ID will be searched for again.
     */
    private CompletableFuture<Jwk> getJwkAndMaybeReload(Optional<String> maybeJwksUri,
                                                        String keyId,
                                                        boolean failOnMissingKeyId) {
        return cache
                .get(maybeJwksUri)
                .thenCompose(jwks -> {
                    try {
                        return CompletableFuture.completedFuture(getJwkForKID(maybeJwksUri, jwks, keyId));
                    } catch (IllegalArgumentException e) {
                        if (failOnMissingKeyId) {
                            throw e;
                        } else {
                            Long lastRefresh = jwksLastRefreshTime.get(maybeJwksUri);
                            if (lastRefresh == null || System.nanoTime() - lastRefresh > keyIdCacheMissRefreshNanos) {
                                // In this case, the key ID was not found, but we haven't refreshed the JWKS in a while,
                                // so it is possible the key ID was added. Refresh the JWKS and try again.
                                cache.synchronous().invalidate(maybeJwksUri);
                            }
                            // There is a small race condition where the JWKS could be refreshed by another thread,
                            // so we retry getting the JWK, even though we might not have invalidated the cache.
                            return getJwkAndMaybeReload(maybeJwksUri, keyId, true);
                        }
                    }
                });
    }

    private CompletableFuture<List<Jwk>> getJwksFromJwksUri(String jwksUri) {
        return httpClient
                .prepareGet(jwksUri)
                .execute()
                .toCompletableFuture()
                .thenCompose(result -> {
                    CompletableFuture<List<Jwk>> future = new CompletableFuture<>();
                    try {
                        HashMap<String, Object> jwks =
                                reader.readValue(result.getResponseBodyAsBytes());
                        future.complete(convertToJwks(jwksUri, jwks));
                    } catch (AuthenticationException e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
                        future.completeExceptionally(e);
                    } catch (Exception e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
                        future.completeExceptionally(new AuthenticationException(
                                "Error retrieving public key at " + jwksUri + ": " + e.getMessage()));
                    }
                    return future;
                });
    }

    CompletableFuture<Jwk> getJwkFromKubernetesApiServer(String keyId) {
        if (openidApi == null) {
            authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
            return CompletableFuture.failedFuture(new AuthenticationException(
                    "Failed to retrieve public key from Kubernetes API server: Kubernetes fallback is not enabled."));
        }
        return getJwkAndMaybeReload(Optional.empty(), keyId, false);
    }

    private CompletableFuture<List<Jwk>> getJwksFromKubernetesApiServer() {
        CompletableFuture<List<Jwk>> future = new CompletableFuture<>();
        try {
            openidApi.getServiceAccountIssuerOpenIDKeysetAsync(new ApiCallback<String>() {
                @Override
                public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                    authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
                    // We want the message and responseBody here: https://github.com/kubernetes-client/java/issues/2066.
                    future.completeExceptionally(
                            new AuthenticationException("Failed to retrieve public key from Kubernetes API server. "
                                    + "Message: " + e.getMessage() + " Response body: " + e.getResponseBody()));
                }

                @Override
                public void onSuccess(String result, int statusCode, Map<String, List<String>> responseHeaders) {
                    try {
                        HashMap<String, Object> jwks = reader.readValue(result);
                        future.complete(convertToJwks("Kubernetes API server", jwks));
                    } catch (AuthenticationException e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
                        future.completeExceptionally(e);
                    } catch (Exception e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
                        future.completeExceptionally(new AuthenticationException(
                                "Error retrieving public key at Kubernetes API server: " + e.getMessage()));
                    }
                }

                @Override
                public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {

                }

                @Override
                public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {

                }
            });
        } catch (ApiException e) {
            authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
            future.completeExceptionally(
                    new AuthenticationException("Failed to retrieve public key from Kubernetes API server: "
                            + e.getMessage()));
        }
        return future;
    }

    private Jwk getJwkForKID(Optional<String> maybeJwksUri, List<Jwk> jwks, String keyId) {
        for (Jwk jwk : jwks) {
            if (jwk.getId().equals(keyId)) {
                return jwk;
            }
        }
        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PUBLIC_KEY);
        throw new IllegalArgumentException("No JWK found for Key ID " + keyId);
    }

    /**
     * The JWK Set is stored in the "keys" key see https://www.rfc-editor.org/rfc/rfc7517#section-5.1.
     *
     * @param jwksUri - the URI used to retrieve the JWKS
     * @param jwks - the JWKS to convert
     * @return a list of {@link Jwk}
     */
    private List<Jwk> convertToJwks(String jwksUri, Map<String, Object> jwks) throws AuthenticationException {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> jwkList = (List<Map<String, Object>>) jwks.get("keys");
            final List<Jwk> result = new ArrayList<>();
            for (Map<String, Object> jwk : jwkList) {
                result.add(Jwk.fromValues(jwk));
            }
            return result;
        } catch (ClassCastException e) {
            throw new AuthenticationException("Malformed JWKS returned by: " + jwksUri);
        }
    }
}
