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

import static org.apache.pulsar.broker.authentication.oidc.AuthenticationExceptionCode.ERROR_RETRIEVING_PROVIDER_METADATA;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_REFRESH_AFTER_WRITE_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_REFRESH_AFTER_WRITE_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsInt;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.WellKnownApi;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.asynchttpclient.AsyncHttpClient;

/**
 * Class used to cache metadata responses from OpenID Providers.
 */
class OpenIDProviderMetadataCache {

    private final ObjectReader reader = new ObjectMapper().readerFor(OpenIDProviderMetadata.class);
    private final AuthenticationProvider authenticationProvider;
    private final AsyncHttpClient httpClient;
    private final WellKnownApi wellKnownApi;
    private final AsyncLoadingCache<Optional<String>, OpenIDProviderMetadata> cache;
    private static final String WELL_KNOWN_OPENID_CONFIG = ".well-known/openid-configuration";
    private static final String SLASH_WELL_KNOWN_OPENID_CONFIG = "/" + WELL_KNOWN_OPENID_CONFIG;

    OpenIDProviderMetadataCache(AuthenticationProvider authenticationProvider, ServiceConfiguration config,
                                AsyncHttpClient httpClient, ApiClient apiClient) {
        this.authenticationProvider = authenticationProvider;
        int maxSize = getConfigValueAsInt(config, CACHE_SIZE, CACHE_SIZE_DEFAULT);
        int refreshAfterWriteSeconds = getConfigValueAsInt(config, CACHE_REFRESH_AFTER_WRITE_SECONDS,
                CACHE_REFRESH_AFTER_WRITE_SECONDS_DEFAULT);
        int expireAfterSeconds = getConfigValueAsInt(config, CACHE_EXPIRATION_SECONDS,
                CACHE_EXPIRATION_SECONDS_DEFAULT);
        this.httpClient = httpClient;
        this.wellKnownApi = apiClient != null ? new WellKnownApi(apiClient) : null;
        AsyncCacheLoader<Optional<String>, OpenIDProviderMetadata> loader = (issuer, executor) -> {
            if (issuer.isPresent()) {
                return loadOpenIDProviderMetadataForIssuer(issuer.get());
            } else {
                return loadOpenIDProviderMetadataForKubernetesApiServer();
            }
        };
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshAfterWriteSeconds, TimeUnit.SECONDS)
                .expireAfterWrite(expireAfterSeconds, TimeUnit.SECONDS)
                .buildAsync(loader);
    }

    /**
     * Retrieve the OpenID Provider Metadata for the provided issuer.
     * <p>
     * Note: this method does not do any validation on the parameterized issuer. The OpenID Connect discovery
     * spec requires that the issuer use the HTTPS scheme: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata.
     * The {@link AuthenticationProviderOpenID} class handles this verification.
     *
     * @param issuer - authority from which to retrieve the OpenID Provider Metadata
     * @return the {@link OpenIDProviderMetadata} for the given issuer. Fail the completable future with
     * AuthenticationException if any exceptions occur while retrieving the metadata.
     */
    CompletableFuture<OpenIDProviderMetadata> getOpenIDProviderMetadataForIssuer(@Nonnull String issuer) {
        return cache.get(Optional.of(issuer));
    }

    /**
     * A loader for the cache that retrieves the metadata from the issuer's /.well-known/openid-configuration endpoint.
     * @return a connection to the issuer's /.well-known/openid-configuration endpoint. Fails with
     * AuthenticationException if the URL is malformed or there is an exception while opening the connection
     */
    private CompletableFuture<OpenIDProviderMetadata> loadOpenIDProviderMetadataForIssuer(String issuer) {
        String url;
        // TODO URI's normalization likely follows RFC2396 (library doesn't say so explicitly), whereas the spec
        //  https://openid.net/specs/openid-connect-discovery-1_0.html#NormalizationSteps
        //  calls for normalization according to RFC3986, which is supposed to obsolete RFC2396. Is this a problem?
        if (issuer.endsWith("/")) {
            url = issuer + WELL_KNOWN_OPENID_CONFIG;
        } else {
            url = issuer + SLASH_WELL_KNOWN_OPENID_CONFIG;
        }

        return httpClient
                .prepareGet(url)
                .execute()
                .toCompletableFuture()
                .thenCompose(result -> {
                    CompletableFuture<OpenIDProviderMetadata> future = new CompletableFuture<>();
                    try {
                        OpenIDProviderMetadata openIDProviderMetadata =
                                reader.readValue(result.getResponseBodyAsBytes());
                        // We can verify this issuer once and cache the result because the issuer uniquely maps
                        // to the cached object.
                        verifyIssuer(issuer, openIDProviderMetadata, false);
                        future.complete(openIDProviderMetadata);
                    } catch (AuthenticationException e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
                        future.completeExceptionally(e);
                    } catch (Exception e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
                        future.completeExceptionally(new AuthenticationException(
                                "Error retrieving OpenID Provider Metadata at " + issuer + ": " + e.getMessage()));
                    }
                    return future;
                });
    }

    /**
     * Retrieve the OpenID Provider Metadata for the Kubernetes API server. This method is used instead of
     * {@link #getOpenIDProviderMetadataForIssuer(String)} because different validations are done. The Kubernetes
     * API server does not technically implement the complete OIDC spec for discovery, but it does implement some of
     * it, so this method validates what it can. Specifically, it skips validation that the Discovery Document
     * provider's URI matches the issuer. It verifies that the issuer on the discovery document matches the issuer
     * claim
     * @return
     */
    CompletableFuture<OpenIDProviderMetadata> getOpenIDProviderMetadataForKubernetesApiServer(String issClaim) {
        return cache.get(Optional.empty()).thenCompose(openIDProviderMetadata -> {
            CompletableFuture<OpenIDProviderMetadata> future = new CompletableFuture<>();
            try {
                verifyIssuer(issClaim, openIDProviderMetadata, true);
                future.complete(openIDProviderMetadata);
            } catch (AuthenticationException e) {
                authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
                future.completeExceptionally(e);
            }
            return future;
        });
    }

    private CompletableFuture<OpenIDProviderMetadata> loadOpenIDProviderMetadataForKubernetesApiServer() {
        CompletableFuture<OpenIDProviderMetadata> future = new CompletableFuture<>();
        try {
            wellKnownApi.getServiceAccountIssuerOpenIDConfigurationAsync(new ApiCallback<>() {
                @Override
                public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                    authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
                    // We want the message and responseBody here: https://github.com/kubernetes-client/java/issues/2066.
                    future.completeExceptionally(new AuthenticationException(
                            "Error retrieving OpenID Provider Metadata from Kubernetes API server. Message: "
                                    + e.getMessage() + " Response body: " + e.getResponseBody()));
                }

                @Override
                public void onSuccess(String result, int statusCode, Map<String, List<String>> responseHeaders) {
                    try {
                        // Validation that the token's issuer matches the issuer returned by the api server must be done
                        // after the cache load operation to ensure each token's issuer matches the fallback issuer
                        OpenIDProviderMetadata openIDProviderMetadata = reader.readValue(result);
                        future.complete(openIDProviderMetadata);
                    } catch (Exception e) {
                        authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
                        future.completeExceptionally(new AuthenticationException(
                                "Error retrieving OpenID Provider Metadata from Kubernetes API Server: "
                                        + e.getMessage()));
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
            authenticationProvider.incrementFailureMetric(ERROR_RETRIEVING_PROVIDER_METADATA);
            future.completeExceptionally(new AuthenticationException(
                    "Error retrieving OpenID Provider Metadata from Kubernetes API server: " + e.getMessage()));
        }
        return future;
    }

    /**
     * Verify the issuer url, as required by the OpenID Connect spec:
     *
     * Per the OpenID Connect Discovery spec, the issuer value returned MUST be identical to the
     * Issuer URL that was directly used to retrieve the configuration information. This MUST also
     * be identical to the iss Claim value in ID Tokens issued from this Issuer.
     * https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationValidation
     *
     * @param issuer - the issuer used to retrieve the metadata
     * @param metadata - the OpenID Provider Metadata
     * @param isK8s - whether the issuer is represented by the Kubernetes API server. This affects error reporting.
     * @throws AuthenticationException if the issuer does not exactly match the metadata issuer
     */
    private void verifyIssuer(@Nonnull String issuer, OpenIDProviderMetadata metadata,
                              boolean isK8s) throws AuthenticationException {
        if (!issuer.equals(metadata.getIssuer())) {
            if (isK8s) {
                authenticationProvider.incrementFailureMetric(AuthenticationExceptionCode.UNSUPPORTED_ISSUER);
                throw new AuthenticationException("Issuer not allowed: " + issuer);
            } else {
                authenticationProvider.incrementFailureMetric(AuthenticationExceptionCode.ISSUER_MISMATCH);
                throw new AuthenticationException(String.format("Issuer URL mismatch: [%s] should match [%s]",
                        issuer, metadata.getIssuer()));
            }
        }
    }
}
