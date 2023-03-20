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

import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_EXPIRATION_SECONDS_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE;
import static org.apache.pulsar.broker.authentication.oidc.AuthenticationProviderOpenID.CACHE_SIZE_DEFAULT;
import static org.apache.pulsar.broker.authentication.oidc.ConfigUtils.getConfigValueAsInt;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.asynchttpclient.AsyncHttpClient;

/**
 * Class used to cache metadata responses from OpenID Providers.
 */
class OpenIDProviderMetadataCache {

    private final ObjectReader reader = new ObjectMapper().readerFor(OpenIDProviderMetadata.class);

    /**
     * A loader for the cache that retrieves the metadata from the issuer's /.well-known/openid-configuration endpoint.
     * @return a connection to the issuer's /.well-known/openid-configuration endpoint
     * @throws AuthenticationException if the URL is malformed or there is an exception while opening the connection
     */
    private AsyncCacheLoader<String, OpenIDProviderMetadata> getLoader(AsyncHttpClient client) {
        return (issuer, executor) ->
                // TODO OIDC spec https://openid.net/specs/openid-connect-discovery-1_0.html#NormalizationSteps
                // calls for normalization according to RFC3986. Is that important to verify here?
                client
                    .prepareGet(issuer + "/.well-known/openid-configuration")
                    .execute()
                    .toCompletableFuture()
                    .thenCompose(result -> {
                        CompletableFuture<OpenIDProviderMetadata> future = new CompletableFuture<>();
                        try {
                            OpenIDProviderMetadata openIDProviderMetadata =
                                    reader.readValue(result.getResponseBodyAsBytes());
                            verifyIssuer(issuer, openIDProviderMetadata);
                            future.complete(openIDProviderMetadata);
                        } catch (IOException e) {
                            future.completeExceptionally(new AuthenticationException(
                                    "Error retrieving OpenID Provider Metadata at " + issuer + ": " + e.getMessage()));
                        } catch (AuthenticationException e) {
                            future.completeExceptionally(e);
                        }
                        return future;
                    });
    }

    private final AsyncLoadingCache<String, OpenIDProviderMetadata> cache;

    OpenIDProviderMetadataCache(ServiceConfiguration config, AsyncHttpClient httpClient) {
        int maxSize = getConfigValueAsInt(config, CACHE_SIZE, CACHE_SIZE_DEFAULT);
        int expireAfterSeconds = getConfigValueAsInt(config, CACHE_EXPIRATION_SECONDS,
                CACHE_EXPIRATION_SECONDS_DEFAULT);
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterSeconds, TimeUnit.SECONDS)
                .buildAsync(getLoader(httpClient));
    }

    /**
     * Retrieve the OpenID Provider Metadata for the provided issuer.
     * <p>
     * Note: this method does not do any validation on the parameterized issuer. The OpenID Connect discovery
     * spec requires that the issuer use the HTTPS scheme: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata.
     * The {@link AuthenticationProviderOpenID} class handles this verification.
     *
     * @param issuer - authority from which to retrieve the OpenID Provider Metadata
     * @return the {@link OpenIDProviderMetadata} for the given issuer
     * @throws AuthenticationException if any exceptions occur while retrieving the metadata.
     */
    CompletableFuture<OpenIDProviderMetadata> getOpenIDProviderMetadataForIssuer(String issuer) {
        if (issuer == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Issuer must not be null."));
        }
        return cache.get(issuer);
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
     * @throws AuthenticationException if the issuer does not exactly match the metadata issuer
     */
    private void verifyIssuer(@Nonnull String issuer, OpenIDProviderMetadata metadata) throws AuthenticationException {
        if (!issuer.equals(metadata.getIssuer())) {
            AuthenticationProviderOpenID.incrementFailureMetric(AuthenticationExceptionCode.ISSUER_MISMATCH);
            throw new AuthenticationException(String.format("Issuer URL mismatch: [%s] should match [%s]",
                    issuer, metadata.getIssuer()));
        }
    }
}
