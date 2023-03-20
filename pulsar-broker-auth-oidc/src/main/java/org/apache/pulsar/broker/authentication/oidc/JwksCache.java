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
import com.auth0.jwk.Jwk;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.asynchttpclient.AsyncHttpClient;

public class JwksCache {

    // Map from an issuer's JWKS URI to its JWKS.
    private final AsyncLoadingCache<String, List<Jwk>> cache;

    private final ObjectReader reader = new ObjectMapper().readerFor(HashMap.class);

    private AsyncCacheLoader<String, List<Jwk>> getLoader(AsyncHttpClient client) {
        return (jwksUri, executor) ->
                client
                        .prepareGet(jwksUri)
                        .execute()
                        .toCompletableFuture()
                        .thenCompose(result -> {
                            CompletableFuture<List<Jwk>> future = new CompletableFuture<>();
                            try {
                                HashMap<String, Object> jwks =
                                        reader.readValue(result.getResponseBodyAsBytes());
                                future.complete(convertToJwks(jwksUri, jwks));
                            } catch (IOException e) {
                                future.completeExceptionally(new AuthenticationException(
                                        "Error retrieving JWKS at " + jwksUri + ": " + e.getMessage()));
                            } catch (AuthenticationException e) {
                                future.completeExceptionally(e);
                            }
                            return future;
                        });
    }

    JwksCache(ServiceConfiguration config, AsyncHttpClient httpClient) {
        int maxSize = getConfigValueAsInt(config, CACHE_SIZE, CACHE_SIZE_DEFAULT);
        int expireAfterSeconds = getConfigValueAsInt(config, CACHE_EXPIRATION_SECONDS,
                CACHE_EXPIRATION_SECONDS_DEFAULT);
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterSeconds, TimeUnit.SECONDS)
                .buildAsync(getLoader(httpClient));
    }

    CompletableFuture<Jwk> getJwk(String jwksUri, String keyId) {
        if (jwksUri == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("jwksUri must not be null."));
        }
        return cache.get(jwksUri).thenApply(jwks -> {
            for (Jwk jwk : jwks) {
                if (jwk.getId().equals(keyId)) {
                    return jwk;
                }
            }
            throw new IllegalArgumentException("No JWK found for jwks uri " + jwksUri + " and Key ID " + keyId);
        });
    }

    /**
     * The JWK Set is stored in the "keys" key see https://www.rfc-editor.org/rfc/rfc7517#section-5.1.
     *
     * @param jwksUri - the URI used to retreive the JWKS
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
            throw new AuthenticationException("Keys value at JWKS URI not a list: " + jwksUri);
        }
    }
}
