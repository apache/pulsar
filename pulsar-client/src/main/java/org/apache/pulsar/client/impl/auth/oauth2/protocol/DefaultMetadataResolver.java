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
package org.apache.pulsar.client.impl.auth.oauth2.protocol;

import com.fasterxml.jackson.databind.ObjectReader;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.resolver.NameResolver;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

/**
 * Resolves OAuth 2.0 authorization server metadata as described in RFC 8414.
 */
public class DefaultMetadataResolver implements MetadataResolver {

    private final URL metadataUrl;
    private final ObjectReader objectReader;
    private final AsyncHttpClient httpClient;
    private final NameResolver<InetAddress> nameResolver;

    public DefaultMetadataResolver(URL metadataUrl, AsyncHttpClient httpClient,
                                   NameResolver<InetAddress> nameResolver) {
        this.metadataUrl = metadataUrl;
        this.objectReader = ObjectMapperFactory.getMapper().reader().forType(Metadata.class);
        this.httpClient = httpClient;
        this.nameResolver = nameResolver;
    }

    /**
     * Gets a well-known metadata URL for the given OAuth issuer URL.
     *
     * @param issuerUrl The authorization server's issuer identifier
     * @return a resolver
     */
    public static DefaultMetadataResolver fromIssuerUrl(URL issuerUrl,
                                                        AsyncHttpClient httpClient,
                                                        NameResolver<InetAddress> nameResolver) {
        return new DefaultMetadataResolver(getWellKnownMetadataUrl(issuerUrl), httpClient, nameResolver);
    }

    /**
     * Gets a well-known metadata URL for the given OAuth issuer URL.
     *
     * @param issuerUrl The authorization server's issuer identifier
     * @return a URL
     * @see <a href="https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig">
     * OAuth Discovery: Obtaining Authorization Server Metadata</a>
     */
    public static URL getWellKnownMetadataUrl(URL issuerUrl) {
        try {
            return URI.create(issuerUrl.toExternalForm() + "/.well-known/openid-configuration").normalize().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Resolves the authorization metadata.
     *
     * @return metadata
     * @throws IOException if the metadata could not be resolved.
     */
    public Metadata resolve() throws IOException {

        try {
            BoundRequestBuilder requestBuilder = httpClient.prepareGet(metadataUrl.toString())
                    .addHeader(HttpHeaderNames.ACCEPT, HttpHeaderValues.APPLICATION_JSON);
            if (nameResolver != null) {
                requestBuilder.setNameResolver(nameResolver);
            }
            Response response = requestBuilder.execute().toCompletableFuture().get();

            Metadata metadata;
            try (InputStream inputStream = response.getResponseBodyAsStream()) {
                metadata = this.objectReader.readValue(inputStream);
            }
            return metadata;

        } catch (IOException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("Cannot obtain authorization metadata from " + metadataUrl, e);
        }
    }
}
