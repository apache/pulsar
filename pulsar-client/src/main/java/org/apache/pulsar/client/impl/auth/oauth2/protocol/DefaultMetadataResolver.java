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
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Resolves OAuth 2.0 authorization server metadata as described in RFC 8414.
 */
public class DefaultMetadataResolver implements MetadataResolver {

    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected static final int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    private final URL metadataUrl;
    private final ObjectReader objectReader;
    private Duration connectTimeout;
    private Duration readTimeout;

    public DefaultMetadataResolver(URL metadataUrl) {
        this.metadataUrl = metadataUrl;
        this.objectReader = ObjectMapperFactory.getMapper().reader().forType(Metadata.class);
        // set a default timeout to ensure that this doesn't block
        this.connectTimeout = Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS);
        this.readTimeout = Duration.ofSeconds(DEFAULT_READ_TIMEOUT_IN_SECONDS);
    }

    public DefaultMetadataResolver withConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public DefaultMetadataResolver withReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    /**
     * Resolves the authorization metadata.
     * @return metadata
     * @throws IOException if the metadata could not be resolved.
     */
    public Metadata resolve() throws IOException {
        try {
            URLConnection c = this.metadataUrl.openConnection();
            if (connectTimeout != null) {
                c.setConnectTimeout((int) connectTimeout.toMillis());
            }
            if (readTimeout != null) {
                c.setReadTimeout((int) readTimeout.toMillis());
            }
            c.setRequestProperty("Accept", "application/json");

            Metadata metadata;
            try (InputStream inputStream = c.getInputStream()) {
                metadata = this.objectReader.readValue(inputStream);
            }
            return metadata;

        } catch (IOException e) {
            throw new IOException("Cannot obtain authorization metadata from " + metadataUrl.toString(), e);
        }
    }

    /**
     * Gets a well-known metadata URL for the given OAuth issuer URL.
     * @param issuerUrl The authorization server's issuer identifier
     * @return a resolver
     */
    public static DefaultMetadataResolver fromIssuerUrl(URL issuerUrl) {
        return new DefaultMetadataResolver(getWellKnownMetadataUrl(issuerUrl));
    }

    /**
     * Gets a well-known metadata URL for the given OAuth issuer URL.
     * @see <a href="https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig">
     *     OAuth Discovery: Obtaining Authorization Server Metadata</a>
     * @param issuerUrl The authorization server's issuer identifier
     * @return a URL
     */
    public static URL getWellKnownMetadataUrl(URL issuerUrl) {
        try {
            return URI.create(issuerUrl.toExternalForm() + "/.well-known/openid-configuration").normalize().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
