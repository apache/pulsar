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
package org.apache.pulsar.client.impl.auth.oauth2;

import io.netty.resolver.NameResolver;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;
import org.asynchttpclient.AsyncHttpClient;

/**
 * An abstract OAuth 2.0 authorization flow.
 */
@Slf4j
abstract class FlowBase implements Flow {

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;
    protected final AsyncHttpClient httpClient;

    protected transient Metadata metadata;

    protected NameResolver<InetAddress> nameResolver;

    protected FlowBase(URL issuerUrl, AsyncHttpClient httpClient, NameResolver<InetAddress> nameResolver) {
        this.issuerUrl = issuerUrl;
        this.httpClient = httpClient;
        this.nameResolver = nameResolver;
    }


    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error("Unable to retrieve OAuth 2.0 server metadata", e);
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    protected MetadataResolver createMetadataResolver() {
        return DefaultMetadataResolver.fromIssuerUrl(issuerUrl, httpClient, nameResolver);
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
