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
package org.apache.pulsar.client.admin.internal.http;

import com.google.common.annotations.VisibleForTesting;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;
import org.apache.pulsar.client.impl.PulsarClientSharedResourcesImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.glassfish.jersey.client.spi.Connector;
import org.glassfish.jersey.client.spi.ConnectorProvider;

/**
 * Admin specific Jersey client connector provider.
 */
public class AsyncHttpConnectorProvider implements ConnectorProvider {

    private final ClientConfigurationData conf;
    private AsyncHttpConnector connector;
    private final int autoCertRefreshTimeSeconds;
    private final boolean acceptGzipCompression;
    private boolean followRedirects = true;

    public AsyncHttpConnectorProvider(ClientConfigurationData conf, int autoCertRefreshTimeSeconds,
                                      boolean acceptGzipCompression) {
        this.conf = conf;
        this.autoCertRefreshTimeSeconds = autoCertRefreshTimeSeconds;
        this.acceptGzipCompression = acceptGzipCompression;
    }

    @Override
    public Connector getConnector(Client client, Configuration runtimeConfig) {
        if (connector == null) {
            connector = new AsyncHttpConnector(conf, autoCertRefreshTimeSeconds, acceptGzipCompression);
            connector.setFollowRedirects(followRedirects);
        }
        return connector;
    }


    public AsyncHttpConnector getConnector(int connectTimeoutMs, int readTimeoutMs, int requestTimeoutMs,
            int autoCertRefreshTimeSeconds, PulsarClientSharedResourcesImpl sharedResources) {
        return new AsyncHttpConnector(connectTimeoutMs, readTimeoutMs, requestTimeoutMs, autoCertRefreshTimeSeconds,
                conf, acceptGzipCompression, sharedResources);
    }

    @VisibleForTesting
    public AsyncHttpConnector getAsyncHttpConnector() {
        return connector;
    }

    public void setFollowRedirects(boolean followRedirects) {
        this.followRedirects = followRedirects;
        if (connector != null) {
            connector.setFollowRedirects(followRedirects);
        }
    }
}
