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
import io.netty.util.concurrent.DefaultThreadFactory;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Configuration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.impl.PulsarClientSharedResourcesImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.tls.ClientTlsFactorySupport;
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
    // PIP-478: one TLS factory per PulsarAdmin. A PulsarAdmin ends up with two connectors — the one Jersey
    // creates lazily on the first request, and the one PulsarAdminImpl builds directly — and each used to
    // resolve its own. For a by-name custom factory that meant constructing and initializing it twice, against
    // the SPI's "initialize is called once"; for a factory adopted from the broker's admin attach it meant
    // closing the same instance twice. The provider is the natural owner: it is per-admin and creates both
    // connectors. Resolved lazily, on the first connector that needs TLS, so a plaintext admin with nothing to
    // serve still allocates nothing.
    private TlsFactoryOwnership sharedTlsFactory;

    public AsyncHttpConnectorProvider(ClientConfigurationData conf, int autoCertRefreshTimeSeconds,
                                      boolean acceptGzipCompression) {
        this.conf = conf;
        this.autoCertRefreshTimeSeconds = autoCertRefreshTimeSeconds;
        this.acceptGzipCompression = acceptGzipCompression;
    }

    @Override
    public Connector getConnector(Client client, Configuration runtimeConfig) {
        if (connector == null) {
            connector = new AsyncHttpConnector(client, conf, autoCertRefreshTimeSeconds, acceptGzipCompression,
                    sharedTlsFactory());
            connector.setFollowRedirects(followRedirects);
        }
        return connector;
    }


    public AsyncHttpConnector getConnector(int connectTimeoutMs, int readTimeoutMs, int requestTimeoutMs,
            int autoCertRefreshTimeSeconds, PulsarClientSharedResourcesImpl sharedResources) {
        return new AsyncHttpConnector(connectTimeoutMs, readTimeoutMs, requestTimeoutMs, autoCertRefreshTimeSeconds,
                conf, acceptGzipCompression, sharedResources, sharedTlsFactory());
    }

    /**
     * The single {@link PulsarTlsFactory} shared by every connector this provider creates, or {@code null}
     * when this admin configuration needs none. Resolved at most once; the connectors take their own
     * subscriptions from it but never close it.
     *
     * @return the shared TLS factory, or {@code null} when none is needed
     */
    @VisibleForTesting
    synchronized TlsFactoryOwnership sharedTlsFactory() {
        if (sharedTlsFactory != null) {
            // Already resolved. The connectors borrow it: this provider stays the owner.
            return TlsFactoryOwnership.borrowing(sharedTlsFactory.factory());
        }
        if (!AsyncHttpConnector.needsTlsFactory(conf)) {
            sharedTlsFactory = TlsFactoryOwnership.none();
            return sharedTlsFactory;
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-admin-tls-factory"));
        try {
            sharedTlsFactory = TlsFactoryOwnership.owning(
                    ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor,
                            conf.getOpenTelemetry()),
                    executor);
        } catch (Exception e) {
            executor.shutdownNow();
            throw new RuntimeException("Failed to resolve the admin client TLS factory", e);
        }
        return TlsFactoryOwnership.borrowing(sharedTlsFactory.factory());
    }

    /**
     * Release the shared TLS factory and the executor driving its rotation. Called when the owning
     * {@code PulsarAdmin} closes; the connectors borrowed the factory and dispose only their own
     * subscriptions.
     */
    public synchronized void close() {
        if (sharedTlsFactory == null) {
            return;
        }
        sharedTlsFactory.close();
        sharedTlsFactory = TlsFactoryOwnership.none();
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
