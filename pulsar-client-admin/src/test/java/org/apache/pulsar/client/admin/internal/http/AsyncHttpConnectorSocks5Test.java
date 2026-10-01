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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.net.InetSocketAddress;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * Unit tests for the SOCKS5 proxy wiring added in {@link AsyncHttpConnector}.
 */
public class AsyncHttpConnectorSocks5Test {

    /**
     * When the configuration is {@code null}, the proxy-configuration helper must be a no-op.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededWithNullConf() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, null);

        verify(builder, never()).setProxyServer(any(ProxyServer.class));
        verify(builder, never()).setProxyServer(any(ProxyServer.Builder.class));
    }

    /**
     * When {@code socks5ProxyAddress} is not set, no proxy should be configured on the builder.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededWithoutAddress() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());
        ClientConfigurationData conf = newAdminConf();

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, conf);

        verify(builder, never()).setProxyServer(any(ProxyServer.class));
        verify(builder, never()).setProxyServer(any(ProxyServer.Builder.class));
    }

    /**
     * When only the SOCKS5 address is provided, a {@link ProxyType#SOCKS_V5} proxy server
     * should be configured without any authentication realm.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededWithAddressOnly() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());
        ClientConfigurationData conf = newAdminConf();
        InetSocketAddress socks5Address = InetSocketAddress.createUnresolved("127.0.0.1", 1080);
        conf.setSocks5ProxyAddress(socks5Address);

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, conf);

        ArgumentCaptor<ProxyServer> captor = ArgumentCaptor.forClass(ProxyServer.class);
        verify(builder).setProxyServer(captor.capture());

        ProxyServer proxyServer = captor.getValue();
        assertNotNull(proxyServer, "ProxyServer must have been configured");
        assertEquals(proxyServer.getProxyType(), ProxyType.SOCKS_V5);
        assertEquals(proxyServer.getHost(), "127.0.0.1");
        assertEquals(proxyServer.getPort(), 1080);
        assertNull(proxyServer.getRealm(), "Realm must not be set when no username is provided");
    }

    /**
     * When both address and credentials are provided, a {@link Realm} with BASIC scheme should be
     * attached to the proxy server so that Netty's Socks5ProxyHandler performs username/password
     * authentication.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededWithCredentials() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());
        ClientConfigurationData conf = newAdminConf();
        InetSocketAddress socks5Address = InetSocketAddress.createUnresolved("proxy.example.com", 2080);
        conf.setSocks5ProxyAddress(socks5Address);
        conf.setSocks5ProxyUsername("user1");
        conf.setSocks5ProxyPassword("p@ssw0rd");

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, conf);

        ArgumentCaptor<ProxyServer> captor = ArgumentCaptor.forClass(ProxyServer.class);
        verify(builder).setProxyServer(captor.capture());

        ProxyServer proxyServer = captor.getValue();
        assertNotNull(proxyServer);
        assertEquals(proxyServer.getProxyType(), ProxyType.SOCKS_V5);
        assertEquals(proxyServer.getHost(), "proxy.example.com");
        assertEquals(proxyServer.getPort(), 2080);

        Realm realm = proxyServer.getRealm();
        assertNotNull(realm, "Realm must be set when a username is provided");
        assertEquals(realm.getPrincipal(), "user1");
        assertEquals(realm.getPassword(), "p@ssw0rd");
        assertEquals(realm.getScheme(), Realm.AuthScheme.BASIC);
    }

    /**
     * When the configured {@link Socks5ProxyScope} does not cover HTTP traffic (e.g.
     * {@link Socks5ProxyScope#BINARY_ONLY}), the helper must skip wiring the proxy on the
     * async-http-client builder even if a SOCKS5 address is provided.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededSkippedWhenScopeBinaryOnly() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setSocks5ProxyAddress(InetSocketAddress.createUnresolved("127.0.0.1", 1080));
        // explicitly force BINARY_ONLY to verify the HTTP-scope guard
        conf.setSocks5ProxyScope(Socks5ProxyScope.BINARY_ONLY);

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, conf);

        verify(builder, never()).setProxyServer(any(ProxyServer.class));
        verify(builder, never()).setProxyServer(any(ProxyServer.Builder.class));
    }

    /**
     * A blank username must be treated the same as no credentials: no realm should be attached.
     */
    @Test
    public void testConfigureSocks5ProxyIfNeededWithBlankUsername() throws Exception {
        DefaultAsyncHttpClientConfig.Builder builder = spy(new DefaultAsyncHttpClientConfig.Builder());
        ClientConfigurationData conf = newAdminConf();
        conf.setSocks5ProxyAddress(InetSocketAddress.createUnresolved("127.0.0.1", 1080));
        conf.setSocks5ProxyUsername("   ");
        conf.setSocks5ProxyPassword("ignored");

        AsyncHttpConnector.configureSocks5ProxyIfNeeded(builder, conf);

        ArgumentCaptor<ProxyServer> captor = ArgumentCaptor.forClass(ProxyServer.class);
        verify(builder).setProxyServer(captor.capture());

        ProxyServer proxyServer = captor.getValue();
        assertNotNull(proxyServer);
        assertNull(proxyServer.getRealm(), "Realm must not be set when the username is blank");
    }

    /**
     * End-to-end smoke test: building a real {@link AsyncHttpConnector} with SOCKS5 configured
     * should not throw and the connector should expose a non-null http client that can be closed
     * cleanly. The SOCKS5 handler is only wired into the Netty pipeline when an actual connection
     * is attempted, so no real SOCKS5 server is required here.
     */
    @Test
    public void testAsyncHttpConnectorConstructionWithSocks5() throws Exception {
        ClientConfigurationData conf = newAdminConf();
        conf.setServiceUrl("http://localhost:8080");
        conf.setAuthentication(new AuthenticationDisabled());
        conf.setSocks5ProxyAddress(InetSocketAddress.createUnresolved("127.0.0.1", 1080));
        conf.setSocks5ProxyUsername("admin");
        conf.setSocks5ProxyPassword("admin");

        AsyncHttpConnector connector = new AsyncHttpConnector(1000, 1000, 1000, 60, conf, false, null);
        try {
            assertNotNull(connector.getHttpClient(), "AsyncHttpClient must be initialized");
        } finally {
            connector.close();
        }
    }

    /**
     * Build a {@link ClientConfigurationData} that matches what {@code PulsarAdminBuilderImpl}
     * produces for admin clients. Admin traffic is HTTP-only, so the default SOCKS5 scope is
     * {@link Socks5ProxyScope#HTTP_ONLY}.
     */
    private static ClientConfigurationData newAdminConf() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);
        return conf;
    }
}
