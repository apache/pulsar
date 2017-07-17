/**
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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationManager;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher.ShutdownService;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Pulsar proxy service
 */
public class ProxyService implements Closeable {

    private final ProxyConfiguration proxyConfig;
    private final String serviceUrl;
    private final String serviceUrlTls;
    private ConfigurationCacheService configurationCacheService;
    private AuthenticationService authenticationService;
    private AuthorizationManager authorizationManager;
    private ZooKeeperClientFactory zkClientFactory = null;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");

    // ConnectionPool is used by the proxy to issue lookup requests
    private final PulsarClientImpl client;

    private final Authentication clientAuthentication;

    private BrokerDiscoveryProvider discoveryProvider;

    private LocalZooKeeperConnectionService localZooKeeperConnectionService;

    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    public ProxyService(ProxyConfiguration proxyConfig) throws IOException {
        checkNotNull(proxyConfig);
        this.proxyConfig = proxyConfig;

        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        this.serviceUrl = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePort());
        this.serviceUrlTls = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePortTls());

        this.acceptorGroup  = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        if (proxyConfig.getBrokerClientAuthenticationPlugin() != null) {
            clientConfiguration.setAuthentication(proxyConfig.getBrokerClientAuthenticationPlugin(),
                    proxyConfig.getBrokerClientAuthenticationParameters());
        }

        this.client = new PulsarClientImpl(serviceUrl, clientConfiguration, workerGroup);
        this.clientAuthentication = clientConfiguration.getAuthentication();
    }

    public void start() throws Exception {
        localZooKeeperConnectionService = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                proxyConfig.getZookeeperServers(), proxyConfig.getZookeeperSessionTimeoutMs());
        localZooKeeperConnectionService.start(new ShutdownService() {
            @Override
            public void shutdown(int exitCode) {
                LOG.error("Lost local ZK session. Shutting down the proxy");
                Runtime.getRuntime().halt(-1);
            }
        });

        discoveryProvider = new BrokerDiscoveryProvider(this.proxyConfig, getZooKeeperClientFactory());
        this.configurationCacheService = new ConfigurationCacheService(discoveryProvider.globalZkCache);
        ServiceConfiguration serviceConfiguration = createServiceConfiguration(proxyConfig);
        authenticationService = new AuthenticationService(serviceConfiguration);
        authorizationManager = new AuthorizationManager(serviceConfiguration, configurationCacheService);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, false));
        // Bind and start to accept incoming connections.
        bootstrap.bind(proxyConfig.getServicePort()).sync();
        LOG.info("Started Pulsar Proxy at {}", serviceUrl);

        if (proxyConfig.isTlsEnabledInProxy()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, true));
            tlsBootstrap.bind(proxyConfig.getServicePortTls()).sync();
            LOG.info("Started Pulsar TLS Proxy on port {}", proxyConfig.getWebServicePortTls());
        }
    }

    long newRequestId() {
        return client.newRequestId();
    }

    ConnectionPool getConnectionPool() {
        return client.getCnxPool();
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    public BrokerDiscoveryProvider getDiscoveryProvider() {
        return discoveryProvider;
    }

    public void close() throws IOException {
        if (localZooKeeperConnectionService != null) {
            localZooKeeperConnectionService.close();
        }
        if (discoveryProvider != null) {
            discoveryProvider.close();
        }
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        client.close();
    }

    private ServiceConfiguration createServiceConfiguration(ProxyConfiguration config) {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setAuthenticationEnabled(config.isAuthenticationEnabled());
        serviceConfiguration.setAuthorizationEnabled(config.isAuthorizationEnabled());
        serviceConfiguration.setAuthenticationProviders(config.getAuthenticationProviders());
        serviceConfiguration.setProperties(config.getProperties());
        return serviceConfiguration;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public ProxyConfiguration getConfiguration() {
        return proxyConfig;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }

    public Authentication getClientAuthentication() {
        return clientAuthentication;
    }

    public ConfigurationCacheService getConfigurationCacheService() {
        return configurationCacheService;
    }

    public void setConfigurationCacheService(ConfigurationCacheService configurationCacheService) {
        this.configurationCacheService = configurationCacheService;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProxyService.class);
}