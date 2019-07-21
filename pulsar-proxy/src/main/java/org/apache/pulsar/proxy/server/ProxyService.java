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
import static org.apache.commons.lang3.StringUtils.isBlank;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar proxy service
 */
public class ProxyService implements Closeable {

    private final ProxyConfiguration proxyConfig;
    private final String serviceUrl;
    private final String serviceUrlTls;
    private ConfigurationCacheService configurationCacheService;
    private final AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ZooKeeperClientFactory zkClientFactory = null;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;

    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");

    private BrokerDiscoveryProvider discoveryProvider;

    protected final AtomicReference<Semaphore> lookupRequestSemaphore;

    protected static int proxyLogLevel;

    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    static final Gauge activeConnections = Gauge
            .build("pulsar_proxy_active_connections", "Number of connections currently active in the proxy").create()
            .register();

    static final Counter newConnections = Counter
            .build("pulsar_proxy_new_connections", "Counter of connections being opened in the proxy").create()
            .register();

    static final Counter rejectedConnections = Counter
            .build("pulsar_proxy_rejected_connections", "Counter for connections rejected due to throttling").create()
            .register();

    static final Counter opsCounter = Counter
            .build("pulsar_proxy_binary_ops", "Counter of proxy operations").create().register();

    static final Counter bytesCounter = Counter
            .build("pulsar_proxy_binary_bytes", "Counter of proxy bytes").create().register();

    public ProxyService(ProxyConfiguration proxyConfig,
                        AuthenticationService authenticationService) throws IOException {
        checkNotNull(proxyConfig);
        this.proxyConfig = proxyConfig;

        this.lookupRequestSemaphore = new AtomicReference<Semaphore>(
                new Semaphore(proxyConfig.getMaxConcurrentLookupRequests(), false));

        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        if (proxyConfig.getServicePort().isPresent()) {
            this.serviceUrl = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePort().get());
        } else {
            this.serviceUrl = null;
        }

        if (proxyConfig.getServicePortTls().isPresent()) {
            this.serviceUrlTls = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePortTls().get());
        } else {
            this.serviceUrlTls = null;
        }

        if (proxyConfig.getproxyLogLevel().isPresent()) {
            ProxyService.proxyLogLevel = Integer.valueOf(proxyConfig.getproxyLogLevel().get());
        } else {
            ProxyService.proxyLogLevel = 0;
        }
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
        this.authenticationService = authenticationService;
    }

    public void start() throws Exception {
        if (!isBlank(proxyConfig.getZookeeperServers()) && !isBlank(proxyConfig.getConfigurationStoreServers())) {
            discoveryProvider = new BrokerDiscoveryProvider(this.proxyConfig, getZooKeeperClientFactory());
            this.configurationCacheService = new ConfigurationCacheService(discoveryProvider.globalZkCache);
            authorizationService = new AuthorizationService(PulsarConfigurationLoader.convertFrom(proxyConfig),
                                                            configurationCacheService);
        }

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, false));
        // Bind and start to accept incoming connections.
        if (proxyConfig.getServicePort().isPresent()) {
            try {
                bootstrap.bind(proxyConfig.getServicePort().get()).sync();
                LOG.info("Started Pulsar Proxy at {}", serviceUrl);
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getServicePort().get(), e);
            }
        }
        LOG.info("Started Pulsar Proxy at {}", serviceUrl);

        if (proxyConfig.getServicePortTls().isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, true));
            tlsBootstrap.bind(proxyConfig.getServicePortTls().get()).sync();
            LOG.info("Started Pulsar TLS Proxy on port {}", proxyConfig.getServicePortTls().get());
        }
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
        if (discoveryProvider != null) {
            discoveryProvider.close();
        }
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
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

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public ConfigurationCacheService getConfigurationCacheService() {
        return configurationCacheService;
    }

    public void setConfigurationCacheService(ConfigurationCacheService configurationCacheService) {
        this.configurationCacheService = configurationCacheService;
    }

    public Semaphore getLookupRequestSemaphore() {
        return lookupRequestSemaphore.get();
    }

    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProxyService.class);
}
