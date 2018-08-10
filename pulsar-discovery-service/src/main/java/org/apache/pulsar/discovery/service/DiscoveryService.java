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
package org.apache.pulsar.discovery.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.discovery.service.server.ServiceConfig;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
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
 * Main discovery-service which starts component to serve incoming discovery-request over binary-proto channel and
 * redirects to one of the active broker
 *
 */
public class DiscoveryService implements Closeable {

    private final ServiceConfig config;
    private final String serviceUrl;
    private final String serviceUrlTls;
    private ConfigurationCacheService configurationCacheService;
    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ZooKeeperClientFactory zkClientFactory = null;
    private BrokerDiscoveryProvider discoveryProvider;
    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");
    private final int numThreads = Runtime.getRuntime().availableProcessors();

    public DiscoveryService(ServiceConfig serviceConfig) {
        checkNotNull(serviceConfig);
        this.config = serviceConfig;
        this.serviceUrl = serviceUrl();
        this.serviceUrlTls = serviceUrlTls();
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
    }

    /**
     * Starts discovery service by initializing zookkeeper and server
     * @throws Exception
     */
    public void start() throws Exception {
        discoveryProvider = new BrokerDiscoveryProvider(this.config, getZooKeeperClientFactory());
        this.configurationCacheService = new ConfigurationCacheService(discoveryProvider.globalZkCache);
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(config);
        authenticationService = new AuthenticationService(serviceConfiguration);
        authorizationService = new AuthorizationService(serviceConfiguration, configurationCacheService);
        startServer();
    }

    /**
     * starts server to handle discovery-request from client-channel
     *
     * @throws Exception
     */
    public void startServer() throws Exception {

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));
        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new ServiceChannelInitializer(this, config, false));
        // Bind and start to accept incoming connections.
        bootstrap.bind(config.getServicePort()).sync();
        LOG.info("Started Pulsar Discovery service on port {}", config.getServicePort());

        if (config.isTlsEnabled()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, config, true));
            tlsBootstrap.bind(config.getServicePortTls()).sync();
            LOG.info("Started Pulsar Discovery TLS service on port {}", config.getServicePortTls());
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
        discoveryProvider.close();
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /**
     * Derive the host
     *
     * @param isBindOnLocalhost
     * @return
     */
    public String host() {
        try {
            if (!config.isBindOnLocalhost()) {
                return InetAddress.getLocalHost().getHostName();
            } else {
                return "localhost";
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException("failed to find host", e);
        }
    }

    public String serviceUrl() {
        return new StringBuilder("pulsar://").append(host()).append(":").append(config.getServicePort()).toString();
    }

    public String serviceUrlTls() {
        if (config.isTlsEnabled()) {
            return new StringBuilder("pulsar+ssl://").append(host()).append(":").append(config.getServicePortTls())
                    .toString();
        } else {
            return "";
        }
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public ServiceConfig getConfiguration() {
        return config;
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

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);
}