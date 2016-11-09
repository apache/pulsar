/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service;

import java.net.InetAddress;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.discovery.service.server.ServiceConfig;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Main discovery-service which starts component to serve incoming discovery-request over binary-proto channel and
 * redirects to one of the active broker
 *
 */
public class DiscoveryService {

    private final ServiceConfig config;
    private final String serviceUrl;
    private final String serviceUrlTls;
    private ZooKeeperClientFactory zkClientFactory = null;
    private BrokerDiscoveryProvider discoveryProvider;
    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;
    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");
    private final int numThreads = Runtime.getRuntime().availableProcessors();

    public DiscoveryService(ServiceConfig serviceConfig) {

        this.config = serviceConfig;
        this.serviceUrl = serviceUrl();
        this.serviceUrlTls = serviceUrlTls();
        discoveryProvider = new BrokerDiscoveryProvider(serviceConfig);
        EventLoopGroup acceptorEventLoop, workersEventLoop;
        if (SystemUtils.IS_OS_LINUX) {
            try {
                acceptorEventLoop = new EpollEventLoopGroup(1, acceptorThreadFactory);
                workersEventLoop = new EpollEventLoopGroup(numThreads, workersThreadFactory);
            } catch (UnsatisfiedLinkError e) {
                acceptorEventLoop = new NioEventLoopGroup(1, acceptorThreadFactory);
                workersEventLoop = new NioEventLoopGroup(numThreads, workersThreadFactory);
            }
        } else {
            acceptorEventLoop = new NioEventLoopGroup(1, acceptorThreadFactory);
            workersEventLoop = new NioEventLoopGroup(numThreads, workersThreadFactory);
        }
        this.acceptorGroup = acceptorEventLoop;
        this.workerGroup = workersEventLoop;
    }

    /**
     * Starts discovery service by initializing zookkeeper and server
     * @throws Exception
     */
    public void start() throws Exception {
        discoveryProvider.start(getZooKeeperClientFactory());
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

        if (workerGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollServerSocketChannel.class);
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        } else {
            bootstrap.channel(NioServerSocketChannel.class);
        }

        bootstrap.childHandler(new ServiceChannelInitializer(this, config, false));
        // Bind and start to accept incoming connections.
        bootstrap.bind(config.getServicePort()).sync();
        LOG.info("Started Pulsar Broker service on port {}", config.getWebServicePort());

        if (config.isTlsEnabled()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, config, true));
            tlsBootstrap.bind(config.getServicePortTls()).sync();
            LOG.info("Started Pulsar Broker TLS service on port {}", config.getWebServicePortTls());
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

    public void close() {
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
            return new StringBuilder("pulsar://").append(host()).append(":").append(config.getServicePortTls())
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

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);
}