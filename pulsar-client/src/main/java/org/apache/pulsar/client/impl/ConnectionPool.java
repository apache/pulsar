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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.util.netty.ChannelFutures.toCompletableFuture;
import static org.apache.pulsar.client.util.MathUtils.signSafeMod;

import com.google.common.annotations.VisibleForTesting;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.Future;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPool implements Closeable {
    protected final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;

    private final Bootstrap bootstrap;
    private final PulsarChannelInitializer channelInitializerHandler;
    private final ClientConfigurationData clientConfig;
    private final EventLoopGroup eventLoopGroup;
    private final int maxConnectionsPerHosts;
    private final boolean isSniProxy;

    protected final DnsNameResolver dnsResolver;

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, () -> new ClientCnx(conf, eventLoopGroup));
    }

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
            Supplier<ClientCnx> clientCnxSupplier) throws PulsarClientException {
        this.eventLoopGroup = eventLoopGroup;
        this.clientConfig = conf;
        this.maxConnectionsPerHosts = conf.getConnectionsPerBroker();
        this.isSniProxy = clientConfig.isUseTls() && clientConfig.getProxyProtocol() != null
                && StringUtils.isNotBlank(clientConfig.getProxyServiceUrl());

        pool = new ConcurrentHashMap<>();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup));

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getConnectionTimeoutMs());
        bootstrap.option(ChannelOption.TCP_NODELAY, conf.isUseTcpNoDelay());
        bootstrap.option(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);

        try {
            channelInitializerHandler = new PulsarChannelInitializer(conf, clientCnxSupplier);
            bootstrap.handler(channelInitializerHandler);
        } catch (Exception e) {
            log.error("Failed to create channel initializer");
            throw new PulsarClientException(e);
        }

        this.dnsResolver = new DnsNameResolverBuilder(eventLoopGroup.next()).traceEnabled(true)
                .channelType(EventLoopUtil.getDatagramChannelClass(eventLoopGroup)).build();
    }

    private static final Random random = new Random();

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress address) {
        return getConnection(address, address);
    }

    void closeAllConnections() {
        pool.values().forEach(map -> map.values().forEach(future -> {
            if (future.isDone()) {
                if (!future.isCompletedExceptionally()) {
                    // Connection was already created successfully, the join will not throw any exception
                    future.join().close();
                } else {
                    // If the future already failed, there's nothing we have to do
                }
            } else {
                // The future is still pending: just register to make sure it gets closed if the operation will
                // succeed
                future.thenAccept(ClientCnx::close);
            }
        }));
    }

    /**
     * Get a connection from the pool.
     * <p>
     * The connection can either be created or be coming from the pool itself.
     * <p>
     * When specifying multiple addresses, the logicalAddress is used as a tag for the broker, while the physicalAddress
     * is where the connection is actually happening.
     * <p>
     * These two addresses can be different when the client is forced to connect through a proxy layer. Essentially, the
     * pool is using the logical address as a way to decide whether to reuse a particular connection.
     *
     * @param logicalAddress
     *            the address to use as the broker tag
     * @param physicalAddress
     *            the real address where the TCP connection should be made
     * @return a future that will produce the ClientCnx object
     */
    public CompletableFuture<ClientCnx> getConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress) {
        if (maxConnectionsPerHosts == 0) {
            // Disable pooling
            return createConnection(logicalAddress, physicalAddress, -1);
        }

        final int randomKey = signSafeMod(random.nextInt(), maxConnectionsPerHosts);

        return pool.computeIfAbsent(logicalAddress, a -> new ConcurrentHashMap<>()) //
                .computeIfAbsent(randomKey, k -> createConnection(logicalAddress, physicalAddress, randomKey));
    }

    private CompletableFuture<ClientCnx> createConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress, int connectionKey) {
        if (log.isDebugEnabled()) {
            log.debug("Connection for {} not found in cache", logicalAddress);
        }

        final CompletableFuture<ClientCnx> cnxFuture = new CompletableFuture<>();

        // Trigger async connect to broker
        createConnection(physicalAddress).thenAccept(channel -> {
            log.info("[{}] Connected to server", channel);

            channel.closeFuture().addListener(v -> {
                // Remove connection from pool when it gets closed
                if (log.isDebugEnabled()) {
                    log.debug("Removing closed connection from pool: {}", v);
                }
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
            });

            // We are connected to broker, but need to wait until the connect/connected handshake is
            // complete
            final ClientCnx cnx = (ClientCnx) channel.pipeline().get("handler");
            if (!channel.isActive() || cnx == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection was already closed by the time we got notified", channel);
                }
                cnxFuture.completeExceptionally(new ChannelException("Connection already closed"));
                return;
            }

            if (!logicalAddress.equals(physicalAddress)) {
                // We are connecting through a proxy. We need to set the target broker in the ClientCnx object so that
                // it can be specified when sending the CommandConnect.
                // That phase will happen in the ClientCnx.connectionActive() which will be invoked immediately after
                // this method.
                cnx.setTargetBroker(logicalAddress);
            }

            cnx.setRemoteHostName(physicalAddress.getHostName());

            cnx.connectionFuture().thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection handshake completed", cnx.channel());
                }
                cnxFuture.complete(cnx);
            }).exceptionally(exception -> {
                log.warn("[{}] Connection handshake failed: {}", cnx.channel(), exception.getMessage());
                cnxFuture.completeExceptionally(exception);
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
                cnx.ctx().close();
                return null;
            });
        }).exceptionally(exception -> {
            eventLoopGroup.execute(() -> {
                log.warn("Failed to open connection to {} : {}", physicalAddress, exception.getMessage());
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
                cnxFuture.completeExceptionally(new PulsarClientException(exception));
            });
            return null;
        });

        return cnxFuture;
    }

    /**
     * Resolve DNS asynchronously and attempt to connect to any IP address returned by DNS server
     */
    private CompletableFuture<Channel> createConnection(InetSocketAddress unresolvedAddress) {
        int port;
        CompletableFuture<List<InetAddress>> resolvedAddress;
        try {
            if (isSniProxy) {
                URI proxyURI = new URI(clientConfig.getProxyServiceUrl());
                port = proxyURI.getPort();
                resolvedAddress = resolveName(proxyURI.getHost());
            } else {
                port = unresolvedAddress.getPort();
                resolvedAddress = resolveName(unresolvedAddress.getHostString());
            }
            return resolvedAddress.thenCompose(
                    inetAddresses -> connectToResolvedAddresses(inetAddresses.iterator(), port,
                            isSniProxy ? unresolvedAddress : null));
        } catch (URISyntaxException e) {
            log.error("Invalid Proxy url {}", clientConfig.getProxyServiceUrl(), e);
            return FutureUtil
                    .failedFuture(new InvalidServiceURL("Invalid url " + clientConfig.getProxyServiceUrl(), e));
        }
    }

    /**
     * Try to connect to a sequence of IP addresses until a successfull connection can be made, or fail if no address is
     * working
     */
    private CompletableFuture<Channel> connectToResolvedAddresses(Iterator<InetAddress> unresolvedAddresses, int port, InetSocketAddress sniHost) {
        CompletableFuture<Channel> future = new CompletableFuture<>();

        // Successfully connected to server
        connectToAddress(unresolvedAddresses.next(), port, sniHost).thenAccept(future::complete).exceptionally(exception -> {
            if (unresolvedAddresses.hasNext()) {
                // Try next IP address
                connectToResolvedAddresses(unresolvedAddresses, port, sniHost).thenAccept(future::complete).exceptionally(ex -> {
                    // This is already unwinding the recursive call
                    future.completeExceptionally(ex);
                    return null;
                });
            } else {
                // Failed to connect to any IP address
                future.completeExceptionally(exception);
            }
            return null;
        });

        return future;
    }

    CompletableFuture<List<InetAddress>> resolveName(String hostname) {
        CompletableFuture<List<InetAddress>> future = new CompletableFuture<>();
        dnsResolver.resolveAll(hostname).addListener((Future<List<InetAddress>> resolveFuture) -> {
            if (resolveFuture.isSuccess()) {
                future.complete(resolveFuture.get());
            } else {
                future.completeExceptionally(resolveFuture.cause());
            }
        });
        return future;
    }

    /**
     * Attempt to establish a TCP connection to an already resolved single IP address
     */
    private CompletableFuture<Channel> connectToAddress(InetAddress ipAddress, int port, InetSocketAddress sniHost) {
        InetSocketAddress remoteAddress = new InetSocketAddress(ipAddress, port);
        if (clientConfig.isUseTls()) {
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channel -> channelInitializerHandler
                            .initTls(channel, sniHost != null ? sniHost : remoteAddress))
                    .thenCompose(channel -> channelInitializerHandler
                            .initSocks5IfConfig(channel))
                    .thenCompose(channel -> toCompletableFuture(channel.connect(remoteAddress)));
        } else {
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channel -> channelInitializerHandler.initSocks5IfConfig(channel))
                    .thenCompose(channel -> toCompletableFuture(channel.connect(remoteAddress)));
        }
    }

    public void releaseConnection(ClientCnx cnx) {
        if (maxConnectionsPerHosts == 0) {
            //Disable pooling
            if (cnx.channel().isActive()) {
                if(log.isDebugEnabled()) {
                    log.debug("close connection due to pooling disabled.");
                }
                cnx.close();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!eventLoopGroup.isShutdown()) {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).await();
            }
        } catch (InterruptedException e) {
            log.warn("EventLoopGroup shutdown was interrupted", e);
        }
        dnsResolver.close();
    }

    private void cleanupConnection(InetSocketAddress address, int connectionKey,
            CompletableFuture<ClientCnx> connectionFuture) {
        ConcurrentMap<Integer, CompletableFuture<ClientCnx>> map = pool.get(address);
        if (map != null) {
            map.remove(connectionKey, connectionFuture);
        }
    }

    @VisibleForTesting
    int getPoolSize() {
        return pool.values().stream().mapToInt(Map::size).sum();
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

}

