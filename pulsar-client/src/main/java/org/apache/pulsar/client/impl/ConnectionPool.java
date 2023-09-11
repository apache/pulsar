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

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import static org.apache.pulsar.common.util.netty.ChannelFutures.toCompletableFuture;
import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPool implements AutoCloseable {
    protected final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;

    private final Bootstrap bootstrap;
    private final PulsarChannelInitializer channelInitializerHandler;
    private final ClientConfigurationData clientConfig;
    private final EventLoopGroup eventLoopGroup;
    private final int maxConnectionsPerHosts;
    private final boolean isSniProxy;

    protected final AddressResolver<InetSocketAddress> addressResolver;
    private final boolean shouldCloseDnsResolver;

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, () -> new ClientCnx(conf, eventLoopGroup));
    }

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                          Supplier<ClientCnx> clientCnxSupplier) throws PulsarClientException {
        this(conf, eventLoopGroup, clientCnxSupplier, Optional.empty());
    }

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                          Supplier<ClientCnx> clientCnxSupplier,
                          Optional<AddressResolver<InetSocketAddress>> addressResolver)
            throws PulsarClientException {
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

        this.shouldCloseDnsResolver = !addressResolver.isPresent();
        this.addressResolver = addressResolver.orElseGet(() -> createAddressResolver(conf, eventLoopGroup));
    }

    private static AddressResolver<InetSocketAddress> createAddressResolver(ClientConfigurationData conf,
                                                                            EventLoopGroup eventLoopGroup) {
        DnsNameResolverBuilder dnsNameResolverBuilder = new DnsNameResolverBuilder()
                .traceEnabled(true).channelType(EventLoopUtil.getDatagramChannelClass(eventLoopGroup));
        if (conf.getDnsLookupBindAddress() != null) {
            InetSocketAddress addr = new InetSocketAddress(conf.getDnsLookupBindAddress(),
                    conf.getDnsLookupBindPort());
            dnsNameResolverBuilder.localAddress(addr);
        }
        DnsResolverUtil.applyJdkDnsCacheSettings(dnsNameResolverBuilder);
        // use DnsAddressResolverGroup to create the AddressResolver since it contains a solution
        // to prevent cache stampede / thundering herds problem when a DNS entry expires while the system
        // is under high load
        return new DnsAddressResolverGroup(dnsNameResolverBuilder).getResolver(eventLoopGroup.next());
    }

    private static final Random random = new Random();

    public int genRandomKeyToSelectCon() {
        if (maxConnectionsPerHosts == 0) {
            return -1;
        }
        return signSafeMod(random.nextInt(), maxConnectionsPerHosts);
    }

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress address) {
        if (maxConnectionsPerHosts == 0) {
            return getConnection(address, address, -1);
        }
        return getConnection(address, address, signSafeMod(random.nextInt(), maxConnectionsPerHosts));
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
            InetSocketAddress physicalAddress, final int randomKey) {
        if (maxConnectionsPerHosts == 0) {
            // Disable pooling
            return createConnection(logicalAddress, physicalAddress, -1);
        }

        final ConcurrentMap<Integer, CompletableFuture<ClientCnx>> innerPool =
                pool.computeIfAbsent(logicalAddress, a -> new ConcurrentHashMap<>());
        CompletableFuture<ClientCnx> completableFuture = innerPool
                .computeIfAbsent(randomKey, k -> createConnection(logicalAddress, physicalAddress, randomKey));
        if (completableFuture.isCompletedExceptionally()) {
            // we cannot cache a failed connection, so we remove it from the pool
            // there is a race condition in which
            // cleanupConnection is called before caching this result
            // and so the clean up fails
            cleanupConnection(logicalAddress, randomKey, completableFuture);
        }

        return completableFuture;
    }

    private CompletableFuture<ClientCnx> createConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress, int connectionKey) {
        if (log.isDebugEnabled()) {
            log.debug("Connection for {} not found in cache", logicalAddress);
        }

        final CompletableFuture<ClientCnx> cnxFuture = new CompletableFuture<>();

        // Trigger async connect to broker
        createConnection(logicalAddress, physicalAddress).thenAccept(channel -> {
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

            cnx.connectionFuture().thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection handshake completed", cnx.channel());
                }
                cnxFuture.complete(cnx);
            }).exceptionally(exception -> {
                log.warn("[{}] Connection handshake failed: {}", cnx.channel(), exception.getMessage());
                cnxFuture.completeExceptionally(exception);
                // this cleanupConnection may happen before that the
                // CompletableFuture is cached into the "pool" map,
                // it is not enough to clean it here, we need to clean it
                // in the "pool" map when the CompletableFuture is cached
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
     * Resolve DNS asynchronously and attempt to connect to any IP address returned by DNS server.
     */
    private CompletableFuture<Channel> createConnection(InetSocketAddress logicalAddress,
                                                        InetSocketAddress unresolvedPhysicalAddress) {
        CompletableFuture<List<InetSocketAddress>> resolvedAddress;
        try {
            if (isSniProxy) {
                URI proxyURI = new URI(clientConfig.getProxyServiceUrl());
                resolvedAddress =
                        resolveName(InetSocketAddress.createUnresolved(proxyURI.getHost(), proxyURI.getPort()));
            } else {
                resolvedAddress = resolveName(unresolvedPhysicalAddress);
            }
            return resolvedAddress.thenCompose(
                    inetAddresses -> connectToResolvedAddresses(
                            logicalAddress,
                            unresolvedPhysicalAddress,
                            inetAddresses.iterator(),
                            isSniProxy ? unresolvedPhysicalAddress : null)
            );
        } catch (URISyntaxException e) {
            log.error("Invalid Proxy url {}", clientConfig.getProxyServiceUrl(), e);
            return FutureUtil
                    .failedFuture(new InvalidServiceURL("Invalid url " + clientConfig.getProxyServiceUrl(), e));
        }
    }

    /**
     * Try to connect to a sequence of IP addresses until a successful connection can be made, or fail if no
     * address is working.
     */
    private CompletableFuture<Channel> connectToResolvedAddresses(InetSocketAddress logicalAddress,
                                                                  InetSocketAddress unresolvedPhysicalAddress,
                                                                  Iterator<InetSocketAddress> resolvedPhysicalAddress,
                                                                  InetSocketAddress sniHost) {
        CompletableFuture<Channel> future = new CompletableFuture<>();

        // Successfully connected to server
        connectToAddress(logicalAddress, resolvedPhysicalAddress.next(), unresolvedPhysicalAddress, sniHost)
                .thenAccept(future::complete)
                .exceptionally(exception -> {
                    if (resolvedPhysicalAddress.hasNext()) {
                        // Try next IP address
                        connectToResolvedAddresses(logicalAddress, unresolvedPhysicalAddress,
                                resolvedPhysicalAddress, sniHost)
                                .thenAccept(future::complete)
                                .exceptionally(ex -> {
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

    CompletableFuture<List<InetSocketAddress>> resolveName(InetSocketAddress unresolvedAddress) {
        CompletableFuture<List<InetSocketAddress>> future = new CompletableFuture<>();
        addressResolver.resolveAll(unresolvedAddress).addListener((Future<List<InetSocketAddress>> resolveFuture) -> {
            if (resolveFuture.isSuccess()) {
                future.complete(resolveFuture.get());
            } else {
                future.completeExceptionally(resolveFuture.cause());
            }
        });
        return future;
    }

    /**
     * Attempt to establish a TCP connection to an already resolved single IP address.
     */
    private CompletableFuture<Channel> connectToAddress(InetSocketAddress logicalAddress,
                                                        InetSocketAddress physicalAddress,
                                                        InetSocketAddress unresolvedPhysicalAddress,
                                                        InetSocketAddress sniHost) {
        if (clientConfig.isUseTls()) {
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channel -> channelInitializerHandler
                            .initTls(channel, sniHost != null ? sniHost : physicalAddress))
                    .thenCompose(channelInitializerHandler::initSocks5IfConfig)
                    .thenCompose(ch ->
                            channelInitializerHandler.initializeClientCnx(ch, logicalAddress,
                                    unresolvedPhysicalAddress))
                    .thenCompose(channel -> toCompletableFuture(channel.connect(physicalAddress)));
        } else {
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channelInitializerHandler::initSocks5IfConfig)
                    .thenCompose(ch ->
                            channelInitializerHandler.initializeClientCnx(ch, logicalAddress,
                                    unresolvedPhysicalAddress))
                    .thenCompose(channel -> toCompletableFuture(channel.connect(physicalAddress)));
        }
    }

    public void releaseConnection(ClientCnx cnx) {
        if (maxConnectionsPerHosts == 0) {
            //Disable pooling
            if (cnx.channel().isActive()) {
                if (log.isDebugEnabled()) {
                    log.debug("close connection due to pooling disabled.");
                }
                cnx.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        closeAllConnections();
        if (shouldCloseDnsResolver) {
            addressResolver.close();
        }
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

    public Set<CompletableFuture<ClientCnx>> getConnections() {
        return Collections.unmodifiableSet(
                pool.values().stream().flatMap(n -> n.values().stream()).collect(Collectors.toSet()));
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

}

