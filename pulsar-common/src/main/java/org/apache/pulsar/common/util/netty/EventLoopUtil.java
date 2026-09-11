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
package org.apache.pulsar.common.util.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringDatagramChannel;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;


@SuppressWarnings("checkstyle:JavadocType")
@CustomLog
public class EventLoopUtil {

    private static final String ENABLE_IO_URING = "pulsar.enableUring";

    /**
     * @return an EventLoopGroup suitable for the current platform
     */
    // Epoll/NioEventLoopGroup are deprecated since Netty 4.2 in favor of MultiThreadIoEventLoopGroup, but are
    // intentionally kept here as the (still-supported) epoll/nio transports; io_uring uses the new IoHandler API.
    @SuppressWarnings("deprecation")
    public static EventLoopGroup newEventLoopGroup(int nThreads, boolean enableBusyWait, ThreadFactory threadFactory) {
        if (Epoll.isAvailable()) {
            if (isIoUringEnabledAndAvailable()) {
                return new MultiThreadIoEventLoopGroup(nThreads, threadFactory, IoUringIoHandler.newFactory());
            } else {
                if (!enableBusyWait) {
                    // Regular Epoll based event loop
                    return new EpollEventLoopGroup(nThreads, threadFactory);
                }

                // With low latency setting, put the Netty event loop on busy-wait loop to reduce cost of
                // context switches
                EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup(nThreads, threadFactory,
                        () -> (selectSupplier, hasTasks) -> SelectStrategy.BUSY_WAIT);

                // Enable CPU affinity on IO threads
                for (int i = 0; i < nThreads; i++) {
                    eventLoopGroup.next().submit(() -> {
                        try {
                            CpuAffinity.acquireCore();
                        } catch (Throwable t) {
                            log.warn().attr("thread", Thread.currentThread().getName())
                                    .exception(t).log("Failed to acquire CPU core for thread");
                        }
                    });
                }

                return eventLoopGroup;
            }
        } else {
            // Fallback to NIO
            return new NioEventLoopGroup(nThreads, threadFactory);
        }
    }

    private static boolean isIoUringEnabledAndAvailable() {
        // By default, io_uring will not be enabled, even if available. The environment variable will be used:
        // enable.io_uring=1
        String ioUringSetting = System.getProperty(ENABLE_IO_URING);
        boolean ioUringEnabled = "1".equalsIgnoreCase(ioUringSetting) || "true".equalsIgnoreCase(ioUringSetting);
        if (ioUringEnabled) {
            // Throw exception if io_uring cannot be used
            IoUring.ensureAvailability();
        }
        return ioUringEnabled;
    }

    /**
     * Returns true if the given EventLoopGroup is backed by Netty's io_uring transport.
     * Since Netty 4.2 the classic {@code IOUringEventLoopGroup} no longer exists; io_uring groups are
     * {@link MultiThreadIoEventLoopGroup} instances (as are epoll/nio groups), so the IO handler type is
     * queried via {@link IoEventLoopGroup#isIoType} rather than {@code instanceof}.
     */
    private static boolean isIoUring(EventLoopGroup eventLoopGroup) {
        return eventLoopGroup instanceof IoEventLoopGroup
                && ((IoEventLoopGroup) eventLoopGroup).isIoType(IoUringIoHandler.class);
    }

    /**
     * Return a SocketChannel class suitable for the given EventLoopGroup implementation.
     *
     * @param eventLoopGroup
     * @return
     */
    @SuppressWarnings("deprecation") // EpollEventLoopGroup is deprecated since Netty 4.2 but still supported
    public static Class<? extends SocketChannel> getClientSocketChannelClass(EventLoopGroup eventLoopGroup) {
        if (isIoUring(eventLoopGroup)) {
            return IoUringSocketChannel.class;
        } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }

    /**
     * Returns the most appropriate SocketChannel implementation based on the system's capabilities
     * and configuration. Precedence:
     * 1) IO_uring (if available and enabled via 'pulsar.enableUring')
     * 2) Epoll (if available)
     * 3) NIO (fallback)
     */
    public static Class<? extends SocketChannel> getClientSocketChannelClass() {
        if (Epoll.isAvailable()) {
            if (isIoUringEnabledAndAvailable()) {
                return IoUringSocketChannel.class;
            } else {
                return EpollSocketChannel.class;
            }
        } else {
            return NioSocketChannel.class;
        }
    }

    @SuppressWarnings("deprecation") // EpollEventLoopGroup is deprecated since Netty 4.2 but still supported
    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass(EventLoopGroup eventLoopGroup) {
        if (isIoUring(eventLoopGroup)) {
            return IoUringServerSocketChannel.class;
        } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    /**
     * Returns the most appropriate ServerSocketChannel implementation based on the system's capabilities
     * and configuration. Precedence:
     * 1) IO_uring (if available and enabled via 'pulsar.enableUring')
     * 2) Epoll (if available)
     * 3) NIO (fallback)
     */
    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        if (Epoll.isAvailable()) {
            if (isIoUringEnabledAndAvailable()) {
                return IoUringServerSocketChannel.class;
            } else {
                return EpollServerSocketChannel.class;
            }
        } else {
            return NioServerSocketChannel.class;
        }
    }

    @SuppressWarnings("deprecation") // EpollEventLoopGroup is deprecated since Netty 4.2 but still supported
    public static Class<? extends DatagramChannel> getDatagramChannelClass(EventLoopGroup eventLoopGroup) {
        if (isIoUring(eventLoopGroup)) {
            return IoUringDatagramChannel.class;
        } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollDatagramChannel.class;
        } else {
            return NioDatagramChannel.class;
        }
    }

    /**
     * Returns the most appropriate DatagramChannel implementation based on the system's capabilities
     * and configuration. Precedence:
     * 1) IO_uring (if available and enabled via 'pulsar.enableUring')
     * 2) Epoll (if available)
     * 3) NIO (fallback)
     */
    public static Class<? extends DatagramChannel> getDatagramChannelClass() {
        if (Epoll.isAvailable()) {
            if (isIoUringEnabledAndAvailable()) {
                return IoUringDatagramChannel.class;
            } else {
                return EpollDatagramChannel.class;
            }
        } else {
            return NioDatagramChannel.class;
        }
    }

    @SuppressWarnings("deprecation") // EpollMode is deprecated in newer Netty but no replacement API exists yet
    public static void enableTriggeredMode(ServerBootstrap bootstrap) {
        if (Epoll.isAvailable()) {
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        }
    }

    /**
     * Shutdowns the EventLoopGroup gracefully. Returns a {@link CompletableFuture}
     * @param eventLoopGroup the event loop to shutdown
     * @return CompletableFuture that completes when the shutdown has completed
     */
    public static CompletableFuture<Void> shutdownGracefully(EventLoopGroup eventLoopGroup) {
        return NettyFutureUtil.toCompletableFutureVoid(eventLoopGroup.shutdownGracefully());
    }
}
