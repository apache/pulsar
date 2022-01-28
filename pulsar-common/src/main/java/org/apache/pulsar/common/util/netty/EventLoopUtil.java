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
package org.apache.pulsar.common.util.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;

@SuppressWarnings("checkstyle:JavadocType")
@Slf4j
public class EventLoopUtil {

    /**
     * @return an EventLoopGroup suitable for the current platform
     */
    public static EventLoopGroup newEventLoopGroup(int nThreads, boolean enableBusyWait, ThreadFactory threadFactory) {
        if (Epoll.isAvailable()) {
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
                        log.warn("Failed to acquire CPU core for thread {} {}", Thread.currentThread().getName(),
                                t.getMessage(), t);
                    }
                });
            }

            return eventLoopGroup;
        } else {
            // Fallback to NIO
            return new NioEventLoopGroup(nThreads, threadFactory);
        }
    }

    /**
     * Return a SocketChannel class suitable for the given EventLoopGroup implementation.
     *
     * @param eventLoopGroup
     * @return
     */
    public static Class<? extends SocketChannel> getClientSocketChannelClass(EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }

    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass(EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    public static Class<? extends DatagramChannel> getDatagramChannelClass(EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollDatagramChannel.class;
        } else {
            return NioDatagramChannel.class;
        }
    }

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
