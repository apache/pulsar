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
package org.apache.pulsar.broker.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.testng.annotations.Test;

public class WebExecutorThreadPoolTest {

    @Test(timeOut = 30000)
    public void testJoinReturnsAfterServerFailsToBindPort() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress(loopback, 0));
            WebExecutorThreadPool pool = new WebExecutorThreadPool(4, "test-web");
            Server server = new Server(pool);
            ServerConnector connector = new ServerConnector(server);
            connector.setHost(loopback.getHostAddress());
            connector.setPort(occupied.getLocalPort());
            server.addConnector(connector);

            assertThatThrownBy(server::start).isInstanceOf(IOException.class);
            assertThat(pool.isStarted()).isFalse();
            server.stop();

            assertJoinCompletes(pool);
        }
    }

    @Test(timeOut = 30000)
    public void testJoinReturnsAfterStartedPoolIsStopped() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(4, "test-web");
        pool.start();
        pool.stop();

        assertJoinCompletes(pool);
    }

    @Test(timeOut = 30000)
    public void testIsPoolThread() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(4, "test-web");
        WebExecutorThreadPool otherPool = new WebExecutorThreadPool(4, "test-other-web");
        pool.start();
        otherPool.start();
        try {
            assertThat(pool.isPoolThread()).isFalse();
            CompletableFuture<Boolean> onPoolThread = new CompletableFuture<>();
            pool.execute(() -> onPoolThread.complete(pool.isPoolThread()));
            assertThat(onPoolThread).succeedsWithin(10, TimeUnit.SECONDS).isEqualTo(true);
            CompletableFuture<Boolean> onOtherPoolThread = new CompletableFuture<>();
            otherPool.execute(() -> onOtherPoolThread.complete(pool.isPoolThread()));
            assertThat(onOtherPoolThread).succeedsWithin(10, TimeUnit.SECONDS).isEqualTo(false);
        } finally {
            pool.stop();
            otherPool.stop();
        }
    }

    private static void assertJoinCompletes(WebExecutorThreadPool pool) {
        CompletableFuture<Void> joined = CompletableFuture.runAsync(() -> {
            try {
                pool.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        assertThat(joined).succeedsWithin(10, TimeUnit.SECONDS);
    }
}
