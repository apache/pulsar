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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
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
    public void testPoolWithoutMaxThreadsQueuesWhenAllThreadsAreBusy() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 0, "test-web", 8192);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockThreads(pool, 2, release);
            CountDownLatch queuedTaskRan = new CountDownLatch(1);
            pool.execute(queuedTaskRan::countDown);

            assertThat(queuedTaskRan.await(200, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(pool.getThreads()).isEqualTo(2);
            assertThat(pool.getMaxThreads()).isEqualTo(2);

            release.countDown();
            assertThat(queuedTaskRan.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testPoolGrowsUpToMaxThreadsBeforeQueueing() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 4, "test-web", 8192);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            // all four tasks run at the same time, so two threads are started beyond the two core threads
            blockThreads(pool, 4, release);
            assertThat(pool.getThreads()).isEqualTo(4);
            assertThat(pool.getMaxThreads()).isEqualTo(4);

            CountDownLatch queuedTaskRan = new CountDownLatch(1);
            pool.execute(queuedTaskRan::countDown);
            assertThat(queuedTaskRan.await(200, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(pool.getThreads()).isEqualTo(4);

            release.countDown();
            assertThat(queuedTaskRan.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testPoolUsesIdleThreadsBeforeStartingNewOnes() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 4, "test-web", 8192);
        pool.start();
        try {
            assertThat(pool.getThreads()).isEqualTo(2);
            for (int i = 0; i < 20; i++) {
                CountDownLatch ran = new CountDownLatch(1);
                pool.execute(ran::countDown);
                assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(pool.getThreads()).isEqualTo(2);
        } finally {
            pool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testThreadsBeyondNumThreadsStopAfterIdleTimeout() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 4, "test-web", 8192);
        pool.setIdleTimeout(100);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockThreads(pool, 4, release);
            assertThat(pool.getThreads()).isEqualTo(4);
            release.countDown();
            Awaitility.await().untilAsserted(() -> assertThat(pool.getThreads()).isEqualTo(2));
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testPoolRejectsTasksWhenQueueIsFullAtMaxThreads() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 3, "test-web", 1);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockThreads(pool, 3, release);
            pool.execute(() -> { });
            assertThatThrownBy(() -> pool.execute(() -> { })).isInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test(timeOut = 120000)
    public void testTaskIsNotStrandedWhileAnIdleThreadRetires() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 3, "test-web", 8192);
        // a very short idle timeout makes the extra thread retire while tasks are being submitted
        pool.setIdleTimeout(1);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockThreads(pool, 2, release);
            for (int i = 0; i < 5000; i++) {
                CountDownLatch ran = new CountDownLatch(1);
                pool.execute(ran::countDown);
                assertThat(ran.await(5, TimeUnit.SECONDS)).as("task %d ran", i).isTrue();
                // vary the pause around the idle timeout so that submissions race with the thread stopping
                TimeUnit.MICROSECONDS.sleep(500 + (i % 13) * 80);
            }
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testGrowingPoolReservesThreadsLikeFixedPool() throws Exception {
        // reserved threads don't stop after being idle, so they are sized from numThreads and not maxThreads
        WebExecutorThreadPool fixedPool = new WebExecutorThreadPool(64, 0, "test-web", 8192);
        WebExecutorThreadPool growingPool = new WebExecutorThreadPool(64, 1024, "test-web", 8192);
        fixedPool.start();
        growingPool.start();
        try {
            assertThat(growingPool.getMaxThreads()).isEqualTo(1024);
            assertThat(growingPool.getReservedThreads()).isEqualTo(fixedPool.getReservedThreads());
        } finally {
            fixedPool.stop();
            growingPool.stop();
        }
    }

    @Test(timeOut = 30000)
    public void testSetMaxThreadsKeepsPoolGrowing() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 3, "test-web", 8192);
        pool.setMaxThreads(4);
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertThat(pool.getMaxThreads()).isEqualTo(4);
            // the pool keeps 2 threads and still grows up to the new maximum
            assertThat(pool.getThreads()).isEqualTo(2);
            blockThreads(pool, 4, release);
            assertThat(pool.getThreads()).isEqualTo(4);
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test
    public void testIdleTimeoutMustBePositive() {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, 4, "test-web", 8192);
        assertThatThrownBy(() -> pool.setIdleTimeout(0)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 ms");
    }

    private static void blockThreads(WebExecutorThreadPool pool, int count, CountDownLatch release)
            throws InterruptedException {
        CountDownLatch started = new CountDownLatch(count);
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tasks.add(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        tasks.forEach(pool::execute);
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
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
