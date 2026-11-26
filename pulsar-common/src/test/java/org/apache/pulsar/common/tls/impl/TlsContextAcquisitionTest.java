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
package org.apache.pulsar.common.tls.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

public class TlsContextAcquisitionTest {
    @Test
    public void repeatedDisposeReleasesUnderlyingHandleOnce() throws Exception {
        LockingFactory factory = new LockingFactory();
        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(factory,
                TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(true), ignored -> { })
                .get(5, TimeUnit.SECONDS).orElseThrow();
        try {
            handle.dispose();
            handle.dispose();
            assertThat(factory.disposeCalls).as("repeated disposal must release the underlying handle once")
                    .hasValue(1);
        } finally {
            handle.dispose();
            factory.close();
        }
    }

    @Test
    public void concurrentDisposeReleasesUnderlyingHandleOnce() throws Exception {
        LockingFactory factory = new LockingFactory();
        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(factory,
                TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(true), ignored -> { })
                .get(5, TimeUnit.SECONDS).orElseThrow();
        ExecutorService pool = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("tls-concurrent-dispose-test"));
        try {
            Future<?> first;
            // Keep the first caller inside the underlying handle's dispose() while the second caller runs.
            factory.lock.lock();
            try {
                first = pool.submit(handle::dispose);
                assertThat(factory.disposeEntered.await(5, TimeUnit.SECONDS)).isTrue();
                Future<?> second = pool.submit(handle::dispose);
                await().atMost(5, TimeUnit.SECONDS)
                        .until(() -> second.isDone() || factory.disposeCalls.get() > 1);
                assertThat(factory.disposeCalls).as("concurrent disposal must release the underlying handle once")
                        .hasValue(1);
                second.get(5, TimeUnit.SECONDS);
            } finally {
                factory.lock.unlock();
            }
            first.get(5, TimeUnit.SECONDS);
            handle.dispose();
            assertThat(factory.disposeCalls).hasValue(1);
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).as("test threads must exit").isTrue();
            handle.dispose();
            factory.close();
        }
    }

    @Test
    public void disposeDoesNotDeadlockWithReload() throws Exception {
        LockingFactory factory = new LockingFactory();
        AtomicInteger deliveries = new AtomicInteger();
        TlsHandle<SslContext> handle = TlsContextAcquisition.acquireNettyContext(factory,
                TlsPurpose.CLIENT_DEFAULT, TlsSynthesisSpec.client(true),
                ignored -> deliveries.incrementAndGet()).get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(deliveries).hasValue(1);

        ExecutorService pool = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("tls-dispose-test"));
        Future<?> close = null;
        try {
            Future<?> reload = pool.submit(() -> {
                factory.lock.lock();
                try {
                    factory.rotationLocked.countDown();
                    if (!factory.deliver.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("reload was not released");
                    }
                    factory.callback.accept(factory.context);
                } finally {
                    factory.lock.unlock();
                }
                return null;
            });
            assertThat(factory.rotationLocked.await(5, TimeUnit.SECONDS)).isTrue();
            close = pool.submit(handle::dispose);
            assertThat(factory.disposeEntered.await(5, TimeUnit.SECONDS)).isTrue();
            factory.deliver.countDown();
            reload.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThat(deliveries).as("a delivery after disposal started must be ignored").hasValue(1);
        } finally {
            factory.deliver.countDown();
            // The factory lock is interruptible so the failing baseline cannot strand test threads.
            if (close != null) {
                close.cancel(true);
            }
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).as("test threads must exit").isTrue();
            handle.dispose();
            factory.close();
        }
    }

    private static final class LockingFactory implements PulsarTlsFactory {
        private final SSLContext context = SSLContext.getDefault();
        private final ReentrantLock lock = new ReentrantLock();
        private final CountDownLatch rotationLocked = new CountDownLatch(1);
        private final CountDownLatch disposeEntered = new CountDownLatch(1);
        private final CountDownLatch deliver = new CountDownLatch(1);
        private final AtomicInteger disposeCalls = new AtomicInteger();
        private Consumer<SSLContext> callback;

        private LockingFactory() throws Exception {
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (instanceClass != SSLContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.completedFuture(Optional.of(new TlsHandle<T>() {
                @Override
                public T get() {
                    return instanceClass.cast(context);
                }

                @Override
                public void dispose() {
                }
            }));
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            if (instanceClass != SSLContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            lock.lock();
            try {
                callback = value -> onLoadOrReload.accept(instanceClass.cast(value));
                callback.accept(context);
                return CompletableFuture.completedFuture(Optional.of(new TlsHandle<T>() {
                    @Override
                    public T get() {
                        return instanceClass.cast(context);
                    }

                    @Override
                    public void dispose() {
                        disposeCalls.incrementAndGet();
                        disposeEntered.countDown();
                        try {
                            lock.lockInterruptibly();
                            try {
                                callback = null;
                            } finally {
                                lock.unlock();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }));
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
        }
    }
}
