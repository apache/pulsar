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
package org.apache.pulsar.metadata.coordination.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Conflicting authoritative acquisitions must retire only the stale read-cache entry. */
public class LockManagerReadCacheTest {
    private static final String PATH = "/locks/owner";
    private static final String SELF = "broker-self";
    private static final String FOREIGN = "broker-foreign";

    @DataProvider
    public Object[][] foreignNodes() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "foreignNodes", timeOut = 10000)
    public void conflictingAcquireInvalidatesReadBeforeExposingFailure(boolean ephemeral) throws Exception {
        try (QuietMemoryStore store = new QuietMemoryStore();
                LockManagerImpl<String> manager = manager(store)) {
            ResourceLock<String> original = manager.acquireLock(PATH, SELF).get(5, TimeUnit.SECONDS);
            try {
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(SELF);
                Stat foreign = store.replace(PATH, FOREIGN, ephemeral);
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(SELF);
                int deletes = store.deletes.get();

                Optional<String> resolved = manager.acquireLock(PATH, SELF).handle((lock, error) -> {
                    assertThat(FutureUtil.unwrapCompletionException(error)).isInstanceOf(LockBusyException.class);
                    // The read must already be invalidated when the public failure becomes observable.
                    return manager.readLock(PATH);
                }).thenCompose(read -> read).get(5, TimeUnit.SECONDS);

                assertThat(resolved).contains(FOREIGN);
                assertThat(store.get(PATH).get(5, TimeUnit.SECONDS).orElseThrow().getStat().getVersion())
                        .isEqualTo(foreign.getVersion());
                assertThat(store.deletes.get()).isEqualTo(deletes);
                assertThat(original.getLockExpiredFuture()).isNotDone();
            } finally {
                store.expireReplaced(original);
            }
        }
    }

    @Test(timeOut = 10000)
    public void conflictingAcquireDoesNotInvalidateAnUnrelatedPath() throws Exception {
        try (QuietMemoryStore store = new QuietMemoryStore();
                LockManagerImpl<String> manager = manager(store)) {
            ResourceLock<String> original = manager.acquireLock(PATH, SELF).get(5, TimeUnit.SECONDS);
            try {
                String other = "/locks/unrelated";
                store.replace(other, "original-other", true);
                assertThat(manager.readLock(other).get(5, TimeUnit.SECONDS)).contains("original-other");
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(SELF);
                store.replace(other, "changed-other", true);
                store.replace(PATH, FOREIGN, true);

                assertThat(failure(manager.acquireLock(PATH, SELF))).isInstanceOf(LockBusyException.class);
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(FOREIGN);
                assertThat(manager.readLock(other).get(5, TimeUnit.SECONDS)).contains("original-other");
            } finally {
                store.expireReplaced(original);
            }
        }
    }

    @Test(timeOut = 10000)
    public void unknownAcquireFailureDoesNotInvalidateReadCache() throws Exception {
        try (QuietMemoryStore store = new QuietMemoryStore();
                LockManagerImpl<String> manager = manager(store)) {
            ResourceLock<String> original = manager.acquireLock(PATH, SELF).get(5, TimeUnit.SECONDS);
            try {
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(SELF);
                store.replace(PATH, FOREIGN, true);
                MetadataStoreException unavailable = new MetadataStoreException("controlled metadata failure");
                store.acquireFailure = unavailable;
                int reads = store.reads.get();

                assertThat(failure(manager.acquireLock(PATH, SELF))).isSameAs(unavailable);
                assertThat(manager.readLock(PATH).get(5, TimeUnit.SECONDS)).contains(SELF);
                assertThat(store.reads.get()).isEqualTo(reads);
            } finally {
                store.acquireFailure = null;
                store.expireReplaced(original);
            }
        }
    }

    private static LockManagerImpl<String> manager(QuietMemoryStore store) {
        return new LockManagerImpl<>(store, String.class, mock(ScheduledExecutorService.class));
    }

    private static Throwable failure(CompletableFuture<?> result) throws Exception {
        return result.handle((ignored, error) -> FutureUtil.unwrapCompletionException(error))
                .get(5, TimeUnit.SECONDS);
    }

    private static final class QuietMemoryStore extends LocalMemoryMetadataStore {
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger deletes = new AtomicInteger();
        private final Set<String> foreignPaths = ConcurrentHashMap.newKeySet();
        private volatile MetadataStoreException acquireFailure;

        private QuietMemoryStore() throws MetadataStoreException {
            super("memory:local", MetadataStoreConfig.builder().fsyncEnable(false).build());
        }

        @Override
        protected CompletableFuture<Void> receivedNotification(Notification notification) {
            // Keep both the cached value and old lock alive until the test explicitly delivers a notification.
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Optional<GetResult>> storeGet(String path, Set<Option> options) {
            reads.incrementAndGet();
            return super.storeGet(path, options).thenApply(result -> result.map(current -> {
                Stat stat = current.getStat();
                // The memory provider has one session. Preserve the real stored bytes/version while
                // modelling the foreign-session flag returned by a shared metadata backend.
                return new GetResult(current.getValue(), new Stat(path, stat.getVersion(),
                        stat.getCreationTimestamp(), stat.getModificationTimestamp(), stat.isEphemeral(),
                        !foreignPaths.contains(path)));
            }));
        }

        @Override
        public CompletableFuture<Stat> storePut(String path, byte[] value, Optional<Long> expected,
                                                Set<Option> options) {
            MetadataStoreException failure = acquireFailure;
            if (failure != null && expected.equals(Optional.of(-1L))) {
                return CompletableFuture.failedFuture(failure);
            }
            return super.storePut(path, value, expected, options);
        }

        @Override
        public CompletableFuture<Void> storeDelete(String path, Optional<Long> expected, Set<Option> options) {
            deletes.incrementAndGet();
            return super.storeDelete(path, expected, options);
        }

        private Stat replace(String path, String value, boolean ephemeral) throws Exception {
            // Another session changes the backend without this client's successful-write cache refresh.
            Stat replaced = super.storePut(path, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value),
                    Optional.empty(), ephemeral ? Set.of(Option.Ephemeral.INSTANCE) : Set.of())
                    .get(5, TimeUnit.SECONDS);
            foreignPaths.add(path);
            return replaced;
        }

        private void expireReplaced(ResourceLock<String> lock) throws Exception {
            super.receivedNotification(new Notification(NotificationType.Deleted, PATH)).get(5, TimeUnit.SECONDS);
            lock.getLockExpiredFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
