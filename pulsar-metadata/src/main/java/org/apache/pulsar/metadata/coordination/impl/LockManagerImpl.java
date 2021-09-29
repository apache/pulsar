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
package org.apache.pulsar.metadata.coordination.impl;

import com.fasterxml.jackson.databind.type.TypeFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;

@Slf4j
class LockManagerImpl<T> implements LockManager<T> {

    private final Map<String, ResourceLockImpl<T>> locks = new ConcurrentHashMap<>();
    private final MetadataStoreExtended store;
    private final MetadataCache<T> cache;
    private final MetadataSerde<T> serde;
    private final ExecutorService executor;

    private enum State {
        Ready, Closed
    }

    private State state = State.Ready;

    LockManagerImpl(MetadataStoreExtended store, Class<T> clazz, ExecutorService executor) {
        this(store, new JSONMetadataSerdeSimpleType<>(
                TypeFactory.defaultInstance().constructSimpleType(clazz, null)),
                executor);
    }

    LockManagerImpl(MetadataStoreExtended store, MetadataSerde<T> serde, ExecutorService executor) {
        this.store = store;
        this.cache = store.getMetadataCache(serde);
        this.serde = serde;
        this.executor = executor;
        store.registerSessionListener(this::handleSessionEvent);
        store.registerListener(this::handleDataNotification);
    }

    @Override
    public CompletableFuture<Optional<T>> readLock(String path) {
        return cache.get(path);
    }

    @Override
    public CompletableFuture<ResourceLock<T>> acquireLock(String path, T value) {
        ResourceLockImpl<T> lock = new ResourceLockImpl<>(store, serde, path);

        CompletableFuture<ResourceLock<T>> result = new CompletableFuture<>();
        lock.acquire(value).thenRun(() -> {
            synchronized (LockManagerImpl.this) {
                if (state == State.Ready) {
                    locks.put(path, lock);
                    lock.getLockExpiredFuture().thenRun(() -> {
                        log.info("Released resource lock on {}", path);
                        synchronized (LockManagerImpl.this) {
                            locks.remove(path, lock);
                        }
                    });
                } else {
                    // LockManager was closed in between. Release the lock asynchronously
                    lock.release();
                }
            }
            result.complete(lock);
        }).exceptionally(ex -> {
            if (ex.getCause() instanceof BadVersionException) {
                result.completeExceptionally(
                        new LockBusyException("Resource at " + path + " is already locked"));
            } else {
                result.completeExceptionally(ex.getCause());
            }
            return null;
        });

        return result;
    }

    private void handleSessionEvent(SessionEvent se) {
        // We want to make sure we're processing one event at a time and that we're done with one event before going
        // for the next one.
        executor.execute(SafeRunnable.safeRun(() -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            if (se == SessionEvent.SessionReestablished) {
                log.info("Metadata store session has been re-established. Revalidating all the existing locks.");
                for (ResourceLockImpl<T> lock : locks.values()) {
                    futures.add(lock.revalidate(lock.getValue()));
                }

            } else if (se == SessionEvent.Reconnected) {
                log.info("Metadata store connection has been re-established. Revalidating locks that were pending.");
                for (ResourceLockImpl<T> lock : locks.values()) {
                    futures.add(lock.revalidateIfNeededAfterReconnection());
                }
            }

            try {
                FutureUtil.waitForAll(futures).get();
            } catch (ExecutionException|InterruptedException e) {
                log.warn("Failure when processing session event", e);
            }
        }));
    }

    private void handleDataNotification(Notification n) {
        if (n.getType() == NotificationType.Deleted) {
            ResourceLockImpl<T> lock = locks.get(n.getPath());
            if (lock != null) {
                lock.lockWasInvalidated();
            }
        }
    }

    @Override
    public CompletableFuture<List<String>> listLocks(String path) {
        return cache.getChildren(path);
    }

    @Override
    public void close() throws Exception {
        try {
            asyncClose().join();
        } catch (CompletionException e) {
            throw MetadataStoreException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        Map<String, ResourceLock<T>> locks;
        synchronized (this) {
            if (state != State.Ready) {
                return CompletableFuture.completedFuture(null);
            }

            locks = new HashMap<>(this.locks);
            this.state = State.Closed;
        }

        return FutureUtils.collect(
                locks.values().stream()
                        .map(ResourceLock::release)
                        .collect(Collectors.toList()))
                .thenApply(x -> null);
    }
}
