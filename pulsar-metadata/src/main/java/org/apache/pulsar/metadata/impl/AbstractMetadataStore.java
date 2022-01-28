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
package org.apache.pulsar.metadata.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;

@Slf4j
public abstract class AbstractMetadataStore implements MetadataStoreExtended, Consumer<Notification> {

    private static final long CACHE_REFRESH_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<SessionEvent>> sessionListeners = new CopyOnWriteArrayList<>();
    protected final ScheduledExecutorService executor;
    private final AsyncLoadingCache<String, List<String>> childrenCache;
    private final AsyncLoadingCache<String, Boolean> existsCache;
    private final CopyOnWriteArrayList<MetadataCacheImpl<?>> metadataCaches = new CopyOnWriteArrayList<>();

    // We don't strictly need to use 'volatile' here because we don't need the precise consistent semantic. Instead,
    // we want to avoid the overhead of 'volatile'.
    @Getter
    private boolean isConnected = true;

    protected abstract CompletableFuture<List<String>> getChildrenFromStore(String path);

    protected abstract CompletableFuture<Boolean> existsFromStore(String path);

    protected AbstractMetadataStore() {
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store"));
        registerListener(this);

        this.childrenCache = Caffeine.newBuilder()
                .refreshAfterWrite(CACHE_REFRESH_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .buildAsync(new AsyncCacheLoader<String, List<String>>() {
                    @Override
                    public CompletableFuture<List<String>> asyncLoad(String key, Executor executor) {
                        return getChildrenFromStore(key);
                    }

                    @Override
                    public CompletableFuture<List<String>> asyncReload(String key, List<String> oldValue,
                            Executor executor) {
                        if (isConnected) {
                            return getChildrenFromStore(key);
                        } else {
                            // Do not refresh if we're not connected
                            return CompletableFuture.completedFuture(oldValue);
                        }
                    }
                });

        this.existsCache = Caffeine.newBuilder()
                .refreshAfterWrite(CACHE_REFRESH_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .buildAsync(new AsyncCacheLoader<String, Boolean>() {
                    @Override
                    public CompletableFuture<Boolean> asyncLoad(String key, Executor executor) {
                        return existsFromStore(key);
                    }

                    @Override
                    public CompletableFuture<Boolean> asyncReload(String key, Boolean oldValue,
                            Executor executor) {
                        if (isConnected) {
                            return existsFromStore(key);
                        } else {
                            // Do not refresh if we're not connected
                            return CompletableFuture.completedFuture(oldValue);
                        }
                    }
                });
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(Class<T> clazz) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<T>(this,
                TypeFactory.defaultInstance().constructSimpleType(clazz, null));
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<T>(this, typeRef);
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<>(this, serde);
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return storeGet(path);
    }

    protected abstract CompletableFuture<Optional<GetResult>> storeGet(String path);

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, expectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public final CompletableFuture<List<String>> getChildren(String path) {
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return childrenCache.get(path);
    }

    @Override
    public final CompletableFuture<Boolean> exists(String path) {
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return existsCache.get(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        listeners.add(listener);
    }

    protected CompletableFuture<Void> receivedNotification(Notification notification) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                listeners.forEach(listener -> {
                    try {
                        listener.accept(notification);
                    } catch (Throwable t) {
                        log.error("Failed to process metadata store notification", t);
                    }
                });

                return null;
            }, executor);
        } catch (RejectedExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public void accept(Notification n) {
        String path = n.getPath();
        NotificationType type = n.getType();

        if (type == NotificationType.Created || type == NotificationType.Deleted) {
            existsCache.synchronous().invalidate(path);
            String parent = parent(path);
            if (parent != null) {
                childrenCache.synchronous().invalidate(parent);
            }
        }

        if (type == NotificationType.ChildrenChanged) {
            childrenCache.synchronous().invalidate(path);
        }

        if (type == NotificationType.Created || type == NotificationType.Deleted || type == NotificationType.Modified) {
            metadataCaches.forEach(c -> c.accept(n));
        }
    }

    protected abstract CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion);

    @Override
    public final CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        // Ensure caches are invalidated before the operation is confirmed
        return storeDelete(path, expectedVersion)
                .thenRun(() -> {
                    existsCache.synchronous().invalidate(path);
                    String parent = parent(path);
                    if (parent != null) {
                        childrenCache.synchronous().invalidate(parent);
                    }

                    metadataCaches.forEach(c -> c.invalidate(path));
                });
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path) {
        return getChildren(path)
                .thenCompose(children -> FutureUtil.waitForAll(
                        children.stream()
                                .map(child -> deleteRecursive(path + "/" + child))
                                .collect(Collectors.toList())))
                .thenCompose(__ -> exists(path))
                .thenCompose(exists -> {
                    if (exists) {
                        return delete(path, Optional.empty());
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    protected abstract CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                        EnumSet<CreateOption> options);

    @Override
    public final CompletableFuture<Stat> put(String path, byte[] data, Optional<Long> optExpectedVersion,
            EnumSet<CreateOption> options) {
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        // Ensure caches are invalidated before the operation is confirmed
        return storePut(path, data, optExpectedVersion, options)
                .thenApply(stat -> {
                    NotificationType type = stat.getVersion() == 0 ? NotificationType.Created
                            : NotificationType.Modified;
                    if (type == NotificationType.Created) {
                        existsCache.synchronous().invalidate(path);
                        String parent = parent(path);
                        if (parent != null) {
                            childrenCache.synchronous().invalidate(parent);
                        }
                    }

                    metadataCaches.forEach(c -> c.refresh(path));
                    return stat;
                });
    }

    @Override
    public void registerSessionListener(Consumer<SessionEvent> listener) {
        sessionListeners.add(listener);
    }

    protected void receivedSessionEvent(SessionEvent event) {
        isConnected = event.isConnected();

        sessionListeners.forEach(l -> {
            try {
                l.accept(event);
            } catch (Throwable t) {
                log.warn("Error in processing session event", t);
            }
        });
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void invalidateAll() {
        childrenCache.synchronous().invalidateAll();
        existsCache.synchronous().invalidateAll();
    }

    /**
     * Run the task in the executor thread and fail the future if the executor is shutting down.
     */
    protected void execute(Runnable task, CompletableFuture<?> future) {
        try {
            executor.execute(task);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    protected static String parent(String path) {
        int idx = path.lastIndexOf('/');
        if (idx <= 0) {
            // No parent
            return null;
        }

        return path.substring(0, idx);
    }

    /**
     * valid path in metadata store should be
     * 1. not blank
     * 2. starts with '/'
     * 3. not ends with '/', except root path "/"
     */
   static boolean isValidPath(String path) {
        return StringUtils.equals(path, "/")
                || StringUtils.isNotBlank(path)
                && path.startsWith("/")
                && !path.endsWith("/");
    }

    protected void notifyParentChildrenChanged(String path) {
        String parent = parent(path);
        while (parent != null) {
            receivedNotification(new Notification(NotificationType.ChildrenChanged, parent));
            parent = parent(parent);
        }
    }
}
