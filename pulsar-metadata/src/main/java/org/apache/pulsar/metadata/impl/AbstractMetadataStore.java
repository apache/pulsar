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
package org.apache.pulsar.metadata.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.apache.pulsar.metadata.impl.stats.MetadataStoreStats;

@Slf4j
public abstract class AbstractMetadataStore implements MetadataStoreExtended, Consumer<Notification> {
    private static final long CACHE_REFRESH_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<SessionEvent>> sessionListeners = new CopyOnWriteArrayList<>();
    protected final String metadataStoreName;
    protected final ScheduledExecutorService executor;
    private final AsyncLoadingCache<String, List<String>> childrenCache;
    private final AsyncLoadingCache<String, Boolean> existsCache;
    private final CopyOnWriteArrayList<MetadataCacheImpl<?>> metadataCaches = new CopyOnWriteArrayList<>();
    private final MetadataStoreStats metadataStoreStats;

    // We don't strictly need to use 'volatile' here because we don't need the precise consistent semantic. Instead,
    // we want to avoid the overhead of 'volatile'.
    @Getter
    private boolean isConnected = true;

    protected final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected abstract CompletableFuture<Boolean> existsFromStore(String path);

    protected AbstractMetadataStore(String metadataStoreName) {
        this.executor = new ScheduledThreadPoolExecutor(1,
                new DefaultThreadFactory(
                        StringUtils.isNotBlank(metadataStoreName) ? metadataStoreName : getClass().getSimpleName()));
        registerListener(this);

        this.childrenCache = Caffeine.newBuilder()
                .refreshAfterWrite(CACHE_REFRESH_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .expireAfterWrite(CACHE_REFRESH_TIME_MILLIS * 2, TimeUnit.MILLISECONDS)
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
                .expireAfterWrite(CACHE_REFRESH_TIME_MILLIS * 2, TimeUnit.MILLISECONDS)
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

        this.metadataStoreName = metadataStoreName;
        this.metadataStoreStats = new MetadataStoreStats(metadataStoreName);
    }

    @Override
    public CompletableFuture<Void> handleMetadataEvent(MetadataEvent event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        get(event.getPath()).thenApply(res -> {
            Set<CreateOption> options = event.getOptions() != null ? event.getOptions()
                    : Collections.emptySet();
            if (res.isPresent()) {
                GetResult existingValue = res.get();
                if (shouldIgnoreEvent(event, existingValue)) {
                    result.complete(null);
                    return result;
                }
            }
            // else update the event
            CompletableFuture<?> updateResult = (event.getType() == NotificationType.Deleted)
                    ? deleteInternal(event.getPath(), Optional.empty())
                    : putInternal(event.getPath(), event.getValue(),
                    Optional.ofNullable(event.getExpectedVersion()), options);
            updateResult.thenApply(stat -> {
                if (log.isDebugEnabled()) {
                    log.debug("successfully updated {}", event.getPath());
                }
                return result.complete(null);
            }).exceptionally(ex -> {
                log.warn("Failed to update metadata {}", event.getPath(), ex.getCause());
                if (ex.getCause() instanceof MetadataStoreException.BadVersionException) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(ex);
                }
                return false;
            });
            return result;
        });
        return result;
    }

    /**
     *  @deprecated Use {@link #registerSyncListener(Optional)} instead.
     */
    @Deprecated
    protected void registerSyncLister(Optional<MetadataEventSynchronizer> synchronizer) {
        this.registerSyncListener(synchronizer);
    }

    protected void registerSyncListener(Optional<MetadataEventSynchronizer> synchronizer) {
        synchronizer.ifPresent(s -> s.registerSyncListener(this::handleMetadataEvent));
    }

    @VisibleForTesting
    protected boolean shouldIgnoreEvent(MetadataEvent event, GetResult existingValue) {
        long existingVersion = existingValue.getStat() != null ? existingValue.getStat().getVersion() : -1;
        long existingTimestamp = existingValue.getStat() != null ? existingValue.getStat().getModificationTimestamp()
                : -1;
        String sourceClusterName = event.getSourceCluster();
        Set<CreateOption> options = event.getOptions() != null ? event.getOptions()
                : Collections.emptySet();
        String currentClusterName = getMetadataEventSynchronizer().get().getClusterName();
        // ignore event from the unknown cluster
        if (sourceClusterName == null || currentClusterName == null) {
            return true;
        }
        // ignore event if metadata is ephemeral or
        // sequential
        if (options.contains(CreateOption.Ephemeral) || event.getOptions().contains(CreateOption.Sequential)) {
            return true;
        }
        // ignore the event if event occurred before the
        // existing update
        if (event.getLastUpdatedTimestamp() < existingTimestamp) {
            return true;
        }
        if (currentClusterName.equals(sourceClusterName)) {
            // if expected version doesn't exist then ignore the
            // event
            if ((event.getExpectedVersion() != null && event.getExpectedVersion() > 0)
                    && event.getExpectedVersion() != existingVersion) {
                return true;
            }

        } else if ((event.getLastUpdatedTimestamp() == existingTimestamp
                && sourceClusterName.compareTo(currentClusterName) < 0)) {
            // ignore: if event is older than existing store
            // metadata
            // or if timestamp is same for both the event then
            // larger cluster-name according to lexical sorting
            // should win for the update.
            return true;
        }
        return false;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(Class<T> clazz, MetadataCacheConfig cacheConfig) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<T>(this,
                TypeFactory.defaultInstance().constructSimpleType(clazz, null), cacheConfig, this.executor);
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef, MetadataCacheConfig cacheConfig) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<T>(this, typeRef, cacheConfig, this.executor);
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde, MetadataCacheConfig cacheConfig) {
        MetadataCacheImpl<T> metadataCache = new MetadataCacheImpl<>(this, serde, cacheConfig, this.executor);
        metadataCaches.add(metadataCache);
        return metadataCache;
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        long start = System.currentTimeMillis();
        if (!isValidPath(path)) {
            metadataStoreStats.recordGetOpsFailed(System.currentTimeMillis() - start);
            return FutureUtil
                    .failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return storeGet(path)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        metadataStoreStats.recordGetOpsFailed(System.currentTimeMillis() - start);
                    } else {
                        metadataStoreStats.recordGetOpsSucceeded(System.currentTimeMillis() - start);
                    }
                });
    }

    protected abstract CompletableFuture<Optional<GetResult>> storeGet(String path);

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, expectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public final CompletableFuture<List<String>> getChildren(String path) {
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return childrenCache.get(path);
    }

    @Override
    public final CompletableFuture<Boolean> exists(String path) {
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        if (!isValidPath(path)) {
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        return existsCache.get(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        // If the metadata store is closed, do nothing here.
        if (!isClosed()) {
            listeners.add(listener);
        }
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
            childrenCache.synchronous().invalidate(path);
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
        log.info("Deleting path: {} (v. {})", path, expectedVersion);
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        long start = System.currentTimeMillis();
        if (!isValidPath(path)) {
            metadataStoreStats.recordDelOpsFailed(System.currentTimeMillis() - start);
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        if (getMetadataEventSynchronizer().isPresent()) {
            MetadataEvent event = new MetadataEvent(path, null, new HashSet<>(),
                    expectedVersion.orElse(null), Instant.now().toEpochMilli(),
                    getMetadataEventSynchronizer().get().getClusterName(), NotificationType.Deleted);
            return getMetadataEventSynchronizer().get().notify(event)
                    .thenCompose(__ -> deleteInternal(path, expectedVersion))
                    .whenComplete((v, t) -> {
                        if (null != t) {
                            metadataStoreStats.recordDelOpsFailed(System.currentTimeMillis() - start);
                        } else {
                            metadataStoreStats.recordDelOpsSucceeded(System.currentTimeMillis() - start);
                        }
                    });
        } else {
            return deleteInternal(path, expectedVersion)
                    .whenComplete((v, t) -> {
                        if (null != t) {
                            metadataStoreStats.recordDelOpsFailed(System.currentTimeMillis() - start);
                        } else {
                            metadataStoreStats.recordDelOpsSucceeded(System.currentTimeMillis() - start);
                        }
                    });
        }
    }

    private CompletableFuture<Void> deleteInternal(String path, Optional<Long> expectedVersion) {
        // Ensure caches are invalidated before the operation is confirmed
        return storeDelete(path, expectedVersion).thenRun(() -> {
            existsCache.synchronous().invalidate(path);
            childrenCache.synchronous().invalidate(path);
            String parent = parent(path);
            if (parent != null) {
                childrenCache.synchronous().invalidate(parent);
            }

            metadataCaches.forEach(c -> c.invalidate(path));
            log.info("Deleted path: {} (v. {})", path, expectedVersion);
        });
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path) {
        log.info("Deleting recursively path: {}", path);
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        return getChildren(path)
                .thenCompose(children -> FutureUtil.waitForAll(
                        children.stream()
                                .map(child -> deleteRecursive(path + "/" + child))
                                .collect(Collectors.toList())))
                .thenCompose(__ -> {
                    log.info("After deleting all children, now deleting path: {}", path);
                    return deleteIfExists(path, Optional.empty());
                });
    }

    protected abstract CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                        EnumSet<CreateOption> options);

    @Override
    public final CompletableFuture<Stat> put(String path, byte[] data, Optional<Long> optExpectedVersion,
            EnumSet<CreateOption> options) {
        if (isClosed()) {
            return alreadyClosedFailedFuture();
        }
        long start = System.currentTimeMillis();
        if (!isValidPath(path)) {
            metadataStoreStats.recordPutOpsFailed(System.currentTimeMillis() - start);
            return FutureUtil.failedFuture(new MetadataStoreException.InvalidPathException(path));
        }
        HashSet<CreateOption> ops = new HashSet<>(options);
        if (getMetadataEventSynchronizer().isPresent()) {
            Long version = optExpectedVersion.isPresent() && optExpectedVersion.get() < 0 ? null
                    : optExpectedVersion.orElse(null);
            MetadataEvent event = new MetadataEvent(path, data, ops, version,
                    Instant.now().toEpochMilli(), getMetadataEventSynchronizer().get().getClusterName(),
                    NotificationType.Modified);
            return getMetadataEventSynchronizer().get().notify(event)
                    .thenCompose(__ -> putInternal(path, data, optExpectedVersion, options))
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            metadataStoreStats.recordPutOpsFailed(System.currentTimeMillis() - start);
                        } else {
                            int len = data == null ? 0 : data.length;
                            metadataStoreStats.recordPutOpsSucceeded(System.currentTimeMillis() - start, len);
                        }
                    });
        } else {
            return putInternal(path, data, optExpectedVersion, options)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            metadataStoreStats.recordPutOpsFailed(System.currentTimeMillis() - start);
                        } else {
                            int len = data == null ? 0 : data.length;
                            metadataStoreStats.recordPutOpsSucceeded(System.currentTimeMillis() - start, len);
                        }
                    });
        }

    }
    public final CompletableFuture<Stat> putInternal(String path, byte[] data, Optional<Long> optExpectedVersion,
            Set<CreateOption> options) {
        // Ensure caches are invalidated before the operation is confirmed
        return storePut(path, data, optExpectedVersion,
                (options != null && !options.isEmpty()) ? EnumSet.copyOf(options) : EnumSet.noneOf(CreateOption.class))
                .thenApply(stat -> {
                    NotificationType type = stat.isFirstVersion() ? NotificationType.Created
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

        // Clear cache after session expired.
        if (event == SessionEvent.SessionReestablished || event == SessionEvent.Reconnected) {
            for (MetadataCacheImpl metadataCache : metadataCaches) {
                metadataCache.invalidateAll();
            }
            invalidateAll();
        }

        // Notice listeners.
        try {
            executor.execute(() -> {
                sessionListeners.forEach(l -> {
                    try {
                        l.accept(event);
                    } catch (Throwable t) {
                        log.warn("Error in processing session event " + event, t);
                    }
                });
            });
        } catch (RejectedExecutionException e) {
            log.warn("Error in processing session event " + event, e);
        }
    }

    protected boolean isClosed() {
        return isClosed.get();
    }

    protected static <T> CompletableFuture<T> alreadyClosedFailedFuture() {
        return FutureUtil.failedFuture(
                new MetadataStoreException.AlreadyClosedException());
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        this.metadataStoreStats.close();
    }

    @VisibleForTesting
    public void invalidateAll() {
        childrenCache.synchronous().invalidateAll();
        existsCache.synchronous().invalidateAll();
    }

    public void invalidateCaches(String...paths) {
        LoadingCache<String, List<String>> loadingCache = childrenCache.synchronous();
        for (String path : paths) {
            loadingCache.invalidate(path);
        }
    }

    /**
     * Run the task in the executor thread and fail the future if the executor is shutting down.
     */
    @VisibleForTesting
    public void execute(Runnable task, CompletableFuture<?> future) {
        try {
            executor.execute(task);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    /**
     * Run the task in the executor thread and fail the future if the executor is shutting down.
     */
    @VisibleForTesting
    public void execute(Runnable task, Supplier<List<CompletableFuture<?>>> futures) {
        try {
            executor.execute(task);
        } catch (final Throwable t) {
            futures.get().forEach(f -> f.completeExceptionally(t));
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
