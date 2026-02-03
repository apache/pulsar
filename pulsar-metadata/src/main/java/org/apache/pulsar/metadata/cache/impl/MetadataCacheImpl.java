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
package org.apache.pulsar.metadata.cache.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@Slf4j
public class MetadataCacheImpl<T> implements MetadataCache<T>, Consumer<Notification> {
    @Getter
    private final MetadataStore store;
    private final MetadataStoreExtended storeExtended;
    private final MetadataSerde<T> serde;
    private final OrderedExecutor executor;
    private final ScheduledExecutorService schedulerExecutor;
    private final MetadataCacheConfig<T> cacheConfig;

    private final AsyncLoadingCache<String, Optional<CacheGetResult<T>>> objCache;

    public MetadataCacheImpl(String cacheName, MetadataStore store, TypeReference<T> typeRef,
                             MetadataCacheConfig<T> cacheConfig, OrderedExecutor executor,
                             ScheduledExecutorService schedulerExecutor) {
        this(cacheName, store, new JSONMetadataSerdeTypeRef<>(typeRef), cacheConfig, executor, schedulerExecutor);
    }

    public MetadataCacheImpl(String cacheName, MetadataStore store, JavaType type, MetadataCacheConfig<T> cacheConfig,
                             OrderedExecutor executor, ScheduledExecutorService schedulerExecutor) {
        this(cacheName, store, new JSONMetadataSerdeSimpleType<>(type), cacheConfig, executor, schedulerExecutor);
    }

    public MetadataCacheImpl(String cacheName, MetadataStore store, MetadataSerde<T> serde,
                             MetadataCacheConfig<T> cacheConfig, OrderedExecutor executor,
                             ScheduledExecutorService schedulerExecutor) {
        this.store = store;
        if (store instanceof MetadataStoreExtended) {
            this.storeExtended = (MetadataStoreExtended) store;
        } else {
            this.storeExtended = null;
        }
        this.serde = serde;
        this.cacheConfig = cacheConfig;
        this.executor = executor;
        this.schedulerExecutor = schedulerExecutor;

        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        if (cacheConfig.getRefreshAfterWriteMillis() > 0) {
            cacheBuilder.refreshAfterWrite(cacheConfig.getRefreshAfterWriteMillis(), TimeUnit.MILLISECONDS);
        }
        if (cacheConfig.getExpireAfterWriteMillis() > 0) {
            cacheBuilder.expireAfterWrite(cacheConfig.getExpireAfterWriteMillis(), TimeUnit.MILLISECONDS);
        }
        this.objCache = cacheBuilder
                .recordStats()
                .buildAsync(new AsyncCacheLoader<String, Optional<CacheGetResult<T>>>() {
                    @Override
                    public CompletableFuture<Optional<CacheGetResult<T>>> asyncLoad(String key, Executor executor) {
                        if (log.isDebugEnabled()) {
                            log.debug("Loading key {} into metadata cache {}", key, cacheName);
                        }
                        return readValueFromStore(key);
                    }

                    @Override
                    public CompletableFuture<Optional<CacheGetResult<T>>> asyncReload(
                            String key,
                            Optional<CacheGetResult<T>> oldValue,
                            Executor executor) {
                        if (store instanceof AbstractMetadataStore && ((AbstractMetadataStore) store).isConnected()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Reloading key {} into metadata cache {}", key, cacheName);
                            }
                            final var future = readValueFromStore(key);
                            future.thenAccept(val -> {
                                if (cacheConfig.getAsyncReloadConsumer() != null) {
                                    cacheConfig.getAsyncReloadConsumer().accept(key, val);
                                }
                            });
                            return future;
                        } else {
                            // Do not try to refresh the cache item if we know that we're not connected to the
                            // metadata store
                            return CompletableFuture.completedFuture(oldValue);
                        }
                    }
                });

        CacheMetricsCollector.CAFFEINE.addCache(cacheName, objCache);
    }

    private CompletableFuture<Optional<CacheGetResult<T>>> readValueFromStore(String path) {
        final var future = new CompletableFuture<Optional<CacheGetResult<T>>>();
        store.get(path).thenComposeAsync(optRes -> {
            // There could be multiple pending reads for the same path, for example, when a path is created,
            // 1. The `accept` method will call `refresh`
            // 2. The `put` method will call `refresh` after the metadata store put operation is done
            // Both will call this method and the same result will be read. In this case, we only need to deserialize
            // the value once.
            if (!optRes.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Key {} not found in metadata store", path);
                }
                return FutureUtils.value(Optional.<CacheGetResult<T>>empty());
            }
            final var res = optRes.get();
            final var cachedFuture = objCache.getIfPresent(path);
            if (cachedFuture != null && cachedFuture != future) {
                if (log.isDebugEnabled()) {
                    log.debug("A new read on key {} is in progress or completed, ignore this one", path);
                }
                return cachedFuture;
            }
            try {
                T obj = serde.deserialize(path, res.getValue(), res.getStat());
                if (log.isDebugEnabled()) {
                    log.debug("Deserialized value for key {} (version: {})", path, res.getStat().getVersion());
                }
                return FutureUtils.value(Optional.of(new CacheGetResult<>(obj, res.getStat())));
            } catch (Throwable t) {
                return FutureUtils.exception(new ContentDeserializationException(
                    "Failed to deserialize payload for key '" + path + "'", t));
            }
        }, executor.chooseThread(path)).whenComplete((result, e) -> {
            if (e != null) {
                future.completeExceptionally(e.getCause());
            } else {
                future.complete(result);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Optional<T>> get(String path) {
        return objCache.get(path)
                .thenApply(optRes -> optRes.map(CacheGetResult::getValue));
    }

    @Override
    public CompletableFuture<Optional<CacheGetResult<T>>> getWithStats(String path) {
        return objCache.get(path);
    }

    @Override
    public Optional<T> getIfCached(String path) {
        CompletableFuture<Optional<CacheGetResult<T>>> future = objCache.getIfPresent(path);
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.join().map(CacheGetResult::getValue);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public CompletableFuture<T> readModifyUpdateOrCreate(String path, Function<Optional<T>, T> modifyFunction) {
        final var executor = this.executor.chooseThread(path);
        return executeWithRetry(() -> objCache.get(path)
                .thenComposeAsync(optEntry -> {
                    Optional<T> currentValue;
                    long expectedVersion;

                    if (optEntry.isPresent()) {
                        CacheGetResult<T> entry = optEntry.get();
                        T clone;
                        try {
                            // Use clone and CAS zk to ensure thread safety
                            clone = serde.deserialize(path, serde.serialize(path, entry.getValue()), entry.getStat());
                        } catch (IOException e) {
                            return FutureUtils.exception(e);
                        }
                        currentValue = Optional.of(clone);
                        expectedVersion = entry.getStat().getVersion();
                    } else {
                        currentValue = Optional.empty();
                        expectedVersion = -1;
                    }

                    T newValueObj;
                    byte[] newValue;
                    try {
                        newValueObj = modifyFunction.apply(currentValue);
                        newValue = serde.serialize(path, newValueObj);
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    return store.put(path, newValue, Optional.of(expectedVersion)).thenAccept(__ -> {
                        refresh(path);
                    }).thenApply(__ -> newValueObj);
                }, executor), path);
    }

    @Override
    public CompletableFuture<T> readModifyUpdate(String path, Function<T, T> modifyFunction) {
        final var executor = this.executor.chooseThread(path);
        return executeWithRetry(() -> objCache.get(path)
                .thenComposeAsync(optEntry -> {
                    if (!optEntry.isPresent()) {
                        return FutureUtils.exception(new NotFoundException(""));
                    }

                    CacheGetResult<T> entry = optEntry.get();
                    T currentValue = entry.getValue();
                    long expectedVersion = entry.getStat().getVersion();

                    T newValueObj;
                    byte[] newValue;
                    try {
                        // Use clone and CAS zk to ensure thread safety
                        currentValue = serde.deserialize(path, serde.serialize(path, currentValue), entry.getStat());
                        newValueObj = modifyFunction.apply(currentValue);
                        newValue = serde.serialize(path, newValueObj);
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    return store.put(path, newValue, Optional.of(expectedVersion)).thenAccept(__ -> {
                        refresh(path);
                    }).thenApply(__ -> newValueObj);
                }, executor), path);
    }

    private CompletableFuture<byte[]> serialize(String path, T value) {
        final var future = new CompletableFuture<byte[]>();
        executor.executeOrdered(path, () -> {
            try {
                future.complete(serde.serialize(path, value));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> create(String path, T value) {
        return serialize(path, value).thenCompose(content -> store.put(path, content, Optional.of(-1L)))
                .thenApply(stat -> {
                    // Make sure we have the value cached before the operation is completed
                    // In addition to caching the value, we need to add a watch on the path,
                    // so when/if it changes on any other node, we are notified and we can
                    // update the cache
                    return objCache.get(path);
                })
                .exceptionallyCompose(ex -> {
                    if (ex.getCause() instanceof BadVersionException) {
                        // Use already exists exception to provide more self-explanatory error message
                        return CompletableFuture.failedFuture(new AlreadyExistsException(ex.getCause()));
                    } else {
                        return CompletableFuture.failedFuture(ex.getCause());
                    }
                })
                .thenApply(__ -> null);
    }

    @Override
    public CompletableFuture<Void> put(String path, T value, EnumSet<CreateOption> options) {
        return serialize(path, value).thenCompose(bytes -> {
            if (storeExtended != null) {
                return storeExtended.put(path, bytes, Optional.empty(), options);
            } else {
                return store.put(path, bytes, Optional.empty());
            }
        }).thenAccept(__ -> {
            if (log.isDebugEnabled()) {
                log.debug("Refreshing path {} after put operation", path);
            }
            refresh(path);
        });
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return store.delete(path, Optional.empty());
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return store.exists(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        return store.getChildren(path);
    }

    @Override
    public void invalidate(String path) {
        objCache.synchronous().invalidate(path);
    }

    @Override
    public void refresh(String path) {
        // Refresh object of path if only it is cached before.
        objCache.asMap().computeIfPresent(path, (oldKey, oldValue) -> readValueFromStore(path));
    }

    @VisibleForTesting
    public void invalidateAll() {
        objCache.synchronous().invalidateAll();
    }

    @Override
    public void accept(Notification t) {
        String path = t.getPath();
        switch (t.getType()) {
        case Created:
        case Modified:
            if (log.isDebugEnabled()) {
                log.debug("Refreshing path {} for {} notification", path, t.getType());
            }
            refresh(path);
            break;

        case Deleted:
            objCache.synchronous().invalidate(path);
            break;

        default:
            break;
        }
    }

    private void execute(Supplier<CompletableFuture<T>> op, String key, CompletableFuture<T> result, Backoff backoff) {
        op.get().thenAccept(result::complete).exceptionally((ex) -> {
            if (ex.getCause() instanceof BadVersionException) {
                // if resource is updated by other than metadata-cache then metadata-cache will get bad-version
                // exception. so, try to invalidate the cache and try one more time.
                objCache.synchronous().invalidate(key);
                long elapsed = System.currentTimeMillis() - backoff.getFirstBackoffTimeInMillis();
                if (backoff.isMandatoryStopMade()) {
                    if (backoff.getFirstBackoffTimeInMillis() == 0) {
                        result.completeExceptionally(ex.getCause());
                    } else {
                        result.completeExceptionally(new TimeoutException(
                                String.format("Timeout to update key %s. Elapsed time: %d ms", key, elapsed)));
                    }
                    return null;
                }
                final var next = backoff.next();
                log.info("Update key {} conflicts. Retrying in {} ms. Mandatory stop: {}. Elapsed time: {} ms", key,
                        next, backoff.isMandatoryStopMade(), elapsed);
                schedulerExecutor.schedule(() -> execute(op, key, result, backoff), next, TimeUnit.MILLISECONDS);
                return null;
            }
            result.completeExceptionally(ex.getCause());
            return null;
        });
    }

    private CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> op, String key) {
        final var backoff = cacheConfig.getRetryBackoff().create();
        CompletableFuture<T> result = new CompletableFuture<>();
        execute(op, key, result, backoff);
        return result;
    }
}
