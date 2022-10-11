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
package org.apache.pulsar.metadata.cache.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@Slf4j
public class MetadataCacheImpl<T> implements MetadataCache<T>, Consumer<Notification> {
    @Getter
    private final MetadataStore store;
    private final MetadataSerde<T> serde;

    private final AsyncLoadingCache<String, Optional<CacheGetResult<T>>> objCache;

    public MetadataCacheImpl(MetadataStore store, TypeReference<T> typeRef, MetadataCacheConfig cacheConfig) {
        this(store, new JSONMetadataSerdeTypeRef<>(typeRef), cacheConfig);
    }

    public MetadataCacheImpl(MetadataStore store, JavaType type, MetadataCacheConfig cacheConfig) {
        this(store, new JSONMetadataSerdeSimpleType<>(type), cacheConfig);
    }

    public MetadataCacheImpl(MetadataStore store, MetadataSerde<T> serde, MetadataCacheConfig cacheConfig) {
        this.store = store;
        this.serde = serde;

        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        if (cacheConfig.getRefreshAfterWriteMillis() > 0) {
            cacheBuilder.refreshAfterWrite(cacheConfig.getRefreshAfterWriteMillis(), TimeUnit.MILLISECONDS);
        }
        if (cacheConfig.getExpireAfterWriteMillis() > 0) {
            cacheBuilder.expireAfterWrite(cacheConfig.getExpireAfterWriteMillis(), TimeUnit.MILLISECONDS);
        }
        this.objCache = cacheBuilder
                .buildAsync(new AsyncCacheLoader<String, Optional<CacheGetResult<T>>>() {
                    @Override
                    public CompletableFuture<Optional<CacheGetResult<T>>> asyncLoad(String key, Executor executor) {
                        return readValueFromStore(key);
                    }

                    @Override
                    public CompletableFuture<Optional<CacheGetResult<T>>> asyncReload(
                            String key,
                            Optional<CacheGetResult<T>> oldValue,
                            Executor executor) {
                        if (store instanceof AbstractMetadataStore && ((AbstractMetadataStore) store).isConnected()) {
                            return readValueFromStore(key);
                        } else {
                            // Do not try to refresh the cache item if we know that we're not connected to the
                            // metadata store
                            return CompletableFuture.completedFuture(oldValue);
                        }
                    }
                });
    }

    private CompletableFuture<Optional<CacheGetResult<T>>> readValueFromStore(String path) {
        return store.get(path)
                .thenCompose(optRes -> {
                    if (!optRes.isPresent()) {
                        return FutureUtils.value(Optional.empty());
                    }

                    try {
                        GetResult res = optRes.get();
                        T obj = serde.deserialize(path, res.getValue(), res.getStat());
                        return FutureUtils
                                .value(Optional.of(new CacheGetResult<>(obj, res.getStat())));
                    } catch (Throwable t) {
                        return FutureUtils.exception(new ContentDeserializationException(
                                "Failed to deserialize payload for key '" + path + "'", t));
                    }
                });
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
        return executeWithRetry(() -> objCache.get(path)
                .thenCompose(optEntry -> {
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
                }), path);
    }

    @Override
    public CompletableFuture<T> readModifyUpdate(String path, Function<T, T> modifyFunction) {
        return executeWithRetry(() -> objCache.get(path)
                .thenCompose(optEntry -> {
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
                }), path);
    }

    @Override
    public CompletableFuture<Void> create(String path, T value) {
        byte[] content;
        try {
            content = serde.serialize(path, value);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        store.put(path, content, Optional.of(-1L))
                .thenAccept(stat -> {
                    // Make sure we have the value cached before the operation is completed
                    // In addition to caching the value, we need to add a watch on the path,
                    // so when/if it changes on any other node, we are notified and we can
                    // update the cache
                    objCache.get(path).whenComplete((stat2, ex) -> {
                        if (ex == null) {
                            future.complete(null);
                        } else {
                            log.error("Exception while getting path {}", path, ex);
                            future.completeExceptionally(ex.getCause());
                        }
                    });
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof BadVersionException) {
                        // Use already exists exception to provide more self-explanatory error message
                        future.completeExceptionally(new AlreadyExistsException(ex.getCause()));
                    } else {
                        future.completeExceptionally(ex.getCause());
                    }
                    return null;
                });

        return future;
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
            refresh(path);
            break;

        case Deleted:
            objCache.synchronous().invalidate(path);
            break;

        default:
            break;
        }
    }

    private CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> op, String key) {
        CompletableFuture<T> result = new CompletableFuture<>();
        op.get().thenAccept(result::complete).exceptionally((ex) -> {
            if (ex.getCause() instanceof BadVersionException) {
                // if resource is updated by other than metadata-cache then metadata-cache will get bad-version
                // exception. so, try to invalidate the cache and try one more time.
                objCache.synchronous().invalidate(key);
                op.get().thenAccept(result::complete).exceptionally((ex1) -> {
                    result.completeExceptionally(ex1.getCause());
                    return null;
                });
                return null;
            }
            result.completeExceptionally(ex.getCause());
            return null;
        });
        return result;
    }
}
