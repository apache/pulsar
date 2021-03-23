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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.Getter;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;

public class MetadataCacheImpl<T> implements MetadataCache<T>, Consumer<Notification> {

    private static final long CACHE_REFRESH_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

    @Getter
    private final MetadataStore store;
    private final MetadataSerde<T> serde;

    private final AsyncLoadingCache<String, Optional<Entry<T, Stat>>> objCache;

    public MetadataCacheImpl(MetadataStore store, TypeReference<T> typeRef) {
        this(store, new JSONMetadataSerdeTypeRef<>(typeRef));
    }

    public MetadataCacheImpl(MetadataStore store, JavaType type) {
        this(store, new JSONMetadataSerdeSimpleType<>(type));
    }

    private MetadataCacheImpl(MetadataStore store, MetadataSerde<T> serde) {
        this.store = store;
        this.serde = serde;

        this.objCache = Caffeine.newBuilder()
                .refreshAfterWrite(CACHE_REFRESH_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .buildAsync(new AsyncCacheLoader<String, Optional<Entry<T, Stat>>>() {
                    @Override
                    public CompletableFuture<Optional<Entry<T, Stat>>> asyncLoad(String key, Executor executor) {
                        return readValueFromStore(key);
                    }

                    @Override
                    public CompletableFuture<Optional<Entry<T, Stat>>> asyncReload(String key,
                            Optional<Entry<T, Stat>> oldValue, Executor executor) {
                        return readValueFromStore(key);
                    }
                });
    }

    private CompletableFuture<Optional<Entry<T, Stat>>> readValueFromStore(String path) {
        return store.get(path)
                .thenCompose(optRes -> {
                    if (!optRes.isPresent()) {
                        return FutureUtils.value(Optional.empty());
                    }

                    try {
                        T obj = serde.deserialize(optRes.get().getValue());
                        return FutureUtils
                                .value(Optional.of(new SimpleImmutableEntry<T, Stat>(obj, optRes.get().getStat())));
                    } catch (Throwable t) {
                        return FutureUtils.exception(new ContentDeserializationException(t));
                    }
                });
    }

    @Override
    public CompletableFuture<Optional<T>> get(String path) {
        return objCache.get(path)
                .thenApply(optRes -> optRes.map(Entry::getKey));
    }

    @Override
    public Optional<T> getIfCached(String path) {
        CompletableFuture<Optional<Map.Entry<T, Stat>>> future = objCache.getIfPresent(path);
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.join().map(Entry::getKey);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public CompletableFuture<Void> readModifyUpdateOrCreate(String path, Function<Optional<T>, T> modifyFunction) {
        return executeWithRetry(() -> objCache.get(path)
                .thenCompose(optEntry -> {
                    Optional<T> currentValue;
                    long expectedVersion;

                    if (optEntry.isPresent()) {
                        T clone;
                        try {
                            // Use clone and CAS zk to ensure thread safety
                            clone = serde.deserialize(serde.serialize(optEntry.get().getKey()));
                        } catch (IOException e) {
                            return FutureUtils.exception(e);
                        }
                        currentValue = Optional.of(clone);
                        expectedVersion = optEntry.get().getValue().getVersion();
                    } else {
                        currentValue = Optional.empty();
                        expectedVersion = -1;
                    }

                    T newValueObj;
                    byte[] newValue;
                    try {
                        newValueObj = modifyFunction.apply(currentValue);
                        newValue = serde.serialize(newValueObj);
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    return store.put(path, newValue, Optional.of(expectedVersion)).thenAccept(stat -> {
                        // Make sure we have the value cached before the operation is completed
                        objCache.put(path,
                                FutureUtils.value(Optional.of(new SimpleImmutableEntry<T, Stat>(newValueObj, stat))));
                    });
                }), path);
    }

    @Override
    public CompletableFuture<Void> readModifyUpdate(String path, Function<T, T> modifyFunction) {
        return executeWithRetry(() -> objCache.get(path)
                .thenCompose(optEntry -> {
                    if (!optEntry.isPresent()) {
                        return FutureUtils.exception(new NotFoundException(""));
                    }

                    Map.Entry<T, Stat> entry = optEntry.get();
                    T currentValue = entry.getKey();
                    long expectedVersion = optEntry.get().getValue().getVersion();

                    T newValueObj;
                    byte[] newValue;
                    try {
                        // Use clone and CAS zk to ensure thread safety
                        currentValue = serde.deserialize(serde.serialize(currentValue));
                        newValueObj = modifyFunction.apply(currentValue);
                        newValue = serde.serialize(newValueObj);
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    return store.put(path, newValue, Optional.of(expectedVersion)).thenAccept(stat -> {
                        // Make sure we have the value cached before the operation is completed
                        objCache.put(path,
                                FutureUtils.value(Optional.of(new SimpleImmutableEntry<T, Stat>(newValueObj, stat))));
                    });
                }), path);
    }

    @Override
    public CompletableFuture<Void> create(String path, T value) {
        byte[] content;
        try {
            content = serde.serialize(value);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        store.put(path, content, Optional.of(-1L))
                .thenAccept(stat -> {
                    // Make sure we have the value cached before the operation is completed
                    objCache.put(path, FutureUtils.value(Optional.of(new SimpleImmutableEntry<T, Stat>(value, stat))));
                    future.complete(null);
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
        return store.delete(path, Optional.empty())
                .thenAccept(v -> {
                    // Mark in the cache that the object was removed
                    objCache.put(path, FutureUtils.value(Optional.empty()));
                });
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
            if (objCache.synchronous().getIfPresent(path) != null) {
                // Trigger background refresh of the cached item
                objCache.synchronous().refresh(path);
            }
            break;

        case Deleted:
            objCache.synchronous().invalidate(path);
            break;

        default:
            break;
        }
    }

    private CompletableFuture<Void> executeWithRetry(Supplier<CompletableFuture<Void>> op, String key) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        op.get().thenAccept(r -> result.complete(null)).exceptionally((ex) -> {
            if (ex.getCause() instanceof BadVersionException) {
                // if resource is updated by other than metadata-cache then metadata-cache will get bad-version
                // exception. so, try to invalidate the cache and try one more time.
                objCache.synchronous().invalidate(key);
                op.get().thenAccept((c) -> result.complete(null)).exceptionally((ex1) -> {
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
