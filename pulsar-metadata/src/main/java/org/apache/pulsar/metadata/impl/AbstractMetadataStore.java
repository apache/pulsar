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

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.cache.MetadataCache;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;

@Slf4j
public abstract class AbstractMetadataStore implements MetadataStore, Consumer<Notification> {

    private static final long CACHE_REFRESH_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final CopyOnWriteArrayList<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();
    protected final ExecutorService executor;
    private final AsyncLoadingCache<String, List<String>> childrenCache;
    private final AsyncLoadingCache<String, Boolean> existsCache;
    private final CopyOnWriteArrayList<MetadataCacheImpl<?>> metadataCaches = new CopyOnWriteArrayList<>();

    protected abstract CompletableFuture<List<String>> getChildrenFromStore(String path);

    protected abstract CompletableFuture<Boolean> existsFromStore(String path);

    protected AbstractMetadataStore() {
        this.executor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("metadata-store"));
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
                        return getChildrenFromStore(key);
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
                        return existsFromStore(key);
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
    public final CompletableFuture<List<String>> getChildren(String path) {
        return childrenCache.get(path);
    }

    @Override
    public final CompletableFuture<Boolean> exists(String path) {
        return existsCache.get(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        listeners.add(listener);
    }

    protected void receivedNotification(Notification notification) {
        executor.execute(() -> {
            listeners.forEach(listener -> {
                try {
                    listener.accept(notification);
                } catch (Throwable t) {
                    log.error("Failed to process metadata store notification", t);
                }
            });
        });
    }

    @Override
    public void accept(Notification n) {
        String path = n.getPath();
        NotificationType type = n.getType();

        if (type == NotificationType.Created || type == NotificationType.Deleted) {
            existsCache.synchronous().invalidate(path);
        }

        if (type == NotificationType.ChildrenChanged) {
            childrenCache.synchronous().invalidate(path);
        }

        if (type == NotificationType.Created || type == NotificationType.Deleted || type == NotificationType.Modified) {
            metadataCaches.forEach(c -> c.accept(n));
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
