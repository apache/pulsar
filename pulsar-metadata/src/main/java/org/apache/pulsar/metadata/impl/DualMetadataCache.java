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
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.extended.CreateOption;

public class DualMetadataCache<T> implements MetadataCache<T> {
    private final DualMetadataStore dualMetadataStore;
    private final Class<T> clazz;
    private final TypeReference<T> typeRef;
    private final String cacheName;
    private final MetadataSerde<T> serde;
    private final MetadataCacheConfig cacheConfig;

    private final AtomicReference<MetadataCache<T>> metadataCache = new AtomicReference<>();

    public DualMetadataCache(DualMetadataStore dualMetadataStore, Class<T> clazz, TypeReference<T> typeRef,
                             String cacheName, MetadataSerde<T> serde,
                             MetadataCacheConfig cacheConfig) {
        this.dualMetadataStore = dualMetadataStore;
        this.clazz = clazz;
        this.typeRef = typeRef;
        this.cacheName = cacheName;
        this.serde = serde;
        this.cacheConfig = cacheConfig;

        var store = dualMetadataStore.targetStore != null
                ? dualMetadataStore.targetStore : dualMetadataStore.sourceStore;

        if (clazz != null) {
            this.metadataCache.set(store.getMetadataCache(clazz, cacheConfig));
        } else if (typeRef != null) {
            this.metadataCache.set(store.getMetadataCache(typeRef, cacheConfig));
        } else {
            this.metadataCache.set(store.getMetadataCache(cacheName, serde, cacheConfig));
        }
    }

    @Override
    public CompletableFuture<Optional<T>> get(String path) {
        return metadataCache.get().get(path);
    }

    @Override
    public CompletableFuture<Optional<CacheGetResult<T>>> getWithStats(String path) {
        return metadataCache.get().getWithStats(path);
    }

    @Override
    public Optional<T> getIfCached(String path) {
        return metadataCache.get().getIfCached(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        return metadataCache.get().getChildren(path);
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return metadataCache.get().exists(path);
    }

    @Override
    public CompletableFuture<T> readModifyUpdateOrCreate(String path, Function<Optional<T>, T> modifyFunction) {
        return metadataCache.get().readModifyUpdateOrCreate(path, modifyFunction);
    }

    @Override
    public CompletableFuture<T> readModifyUpdate(String path, Function<T, T> modifyFunction) {
        return metadataCache.get().readModifyUpdate(path, modifyFunction);
    }

    @Override
    public CompletableFuture<Void> create(String path, T value) {
        return metadataCache.get().create(path, value);
    }

    @Override
    public CompletableFuture<Void> put(String path, T value, EnumSet<CreateOption> options) {
        return metadataCache.get().put(path, value, options);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return metadataCache.get().delete(path);
    }

    @Override
    public void invalidate(String path) {
        metadataCache.get().invalidate(path);
    }

    @Override
    public void invalidateAll() {
        metadataCache.get().invalidateAll();
    }

    @Override
    public void refresh(String path) {
        metadataCache.get().refresh(path);
    }

    void handleSwitchToTargetStore() {
            if (clazz != null) {
                metadataCache.set(dualMetadataStore.targetStore.getMetadataCache(clazz, cacheConfig));
            } else if (typeRef != null) {
                metadataCache.set(dualMetadataStore.targetStore.getMetadataCache(typeRef, cacheConfig));
            } else {
                metadataCache.set(dualMetadataStore.targetStore.getMetadataCache(cacheName, serde, cacheConfig));
            }
    }
}
