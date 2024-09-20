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

package org.apache.pulsar.metadata.tableview.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreTableView;
import org.apache.pulsar.metadata.api.NotificationType;

@Slf4j
public class MetadataStoreTableViewImpl<T> implements MetadataStoreTableView<T> {

    private static final int FILL_TIMEOUT_IN_MILLIS = 300_000;
    private static final int MAX_CONCURRENT_METADATA_OPS_DURING_FILL = 50;
    private static final long CACHE_REFRESH_FREQUENCY_IN_MILLIS = 600_000;
    private final ConcurrentMap<String, T> data;
    private final Map<String, T> immutableData;
    private final String name;
    private final MetadataStore store;
    private final MetadataCache<T> cache;
    private final Predicate<String> listenPathValidator;
    private final BiPredicate<T, T> conflictResolver;
    private final List<BiConsumer<String, T>> tailItemListeners;
    private final List<BiConsumer<String, T>> existingItemListeners;
    private final long timeoutInMillis;
    private final String pathPrefix;

    /**
     * Construct MetadataStoreTableViewImpl.
     *
     * @param clazz                 clazz of the value type
     * @param name                  metadata store tableview name
     * @param store                 metadata store
     * @param pathPrefix            metadata store path prefix
     * @param listenPathValidator   path validator to listen
     * @param conflictResolver      resolve conflicts for concurrent puts
     * @param tailItemListeners     listener for tail item(recently updated) notifications
     * @param existingItemListeners listener for existing items in metadata store
     * @param timeoutInMillis       timeout duration for each sync operation.
     * @throws MetadataStoreException if init fails.
     */
    @Builder
    public MetadataStoreTableViewImpl(@NonNull Class<T> clazz,
                                      @NonNull String name,
                                      @NonNull MetadataStore store,
                                      @NonNull String pathPrefix,
                                      @NonNull BiPredicate<T, T> conflictResolver,
                                      Predicate<String> listenPathValidator,
                                      List<BiConsumer<String, T>> tailItemListeners,
                                      List<BiConsumer<String, T>> existingItemListeners,
                                      long timeoutInMillis) {
        this.name = name;
        this.data = new ConcurrentHashMap<>();
        this.immutableData = Collections.unmodifiableMap(data);
        this.pathPrefix = pathPrefix;
        this.conflictResolver = conflictResolver;
        this.listenPathValidator = listenPathValidator;
        this.tailItemListeners = new ArrayList<>();
        if (tailItemListeners != null) {
            this.tailItemListeners.addAll(tailItemListeners);
        }
        this.existingItemListeners = new ArrayList<>();
        if (existingItemListeners != null) {
            this.existingItemListeners.addAll(existingItemListeners);
        }
        this.timeoutInMillis = timeoutInMillis;
        this.store = store;
        this.cache = store.getMetadataCache(clazz,
                MetadataCacheConfig.<T>builder()
                        .expireAfterWriteMillis(-1)
                        .refreshAfterWriteMillis(CACHE_REFRESH_FREQUENCY_IN_MILLIS)
                        .asyncReloadConsumer(this::consumeAsyncReload)
                        .build());
        store.registerListener(this::handleNotification);
    }

    public void start() throws MetadataStoreException {
        fill();
    }

    private void consumeAsyncReload(String path, Optional<CacheGetResult<T>> cached) {
        if (!isValidPath(path)) {
            return;
        }
        String key = getKey(path);
        var val = getValue(cached);
        handleTailItem(key, val);
    }

    private boolean isValidPath(String path) {
        if (listenPathValidator != null && !listenPathValidator.test(path)) {
            return false;
        }
        return true;
    }

    private T getValue(Optional<CacheGetResult<T>> cached) {
        return cached.map(CacheGetResult::getValue).orElse(null);
    }

    boolean updateData(String key, T cur) {
        MutableBoolean updated = new MutableBoolean();
        data.compute(key, (k, prev) -> {
            if (Objects.equals(prev, cur)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} skipped item key={} value={} prev={}",
                            name, key, cur, prev);
                }
                updated.setValue(false);
                return prev;
            } else {
                updated.setValue(true);
                return cur;
            }
        });
        return updated.booleanValue();
    }

    private void handleTailItem(String key, T val) {
        if (updateData(key, val)) {
            if (log.isDebugEnabled()) {
                log.debug("{} applying item key={} value={}",
                        name,
                        key,
                        val);
            }
            for (var listener : tailItemListeners) {
                try {
                    listener.accept(key, val);
                } catch (Throwable e) {
                    log.error("{} failed to listen tail item key:{}, val:{}",
                            name,
                            key, val, e);
                }
            }
        }

    }

    private CompletableFuture<Void> doHandleNotification(String path) {
        if (!isValidPath(path)) {
            return CompletableFuture.completedFuture(null);
        }
        return cache.get(path).thenAccept(valOpt -> {
            String key = getKey(path);
            var val = valOpt.orElse(null);
            handleTailItem(key, val);
        }).exceptionally(e -> {
            log.error("{} failed to handle notification for path:{}", name, path, e);
            return null;
        });
    }

    private void handleNotification(org.apache.pulsar.metadata.api.Notification notification) {

        if (notification.getType() == NotificationType.ChildrenChanged) {
            return;
        }

        String path = notification.getPath();

        doHandleNotification(path);
    }


    private CompletableFuture<Void> handleExisting(String path) {
        if (!isValidPath(path)) {
            return CompletableFuture.completedFuture(null);
        }
        return cache.get(path)
                .thenAccept(valOpt -> {
                    valOpt.ifPresent(val -> {
                        String key = getKey(path);
                        updateData(key, val);
                        if (log.isDebugEnabled()) {
                            log.debug("{} applying existing item key={} value={}",
                                    name,
                                    key,
                                    val);
                        }
                        for (var listener : existingItemListeners) {
                            try {
                                listener.accept(key, val);
                            } catch (Throwable e) {
                                log.error("{} failed to listen existing item key:{}, val:{}", name, key, val,
                                        e);
                                throw e;
                            }
                        }
                    });
                });
    }

    private void fill() throws MetadataStoreException {
        final var deadline = System.currentTimeMillis() + FILL_TIMEOUT_IN_MILLIS;
        log.info("{} start filling existing items under the pathPrefix:{}", name, pathPrefix);
        ConcurrentLinkedDeque<String> q = new ConcurrentLinkedDeque<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        q.add(pathPrefix);
        LongAdder count = new LongAdder();
        while (!q.isEmpty()) {
            var now = System.currentTimeMillis();
            if (now >= deadline) {
                String err = name + " failed to fill existing items in "
                        + TimeUnit.MILLISECONDS.toSeconds(FILL_TIMEOUT_IN_MILLIS) + " secs. Filled count:"
                        + count.sum();
                log.error(err);
                throw new MetadataStoreException(err);
            }
            int size = Math.min(MAX_CONCURRENT_METADATA_OPS_DURING_FILL, q.size());
            for (int i = 0; i < size; i++) {
                String path = q.poll();
                futures.add(store.getChildren(path)
                        .thenCompose(children -> {
                            // The path is leaf
                            if (children.isEmpty()) {
                                count.increment();
                                return handleExisting(path);
                            } else {
                                for (var child : children) {
                                    q.add(path + "/" + child);
                                }
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
            }
            try {
                FutureUtil.waitForAll(futures).get(
                        Math.min(timeoutInMillis, deadline - now),
                        TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                Throwable c = FutureUtil.unwrapCompletionException(e);
                log.error("{} failed to fill existing items", name, c);
                throw new MetadataStoreException(c);
            }
            futures.clear();
        }
        log.info("{} completed filling existing items with size:{}", name, count.sum());
    }


    private String getPath(String key) {
        return pathPrefix + "/" + key;
    }

    private String getKey(String path) {
        return path.replaceFirst(pathPrefix + "/", "");
    }

    public boolean exists(String key) {
        return immutableData.containsKey(key);
    }

    public T get(String key) {
        return data.get(key);
    }

    public CompletableFuture<Void> put(String key, T value) {
        String path = getPath(key);
        return cache.readModifyUpdateOrCreate(path, (old) -> {
            if (conflictResolver.test(old.orElse(null), value)) {
                return value;
            } else {
                throw new ConflictException(
                        String.format("Failed to update from old:%s to value:%s", old, value));
            }
        }).thenCompose(__ -> doHandleNotification(path)); // immediately notify local tableview
    }

    public CompletableFuture<Void> delete(String key) {
        String path = getPath(key);
        return cache.delete(path)
                .thenCompose(__ -> doHandleNotification(path)); // immediately notify local tableview
    }

    public int size() {
        return immutableData.size();
    }

    public boolean isEmpty() {
        return immutableData.isEmpty();
    }

    public Set<Map.Entry<String, T>> entrySet() {
        return immutableData.entrySet();
    }

    public Set<String> keySet() {
        return immutableData.keySet();
    }

    public Collection<T> values() {
        return immutableData.values();
    }

    public void forEach(BiConsumer<String, T> action) {
        immutableData.forEach(action);
    }

}
