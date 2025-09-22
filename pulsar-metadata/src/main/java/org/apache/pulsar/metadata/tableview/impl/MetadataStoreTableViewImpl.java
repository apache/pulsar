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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreTableView;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.jspecify.annotations.Nullable;

@Slf4j
public class MetadataStoreTableViewImpl<T> implements MetadataStoreTableView<T> {

    private static final int FILL_TIMEOUT_IN_MILLIS = 300_000;
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
    private final List<BiConsumer<String, T>> outdatedItemListeners;
    private final boolean clearUntrustedData;
    private final long timeoutInMillis;
    private final String pathPrefix;
    private final Consumer<Throwable> tableViewShutDownListener;

    @Deprecated
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
        this(clazz, name, store, pathPrefix, conflictResolver, listenPathValidator, tailItemListeners,
                existingItemListeners, null, false, timeoutInMillis, null);
    }

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
     * @param outdatedItemListeners Let's introduce how to ensure the correct element values: 1. The table
     *        view will try its best to ensure that the data is always the latest. When it cannot be guaranteed, see
     *        the next Article 2. If you get an old value, you will receive an update event later, ultimately ensuring
     *        the accuracy of the data. 3. You will receive this notification when the first two cannot be guaranteed
     *        due to the expiration of the metadata store session of the table view。After that, you also received
     *        notifications {@param tailItemListeners} and {@param existingItemListeners}, indicating that the table
     *        view can once again ensure that the first two can work properly.
     * @param clearUntrustedData clear the items that have received the event {@param itemStateOutdatedListeners}. Which
     *        means that rather that to get an untrusted value, to clear the untrusted values. You will not get a
     *        {@param tailItemListeners} for this change, which changed the value cached to null.
     * @param tableViewShutDownListener When the table view still cannot guarantee the real-time performance of the
     *        data after making its best efforts, you will receive this event. It is recommended to restart the table
     *        view at this time. This event will happen after {@param itemStateOutdatedListeners}, which mean it will be
     *        disabled if {@param itemStateOutdatedListeners} is null.
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
                                      @Nullable List<BiConsumer<String, T>> outdatedItemListeners,
                                      boolean clearUntrustedData,
                                      long timeoutInMillis,
                                      @Nullable Consumer<Throwable> tableViewShutDownListener) {
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
        this.outdatedItemListeners = new ArrayList<>();
        if (outdatedItemListeners != null) {
            this.outdatedItemListeners.addAll(outdatedItemListeners);
        }
        this.clearUntrustedData = clearUntrustedData;
        this.timeoutInMillis = timeoutInMillis;
        this.store = store;
        this.tableViewShutDownListener = tableViewShutDownListener;
        this.cache = store.getMetadataCache(clazz,
                MetadataCacheConfig.<T>builder()
                        .expireAfterWriteMillis(-1)
                        .refreshAfterWriteMillis(CACHE_REFRESH_FREQUENCY_IN_MILLIS)
                        .retryBackoff(MetadataCacheConfig.NO_RETRY_BACKOFF_BUILDER)
                        .asyncReloadConsumer(this::consumeAsyncReload)
                        .build());
        store.registerListener(this::handleNotification);
        if (store instanceof AbstractMetadataStore abstractMetadataStore) {
            abstractMetadataStore.registerSessionListener(this::handleSessionEvent);
        } else {
            // Since ServiceUnitStateMetadataStoreTableViewImpl has checked the configuration that named
            // "zookeeperSessionExpiredPolicy", skip to print the duplicated log here.
        }
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

    public void handleSessionEvent(SessionEvent sessionEvent) {
        if (CollectionUtils.isEmpty(outdatedItemListeners)) {
            log.warn("{} Skipped handle metadata store session event {} because does not set itemOutdatedListeners",
                name, sessionEvent);
            return;
        }
        if (sessionEvent == SessionEvent.SessionLost) {
            Map<String, T> snapshot = new HashMap<>(data);
            log.warn("{} clearing owned bundles because metadata store session lost {}",  name, snapshot);
            for (Map.Entry<String, T> entry : snapshot.entrySet()) {
                for (var listener : outdatedItemListeners) {
                    try {
                        listener.accept(entry.getKey(), entry.getValue());
                        if (clearUntrustedData) {
                            data.remove(entry.getKey());
                        }
                    } catch (Throwable e) {
                        if (tableViewShutDownListener == null) {
                            log.warn("{} failed to listen item whose state is unknown because of metadata store"
                                    + " session lost. key:{}, val:{}", name, entry.getKey(), entry.getValue(), e);
                        } else {
                            log.warn("{} Shutdown table view, because failed to listen item whose state is unknown due"
                                    + " to metadata store session lost. key:{}, val:{}", name, entry.getKey(),
                                    entry.getValue(), e);
                            tableViewShutDownListener.accept(e);
                        }
                        break;
                    }
                }
            }
        } else if (sessionEvent == SessionEvent.SessionReestablished) {
            log.info("{} Refilling bundle owner list after metadata store session reestablished",  name);
            // Since just get a session expired issue, we'd better to print the initialize detail logs.
            fillAsync(null, true).exceptionally(ex -> {
                if (tableViewShutDownListener == null) {
                    log.warn("{} failed to fill existing items after session reestablished", name, ex);
                } else {
                    log.error("{} Shutdown table view because failed to fill existing items after session"
                        + " reestablished", name, ex);
                    tableViewShutDownListener.accept(ex);
                }
                return null;
            });
        }
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

    /**
     * Note: this method only be called when a broker is starting, please call {@link #fillAsync(AtomicLong, boolean)}
     * for other use-case, otherwise, you may get a thread deadlock error.
     */
    private void fill() throws MetadataStoreException {
        AtomicLong loadedCounter = new AtomicLong();
        long maxWaitTime = Math.min(timeoutInMillis, FILL_TIMEOUT_IN_MILLIS);
        try {
            fillAsync(loadedCounter, false).get(maxWaitTime, TimeUnit.MILLISECONDS);
            log.info("{} completed filling existing items with size:{}", name, loadedCounter.get());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String err = name + " failed to fill existing items in "
                    + TimeUnit.MILLISECONDS.toSeconds(maxWaitTime) + " secs. Filled count:"
                    + loadedCounter.get();
            log.error(err);
            throw new MetadataStoreException(err, FutureUtil.unwrapCompletionException(e));
        }
    }

    private CompletableFuture<Void> handleExistingLeafs(String rootPath, String path, @Nullable AtomicLong count,
                                                        boolean printDetails) {
        return store.getChildren(path).thenCompose(children -> {
            if (children.isEmpty()) {
                // Skip root path.
                if (rootPath.equals(path)) {
                    return CompletableFuture.completedFuture(null);
                }
                // Leaf node.
                if (count != null) {
                    count.incrementAndGet();
                }
                if (printDetails) {
                    log.info("handling exist leaf {}", path);
                }
                return handleExisting(path);
            } else {
                // Dir node.
                // The limitation of max quantity of concurrency reading is not needed, because the metadata store
                // client works in a single thread, and all OPs will be packaged into a batch, the more the faster.
                // If metadata store receives too many requests, it will split them into multi requests itself.
                if (printDetails) {
                    log.info("handling exist dir {}", path);
                }
                List<CompletableFuture<Void>> futureList = new ArrayList<>();
                for (var child : children) {
                    futureList.add(handleExistingLeafs(rootPath, path + "/" + child, count, printDetails));
                }
                return FutureUtil.waitForAll(futureList);
            }
        });
    }

    /**
     * @param loadedCounter a metric value, to know how many value has been loaded, which is an internal value.
     */
    private CompletableFuture<Void> fillAsync(@Nullable AtomicLong loadedCounter, boolean printDetails) {
        return handleExistingLeafs(pathPrefix, pathPrefix, loadedCounter, printDetails);
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

        var cached = cache.getIfCached(getPath(key));
        // Added this logging to print warn any discrepancy between cache and tableview
        var val = immutableData.get(key);
        if (!Objects.equals(cached.orElse(null), val)) {
            log.warn("cache and tableview are out of sync on item key={} value={} cached={}. Prolonged inconsistency "
                    + "may require broker restart to mitigate the issue.", key, val, cached);
        }
        return val;
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
        }).thenCompose(__ -> doHandleNotification(path)) // immediately notify local tableview
        .exceptionally(e -> {
            if (e.getCause() instanceof MetadataStoreException.BadVersionException) {
                throw FutureUtil.wrapToCompletionException(new ConflictException(
                        String.format("Failed to update to value:%s", value)));
            }

            throw FutureUtil.wrapToCompletionException(e.getCause());
        });
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
