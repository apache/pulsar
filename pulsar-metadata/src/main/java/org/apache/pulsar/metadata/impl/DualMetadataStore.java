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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreLifecycle;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

/**
 * Wrapper around a metadata store that provides transparent migration capability.
 *
 * <p>When migration is not active, all operations are forwarded to the source store.
 * When migration starts (detected via flag in source store), this wrapper:
 * <ul>
 *   <li>Initializes connection to target store</li>
 *   <li>Recreates ephemeral nodes in target store</li>
 *   <li>Routes reads/writes based on migration phase</li>
 * </ul>
 */
@Slf4j
public class DualMetadataStore implements MetadataStoreExtended {

    @Getter
    final MetadataStoreExtended sourceStore;
    volatile MetadataStoreExtended targetStore = null;

    private volatile MigrationState migrationState = MigrationState.NOT_STARTED;

    private final MetadataStoreConfig config;
    private String participantId;
    private final Set<String> localEphemeralPaths = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService executor;

    private final MetadataCache<MigrationState> migrationStateCache;

    private final Set<Consumer<Notification>> listeners = ConcurrentHashMap.newKeySet();
    private final Set<Consumer<SessionEvent>> sessionListeners = ConcurrentHashMap.newKeySet();

    private final AtomicInteger pendingSourceWrites = new AtomicInteger();

    private final Set<DualMetadataCache<?>> caches = ConcurrentHashMap.newKeySet();

    private static final IllegalStateException READ_ONLY_STATE_EXCEPTION =
            new IllegalStateException("Write operations not allowed during migrations");

    public DualMetadataStore(MetadataStore sourceStore, MetadataStoreConfig config) throws MetadataStoreException {
        this.sourceStore = (MetadataStoreExtended) sourceStore;
        this.config = config;
        this.executor = new ScheduledThreadPoolExecutor(1,
                new DefaultThreadFactory("pulsar-dual-metadata-store", true));
        this.migrationStateCache = sourceStore.getMetadataCache(MigrationState.class);

        if (sourceStore instanceof MetadataStoreLifecycle msl) {
            msl.initializeCluster();
        }

        readCurrentState();
        registerAsParticipant();

        // Watch for migration events
        watchForMigrationEvents();
    }

    private void readCurrentState() throws MetadataStoreException {
        try {
            // Read the current state
            var initialState = migrationStateCache.get(MigrationState.MIGRATION_FLAG_PATH).get();
            initialState.ifPresent(state -> this.migrationState = state);

            if (migrationState.getPhase() == MigrationPhase.COMPLETED) {
                initializeTargetStore(migrationState.getTargetUrl());
            }

        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    private void registerAsParticipant() throws MetadataStoreException {
        try {
            // Register ourselves as participant in an eventual migration
            Stat stat = this.sourceStore.put(MigrationState.PARTICIPANTS_PATH + "/id-", new byte[0],
                    Optional.empty(), EnumSet.of(CreateOption.Sequential, CreateOption.Ephemeral)).get();
            participantId = stat.getPath();
            log.info("Participant metadata store created: {}", participantId);
        } catch (Throwable e) {
            throw new MetadataStoreException(e);
        }
    }

    private void watchForMigrationEvents() {
        // Register listener for migration-related paths
        sourceStore.registerListener(notification -> {
            if (!MigrationState.MIGRATION_FLAG_PATH.equals(notification.getPath())) {
                return;
            }

            migrationStateCache.get(MigrationState.MIGRATION_FLAG_PATH)
                    .thenAccept(migrationState -> {
                        this.migrationState = migrationState.orElse(MigrationState.NOT_STARTED);

                        switch (this.migrationState.getPhase()) {
                            case PREPARATION -> executor.execute(this::handleMigrationStart);
                            case COMPLETED -> executor.execute(this::handleMigrationComplete);
                            case FAILED -> executor.execute(this::handleMigrationFailed);
                            default -> {
                                // no-op
                            }
                        }
                    });
        });
    }

    private void handleMigrationStart() {
        try {
            log.info("=== Starting Metadata Migration Preparation ===");
            log.info("Target metadata store URL: {}", migrationState.getTargetUrl());

            // Mark the session as lost so that all the component will avoid trying to make metadata writes
            // for anything that can be deferred (eg: ledgers rollovers)
            sessionListeners.forEach(listener -> listener.accept(SessionEvent.SessionLost));

            // Initialize target store
            initializeTargetStore(migrationState.getTargetUrl());

            this.recreateEphemeralNodesInTarget();

            // Acknowledge preparation by deleting the participant id
            sourceStore.delete(participantId, Optional.empty()).get();

            log.info("=== Migration Preparation Complete ===");

        } catch (Exception e) {
            log.error("Failed during migration preparation", e);
        }
    }

    private void handleMigrationComplete() {
        log.info("=== Metadata Migration Complete ===");

        caches.forEach(DualMetadataCache::handleSwitchToTargetStore);
        listeners.forEach(targetStore::registerListener);
        sessionListeners.forEach(targetStore::registerSessionListener);

        sessionListeners.forEach(listener -> listener.accept(SessionEvent.SessionReestablished));
    }

    private void handleMigrationFailed() {
        log.info("=== Metadata Migration Failed ===");
        sessionListeners.forEach(listener -> listener.accept(SessionEvent.SessionReestablished));
    }


    private synchronized void initializeTargetStore(String targetUrl) throws MetadataStoreException {
        if (this.targetStore != null) {
            return;
        }

        log.info("Initializing target metadata store: {}", targetUrl);
        this.targetStore = (MetadataStoreExtended) MetadataStoreFactoryImpl.create(
                targetUrl,
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(config.getSessionTimeoutMillis())
                        .batchingEnabled(config.isBatchingEnabled())
                        .batchingMaxDelayMillis(config.getBatchingMaxDelayMillis())
                        .batchingMaxOperations(config.getBatchingMaxOperations())
                        .batchingMaxSizeKb(config.getBatchingMaxSizeKb())
                        .build()
        );

        log.info("Target store initialized successfully");
    }

    private void recreateEphemeralNodesInTarget() throws Exception {
        log.info("Found {} local ephemeral nodes to recreate", localEphemeralPaths.size());
        var futures = localEphemeralPaths.stream()
                .map(path ->
                        sourceStore.get(path)
                                .thenCompose(ogr ->
                                        ogr.map(gr -> targetStore.put(path, gr.getValue(), Optional.empty(),
                                                        EnumSet.of(CreateOption.Ephemeral)))
                                                .orElse(
                                                        CompletableFuture.completedFuture(null))
                                )
                ).toList();

        FutureUtil.waitForAll(futures).get();
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        return switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> sourceStore.get(path);
            case COMPLETED -> targetStore.get(path);
        };
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        return switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> sourceStore.getChildren(path);
            case COMPLETED -> targetStore.getChildren(path);
        };
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        return switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> sourceStore.getChildrenFromStore(path);
            case COMPLETED -> targetStore.getChildrenFromStore(path);
        };
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> sourceStore.exists(path);
            case COMPLETED -> targetStore.exists(path);
        };
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, expectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
                                       EnumSet<CreateOption> options) {
        switch (migrationState.getPhase()) {
            case NOT_STARTED, FAILED -> {
                // Track ephemeral nodes
                if (options.contains(CreateOption.Ephemeral)) {
                    localEphemeralPaths.add(path);
                }

                // Track pending writes
                pendingSourceWrites.incrementAndGet();
                var future = sourceStore.put(path, value, expectedVersion, options);
                future.whenComplete((result, e) -> pendingSourceWrites.decrementAndGet());
                return future;
            }

            case PREPARATION, COPYING -> {
                return CompletableFuture.failedFuture(READ_ONLY_STATE_EXCEPTION);
            }

            case COMPLETED -> {
                return targetStore.put(path, value, expectedVersion, options);
            }

            default -> throw new IllegalStateException("Invalid phase " + migrationState.getPhase());
        }
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        switch (migrationState.getPhase()) {
            case NOT_STARTED, FAILED -> {
                localEphemeralPaths.remove(path);

                pendingSourceWrites.incrementAndGet();
                var future = sourceStore.delete(path, expectedVersion);
                future.whenComplete((result, e) -> pendingSourceWrites.decrementAndGet());
                return future;
            }

            case PREPARATION, COPYING -> {
                return CompletableFuture.failedFuture(READ_ONLY_STATE_EXCEPTION);
            }

            case COMPLETED -> {
                return targetStore.delete(path, expectedVersion);
            }

            default -> throw new IllegalStateException("Invalid phase " + migrationState.getPhase());
        }
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path) {
        switch (migrationState.getPhase()) {
            case NOT_STARTED, FAILED -> {
                pendingSourceWrites.incrementAndGet();
                var future = sourceStore.deleteRecursive(path);
                future.whenComplete((result, e) -> pendingSourceWrites.decrementAndGet());
                return future;
            }
            case PREPARATION, COPYING -> {
                return CompletableFuture.failedFuture(READ_ONLY_STATE_EXCEPTION);
            }
            case COMPLETED -> {
                return targetStore.deleteRecursive(path);
            }

            default -> throw new IllegalStateException("Invalid phase " + migrationState.getPhase());
        }
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> {
                listeners.add(listener);
                sourceStore.registerListener(listener);
            }

            case COMPLETED -> targetStore.registerListener(listener);
        }
    }

    @Override
    public void registerSessionListener(Consumer<SessionEvent> listener) {
        switch (migrationState.getPhase()) {
            case NOT_STARTED, PREPARATION, COPYING, FAILED -> {
                sessionListeners.add(listener);
                sourceStore.registerSessionListener(listener);
            }

            case COMPLETED -> targetStore.registerSessionListener(listener);
        }
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(Class<T> clazz, MetadataCacheConfig cacheConfig) {
        var cache =  new DualMetadataCache<>(this, clazz, null, null, null, cacheConfig);
        caches.add(cache);
        return cache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef, MetadataCacheConfig cacheConfig) {
        var cache =  new DualMetadataCache<>(this, null, typeRef, null, null, cacheConfig);
        caches.add(cache);
        return cache;
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(String cacheName, MetadataSerde<T> serde,
                                                 MetadataCacheConfig cacheConfig) {
        var cache =  new DualMetadataCache<>(this, null, null, cacheName, serde, cacheConfig);
        caches.add(cache);
        return cache;
    }

    @Override
    public Optional<MetadataEventSynchronizer> getMetadataEventSynchronizer() {
        return sourceStore.getMetadataEventSynchronizer();
    }

    @Override
    public void updateMetadataEventSynchronizer(MetadataEventSynchronizer synchronizer) {
        sourceStore.updateMetadataEventSynchronizer(synchronizer);
    }

    @Override
    public CompletableFuture<Void> handleMetadataEvent(MetadataEvent event) {
        return sourceStore.handleMetadataEvent(event);
    }

    @Override
    public void close() throws Exception {
        log.info("Closing DualMetadataStore");

        // Close target store first (if exists)
        if (targetStore != null) {
            try {
                targetStore.close();
                log.info("Target store closed");
            } catch (Exception e) {
                log.error("Error closing target store", e);
            }
        }

        // Close source store
        try {
            sourceStore.close();
            log.info("Source store closed");
        } catch (Exception e) {
            log.error("Error closing source store", e);
        }

        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
