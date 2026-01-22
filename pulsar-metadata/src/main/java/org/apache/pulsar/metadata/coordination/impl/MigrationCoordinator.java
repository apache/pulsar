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
package org.apache.pulsar.metadata.coordination.impl;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.options.defs.OptionOverrideModificationsCount;
import io.oxia.client.api.options.defs.OptionOverrideVersionId;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStoreProvider;

/**
 * Coordinates metadata store migration process.
 */
@Slf4j
public class MigrationCoordinator {
    private final MetadataStore sourceStore;
    private final String targetUrl;
    private final AsyncOxiaClient oxiaClient;
    private final MetadataCache<MigrationState> migrationStateCache;

    private static final int MAX_PENDING_OPS = 1000;

    public MigrationCoordinator(MetadataStore sourceStore, String targetUrl) throws MetadataStoreException {
        this.sourceStore = sourceStore;
        this.targetUrl = targetUrl;
        this.migrationStateCache = sourceStore.getMetadataCache(MigrationState.class);

        if (!targetUrl.startsWith("oxia://")) {
            throw new MetadataStoreException("Expected target metadata store to be Oxia");
        }

        this.oxiaClient = new OxiaMetadataStoreProvider().getOxiaClient(targetUrl);
    }

    /**
     * Start the migration process.
     *
     * @throws Exception if migration fails
     */
    public void startMigration() throws Exception {
        log.info("=== Starting Migration ===");
        log.info("Source: {} (current)", sourceStore.getClass().getSimpleName());
        log.info("Target: {}", targetUrl);

        try {
            // 1. Create migration flag
            setInitialMigrationPhase();

            // 2. Wait for participants to prepare
            waitForPreparation();

            // 3. Copy persistent data
            updatePhase(MigrationPhase.COPYING);
            copyPersistentData();

            // 4. Set state to completed
            updatePhase(MigrationPhase.COMPLETED);

            log.info("=== Migration Complete ===");
        } catch (Exception e) {
            log.error("Migration failed", e);
            updatePhase(MigrationPhase.FAILED);
            throw e;
        }
    }

    private void setInitialMigrationPhase() throws MetadataStoreException {
        try {
            sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                    ObjectMapperFactory.getMapper().writer()
                            .writeValueAsBytes(new MigrationState(MigrationPhase.PREPARATION, targetUrl)),
                    Optional.of(-1L)).get();
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    private void updatePhase(MigrationPhase phase) throws MetadataStoreException {
        try {
            migrationStateCache.put(MigrationState.MIGRATION_FLAG_PATH,
                    new MigrationState(phase, targetUrl), EnumSet.noneOf(CreateOption.class)).get();
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    private void waitForPreparation() throws Exception {
        log.info("Waiting for all participants to prepare...");

        while (true) {
            List<String> pending = sourceStore.getChildren(MigrationState.PARTICIPANTS_PATH).get();
            if (pending.isEmpty()) {
                break;
            }

            log.info("Waiting for participants to prepare. pending: {}", pending);
            Thread.sleep(1000);
        }

        log.info("All migration participants ready");
    }

    private void copyPersistentData() throws Exception {
        log.info("Starting persistent data copy...");

        AtomicLong copiedCount = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(MAX_PENDING_OPS);
        AtomicReference<Throwable> exception = new AtomicReference<>();

        // Bootstrap first level
        BlockingQueue<String> workQueue = new LinkedBlockingQueue<>(getChildren("/").get());

        while (true) {
            String path = workQueue.poll(1, TimeUnit.SECONDS);
            if (path == null) {
                // Wait until all pending ops are done
                if (semaphore.availablePermits() != MAX_PENDING_OPS) {
                    continue;
                } else {
                    break;
                }
            }

            semaphore.acquire();
            copy(path).whenComplete((res, e) -> {
                semaphore.release();
                if (e != null) {
                    exception.compareAndSet(null, e);
                }

                copiedCount.incrementAndGet();
            });

            semaphore.acquire();
            getChildren(path).whenComplete((res, e) -> {
                if (e != null) {
                    exception.compareAndSet(null, e);
                }

                workQueue.addAll(res);
                semaphore.release();
            });

            if (exception.get() != null) {
                break;
            }
        }

        if (exception.get() != null) {
            throw new Exception(exception.get());
        }

        log.info("All data copied. total records={}", copiedCount.get());
    }

    private CompletableFuture<List<String>> getChildren(String parent) {
        return sourceStore.getChildren(parent)
                .thenApply(list -> list.stream().map(x -> {
                    if ("/".equals(parent)) {
                        return "/" + x;
                    } else {
                        return parent + "/" + x;
                    }
                }).toList());
    }

    private CompletableFuture<Void> copy(String path) {
        return sourceStore.get(path)
                .thenCompose(ogr -> {
                    if (ogr.isPresent()) {
                        var gr = ogr.get();
                        if (gr.getStat().isEphemeral()) {
                            // Ignore ephemeral at this point
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return oxiaClient.put(path, gr.getValue(),
                                    Set.of(new OptionOverrideVersionId(gr.getStat().getVersion()),
                                            new OptionOverrideModificationsCount(gr.getStat().getVersion())
                                    )
                            ).thenRun(() -> log.debug("--- Copied {}", path));
                        }
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).thenApply(x -> null);
    }
}
