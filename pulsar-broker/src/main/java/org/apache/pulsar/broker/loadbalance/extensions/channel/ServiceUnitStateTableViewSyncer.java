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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Defines ServiceUnitTableViewSyncer.
 * It syncs service unit(bundle) states between metadata store and system topic table views.
 * One could enable this syncer before migration from one to the other and disable it after the migration finishes.
 */
@Slf4j
public class ServiceUnitStateTableViewSyncer implements Closeable {
    private static final int MAX_CONCURRENT_SYNC_COUNT = 100;
    private volatile ServiceUnitStateTableView systemTopicTableView;
    private volatile ServiceUnitStateTableView metadataStoreTableView;
    private volatile boolean isActive = false;

    public void start(PulsarService pulsar)
            throws IOException, TimeoutException, InterruptedException, ExecutionException {
        if (!pulsar.getConfiguration().isLoadBalancerServiceUnitTableViewSyncerEnabled()) {
            return;
        }

        if (isActive) {
            return;
        }

        try {
            long started = System.currentTimeMillis();

            if (metadataStoreTableView != null) {
                metadataStoreTableView.close();
                metadataStoreTableView = null;
            }
            metadataStoreTableView = new ServiceUnitStateMetadataStoreTableViewImpl();
            metadataStoreTableView.start(
                    pulsar,
                    this::syncToSystemTopic,
                    (k, v) -> {}
            );
            log.info("Started MetadataStoreTableView");

            if (systemTopicTableView != null) {
                systemTopicTableView.close();
                systemTopicTableView = null;
            }
            systemTopicTableView = new ServiceUnitStateTableViewImpl();
            systemTopicTableView.start(
                    pulsar,
                    this::syncToMetadataStore,
                    (k, v) -> {}
            );
            log.info("Started SystemTopicTableView");

            Map<String, ServiceUnitStateData> merged = new HashMap<>();
            metadataStoreTableView.entrySet().forEach(e -> merged.put(e.getKey(), e.getValue()));
            systemTopicTableView.entrySet().forEach(e -> merged.put(e.getKey(), e.getValue()));

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            var opTimeout = pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds();

            // Directly use store to sync existing items to metadataStoreTableView(otherwise, they are conflicted out)
            var store = pulsar.getLocalMetadataStore();
            var writer = ObjectMapperFactory.getMapper().writer();
            for (var e : merged.entrySet()) {
                futures.add(store.put(ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + e.getKey(),
                        writer.writeValueAsBytes(e.getValue()), Optional.empty()).thenApply(__ -> null));
                if (futures.size() == MAX_CONCURRENT_SYNC_COUNT) {
                    FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
                    futures.clear();
                }
            }
            FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
            futures.clear();

            for (var e : merged.entrySet()) {
                futures.add(syncToSystemTopic(e.getKey(), e.getValue()));
                if (futures.size() == MAX_CONCURRENT_SYNC_COUNT) {
                    FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
                    futures.clear();
                }
            }
            FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
            futures.clear();

            int size = merged.size();
            int syncTimeout = opTimeout * size;
            while (metadataStoreTableView.entrySet().size() != size || systemTopicTableView.entrySet().size() != size) {
                if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started) > syncTimeout) {
                    throw new TimeoutException(
                            "Failed to sync tableviews. MetadataStoreTableView.size: "
                                    + metadataStoreTableView.entrySet().size()
                                    + ", SystemTopicTableView.size: " + systemTopicTableView.entrySet().size() + " in "
                                    + syncTimeout + " secs");
                }
                Thread.sleep(100);
            }

            log.info("Successfully started ServiceUnitStateTableViewSyncer MetadataStoreTableView.size:{} , "
                            + "SystemTopicTableView.size: {} in {} secs",
                    metadataStoreTableView.entrySet().size(), systemTopicTableView.entrySet().size(),
                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started));
            isActive = true;

        } catch (Throwable e) {
            log.error("Failed to start ServiceUnitStateTableViewSyncer", e);
            throw e;
        }
    }

    private CompletableFuture<Void> syncToSystemTopic(String key, ServiceUnitStateData data) {
        return systemTopicTableView.put(key, data);
    }

    private CompletableFuture<Void>  syncToMetadataStore(String key, ServiceUnitStateData data) {
        return metadataStoreTableView.put(key, data);
    }

    @Override
    public void close() throws IOException {
        if (!isActive) {
            return;
        }

        try {
            if (systemTopicTableView != null) {
                systemTopicTableView.close();
                systemTopicTableView = null;
                log.info("Closed SystemTopicTableView");
            }
        } catch (Exception e) {
            log.error("Failed to close SystemTopicTableView", e);
            throw e;
        }

        try {
            if (metadataStoreTableView != null) {
                metadataStoreTableView.close();
                metadataStoreTableView = null;
                log.info("Closed MetadataStoreTableView");
            }
        } catch (Exception e) {
            log.error("Failed to close MetadataStoreTableView", e);
            throw e;
        }

        log.info("Successfully closed ServiceUnitStateTableViewSyncer.");
        isActive = false;
    }

    public boolean isActive() {
        return isActive;
    }
}
