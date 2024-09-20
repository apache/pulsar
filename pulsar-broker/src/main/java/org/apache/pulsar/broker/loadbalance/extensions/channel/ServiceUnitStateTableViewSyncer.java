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

import static org.apache.pulsar.broker.ServiceConfiguration.ServiceUnitTableViewSyncerType.SystemTopicToMetadataStoreSyncer;
import static org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl.COMPACTION_THRESHOLD;
import static org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl.configureSystemTopics;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
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
    private static final int SYNC_WAIT_TIME_IN_SECS = 300;
    private PulsarService pulsar;
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
        this.pulsar = pulsar;

        try {

            syncExistingItems();
            // disable compaction
            if (!configureSystemTopics(pulsar, 0)) {
                throw new IllegalStateException("Failed to disable compaction");
            }
            syncTailItems();

            isActive = true;

        } catch (Throwable e) {
            log.error("Failed to start ServiceUnitStateTableViewSyncer", e);
            throw e;
        }
    }

    private CompletableFuture<Void> syncToSystemTopic(String key, ServiceUnitStateData data) {
        return systemTopicTableView.put(key, data);
    }

    private CompletableFuture<Void> syncToMetadataStore(String key, ServiceUnitStateData data) {
        return metadataStoreTableView.put(key, data);
    }

    private void dummy(String key, ServiceUnitStateData data) {
    }

    private void syncExistingItems()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        long started = System.currentTimeMillis();
        @Cleanup
        ServiceUnitStateTableView metadataStoreTableView = new ServiceUnitStateMetadataStoreTableViewImpl();
        metadataStoreTableView.start(
                pulsar,
                this::dummy,
                this::dummy
        );

        @Cleanup
        ServiceUnitStateTableView systemTopicTableView = new ServiceUnitStateTableViewImpl();
        systemTopicTableView.start(
                pulsar,
                this::dummy,
                this::dummy
        );


        var syncer = pulsar.getConfiguration().getLoadBalancerServiceUnitTableViewSyncer();
        if (syncer == SystemTopicToMetadataStoreSyncer) {
            clean(metadataStoreTableView);
            syncExistingItemsToMetadataStore(systemTopicTableView);
        } else {
            clean(systemTopicTableView);
            syncExistingItemsToSystemTopic(metadataStoreTableView, systemTopicTableView);
        }

        if (!waitUntilSynced(metadataStoreTableView, systemTopicTableView, started)) {
            throw new TimeoutException(
                    syncer + " failed to sync existing items in tableviews. MetadataStoreTableView.size: "
                            + metadataStoreTableView.entrySet().size()
                            + ", SystemTopicTableView.size: " + systemTopicTableView.entrySet().size() + " in "
                            + SYNC_WAIT_TIME_IN_SECS + " secs");
        }

        log.info("Synced existing items MetadataStoreTableView.size:{} , "
                        + "SystemTopicTableView.size: {} in {} secs",
                metadataStoreTableView.entrySet().size(), systemTopicTableView.entrySet().size(),
                TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started));
    }

    private void syncTailItems() throws InterruptedException, IOException, TimeoutException {
        long started = System.currentTimeMillis();

        if (metadataStoreTableView != null) {
            metadataStoreTableView.close();
            metadataStoreTableView = null;
        }

        if (systemTopicTableView != null) {
            systemTopicTableView.close();
            systemTopicTableView = null;
        }

        this.metadataStoreTableView = new ServiceUnitStateMetadataStoreTableViewImpl();
        this.metadataStoreTableView.start(
                pulsar,
                this::syncToSystemTopic,
                this::dummy
        );
        log.info("Started MetadataStoreTableView");

        this.systemTopicTableView = new ServiceUnitStateTableViewImpl();
        this.systemTopicTableView.start(
                pulsar,
                this::syncToMetadataStore,
                this::dummy
        );
        log.info("Started SystemTopicTableView");

        var syncer = pulsar.getConfiguration().getLoadBalancerServiceUnitTableViewSyncer();
        if (!waitUntilSynced(metadataStoreTableView, systemTopicTableView, started)) {
            throw new TimeoutException(
                    syncer + " failed to sync tableviews. MetadataStoreTableView.size: "
                            + metadataStoreTableView.entrySet().size()
                            + ", SystemTopicTableView.size: " + systemTopicTableView.entrySet().size() + " in "
                            + SYNC_WAIT_TIME_IN_SECS + " secs");
        }


        log.info("Successfully started ServiceUnitStateTableViewSyncer MetadataStoreTableView.size:{} , "
                        + "SystemTopicTableView.size: {} in {} secs",
                metadataStoreTableView.entrySet().size(), systemTopicTableView.entrySet().size(),
                TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started));
    }

    private void syncExistingItemsToMetadataStore(ServiceUnitStateTableView src)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        // Directly use store to sync existing items to metadataStoreTableView(otherwise, they are conflicted out)
        var store = pulsar.getLocalMetadataStore();
        var writer = ObjectMapperFactory.getMapper().writer();
        var opTimeout = pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        var srcIter = src.entrySet().iterator();
        while (srcIter.hasNext()) {
            var e = srcIter.next();
            futures.add(store.put(ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + e.getKey(),
                    writer.writeValueAsBytes(e.getValue()), Optional.empty()).thenApply(__ -> null));
            if (futures.size() == MAX_CONCURRENT_SYNC_COUNT || !srcIter.hasNext()) {
                FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
            }
        }
    }

    private void syncExistingItemsToSystemTopic(ServiceUnitStateTableView src,
                                                ServiceUnitStateTableView dst)
            throws ExecutionException, InterruptedException, TimeoutException {
        var opTimeout = pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        var srcIter = src.entrySet().iterator();
        while (srcIter.hasNext()) {
            var e = srcIter.next();
            futures.add(dst.put(e.getKey(), e.getValue()));
            if (futures.size() == MAX_CONCURRENT_SYNC_COUNT || !srcIter.hasNext()) {
                FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
            }
        }
    }

    private void clean(ServiceUnitStateTableView dst)
            throws ExecutionException, InterruptedException, TimeoutException {
        var opTimeout = pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        var dstIter = dst.entrySet().iterator();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        while (dstIter.hasNext()) {
            var e = dstIter.next();
            futures.add(dst.delete(e.getKey()));
            if (futures.size() == MAX_CONCURRENT_SYNC_COUNT || !dstIter.hasNext()) {
                FutureUtil.waitForAll(futures).get(opTimeout, TimeUnit.SECONDS);
            }
        }
    }

    private boolean waitUntilSynced(ServiceUnitStateTableView srt, ServiceUnitStateTableView dst, long started)
            throws InterruptedException {
        while (srt.entrySet().size() != dst.entrySet().size()) {
            if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started)
                    > SYNC_WAIT_TIME_IN_SECS) {
                return false;
            }
            Thread.sleep(100);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (!isActive) {
            return;
        }

        if (!configureSystemTopics(pulsar, COMPACTION_THRESHOLD)) {
            throw new IllegalStateException("Failed to enable compaction");
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
