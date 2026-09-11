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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.jspecify.annotations.NonNull;

/**
 * Defines ServiceUnitTableViewSyncer.
 * It syncs service unit(bundle) states between metadata store and system topic table views.
 * One could enable this syncer before migration from one to the other and disable it after the migration finishes.
 */
@CustomLog
public class ServiceUnitStateTableViewSyncer implements Closeable {
    private static final int MAX_CONCURRENT_SYNC_COUNT = 100;
    private static final int SYNC_WAIT_TIME_IN_SECS = 300;
    private static final long RECONCILE_INTERVAL_IN_MILLIS = 5_000;
    private static final BiConsumer<String, ServiceUnitStateData> NOOP_CONSUMER = (__, ___) -> {
    };
    private volatile int syncWaitTimeInSecs = SYNC_WAIT_TIME_IN_SECS;
    private PulsarService pulsar;
    private volatile ServiceUnitStateTableView systemTopicTableView;
    private volatile ServiceUnitStateTableView metadataStoreTableView;
    private volatile boolean isActive = false;
    private final ObjectWriter jsonWriter = ObjectMapperFactory.getMapper().writer();


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
            log.error().exception(e).log("Failed to start ServiceUnitStateTableViewSyncer");
            throw e;
        }
    }

    private CompletableFuture<Void> syncToSystemTopic(String key, ServiceUnitStateData data) {
        return logIfFailed(sync(systemTopicTableView, key, data), key, data, "systemTopic");
    }

    private CompletableFuture<Void> syncToMetadataStore(String key, ServiceUnitStateData data) {
        return logIfFailed(sync(metadataStoreTableView, key, data), key, data, "metadataStore");
    }

    private CompletableFuture<Void> sync(ServiceUnitStateTableView dst, String key, ServiceUnitStateData data) {
        // A null tail item is a tombstone: the source view removed the key. Route it to
        // delete() rather than put(): the metadata-store view's put() rejects null
        // (@NonNull) and the system-topic view's delete() is itself a null-valued put(),
        // so a uniform delete keeps both sync directions symmetric and prevents a missed
        // deletion from leaving the two views with different sizes (which would make
        // waitUntilSynced spin until the timeout budget).
        return data == null ? dst.delete(key) : dst.put(key, data);
    }

    private CompletableFuture<Void> logIfFailed(CompletableFuture<Void> future, String key,
                                                ServiceUnitStateData data, String dst) {
        return future.whenComplete((__, e) -> {
            if (e != null && !(e instanceof PulsarClientException.AlreadyClosedException)) {
                log.warn().attr("dst", dst).attr("serviceUnit", key).attr("data", data).exception(e)
                        .log("Failed to sync tableview item; sizes may diverge until the next update");
            }
        });
    }

    private void syncExistingItems()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        long started = System.currentTimeMillis();

        @Cleanup
        ServiceUnitStateTableView metadataStoreTableView = new ServiceUnitStateMetadataStoreTableViewImpl();
        metadataStoreTableView.start(
                pulsar,
                NOOP_CONSUMER,
                NOOP_CONSUMER,
                NOOP_CONSUMER
        );

        @Cleanup
        ServiceUnitStateTableView systemTopicTableView = new ServiceUnitStateTableViewImpl();
        systemTopicTableView.start(
                pulsar,
                NOOP_CONSUMER,
                NOOP_CONSUMER,
                NOOP_CONSUMER
        );


        var syncer = pulsar.getConfiguration().getLoadBalancerServiceUnitTableViewSyncer();
        ServiceUnitStateTableView src;
        ServiceUnitStateTableView dst;
        if (syncer == SystemTopicToMetadataStoreSyncer) {
            clean(metadataStoreTableView);
            syncExistingItemsToMetadataStore(systemTopicTableView);
            src = systemTopicTableView;
            dst = metadataStoreTableView;
        } else {
            clean(systemTopicTableView);
            syncExistingItemsToSystemTopic(metadataStoreTableView, systemTopicTableView);
            src = metadataStoreTableView;
            dst = systemTopicTableView;
        }

        if (!waitUntilSynced(src, dst, started)) {
            throw new TimeoutException(
                    syncer + " failed to sync existing items in tableviews. MetadataStoreTableView.size: "
                            + metadataStoreTableView.entrySet().size()
                            + ", SystemTopicTableView.size: " + systemTopicTableView.entrySet().size() + " in "
                            + syncWaitTimeInSecs + " secs");
        }

        log.info().attr("metadataStoreTableViewSize", metadataStoreTableView.entrySet().size())
                .attr("systemTopicTableViewSize", systemTopicTableView.entrySet().size())
                .attr("elapsedSecs", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started))
                .log("Synced existing items");
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
                NOOP_CONSUMER,
                NOOP_CONSUMER
        );
        log.info("Started MetadataStoreTableView");

        this.systemTopicTableView = new ServiceUnitStateTableViewImpl();
        this.systemTopicTableView.start(
                pulsar,
                this::syncToMetadataStore,
                NOOP_CONSUMER,
                NOOP_CONSUMER
        );
        log.info("Started SystemTopicTableView");

        var syncer = pulsar.getConfiguration().getLoadBalancerServiceUnitTableViewSyncer();
        var src = syncer == SystemTopicToMetadataStoreSyncer ? systemTopicTableView : metadataStoreTableView;
        var dst = syncer == SystemTopicToMetadataStoreSyncer ? metadataStoreTableView : systemTopicTableView;
        if (!waitUntilSynced(src, dst, started)) {
            throw new TimeoutException(
                    syncer + " failed to sync tableviews. MetadataStoreTableView.size: "
                            + metadataStoreTableView.entrySet().size()
                            + ", SystemTopicTableView.size: " + systemTopicTableView.entrySet().size() + " in "
                            + syncWaitTimeInSecs + " secs");
        }


        log.info().attr("metadataStoreTableViewSize", metadataStoreTableView.entrySet().size())
                .attr("systemTopicTableViewSize", systemTopicTableView.entrySet().size())
                .attr("elapsedSecs", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started))
                .log("Successfully started ServiceUnitStateTableViewSyncer");
    }

    private void syncExistingItemsToMetadataStore(ServiceUnitStateTableView src)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        // Directly use store to sync existing items to metadataStoreTableView(otherwise, they are conflicted out)
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        var srcIter = src.entrySet().iterator();
        while (srcIter.hasNext()) {
            var e = srcIter.next();
            futures.add(writeToMetadataStore(e.getKey(), e.getValue()));
            maybeWaitCompletion(futures, !srcIter.hasNext());
        }
    }

    private void maybeWaitCompletion(List<CompletableFuture<Void>> futures, boolean forceWait)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!futures.isEmpty() && (futures.size() == MAX_CONCURRENT_SYNC_COUNT || forceWait)) {
            FutureUtil.waitForAll(futures)
                    .get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            futures.clear();
        }
    }

    private @NonNull CompletableFuture<Void> writeToMetadataStore(String key, ServiceUnitStateData value)
            throws JsonProcessingException {
        return pulsar.getLocalMetadataStore().put(ServiceUnitStateMetadataStoreTableViewImpl.PATH_PREFIX + "/" + key,
                jsonWriter.writeValueAsBytes(value), Optional.empty()).thenApply(__ -> null);
    }

    private void syncExistingItemsToSystemTopic(ServiceUnitStateTableView src,
                                                ServiceUnitStateTableView dst)
            throws ExecutionException, InterruptedException, TimeoutException {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        var srcIter = src.entrySet().iterator();
        while (srcIter.hasNext()) {
            var e = srcIter.next();
            futures.add(dst.put(e.getKey(), e.getValue()));
            maybeWaitCompletion(futures, !srcIter.hasNext());
        }
    }

    private void clean(ServiceUnitStateTableView dst)
            throws ExecutionException, InterruptedException, TimeoutException {
        var dstIter = dst.entrySet().iterator();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        while (dstIter.hasNext()) {
            var e = dstIter.next();
            futures.add(dst.delete(e.getKey()));
            maybeWaitCompletion(futures, !dstIter.hasNext());
        }
    }

    private boolean waitUntilSynced(ServiceUnitStateTableView src, ServiceUnitStateTableView dst, long started)
            throws InterruptedException {
        long lastReconciled = started;
        while (src.entrySet().size() != dst.entrySet().size()) {
            long now = System.currentTimeMillis();
            if (TimeUnit.MILLISECONDS.toSeconds(now - started) > syncWaitTimeInSecs) {
                return false;
            }
            // Give in-flight syncs a grace period to settle on their own, then reconcile
            // periodically: updates that raced with the table views' (re)start were replayed
            // to the fresh views as existing items — which are deliberately not wired to
            // sync — so without reconciliation the views would never converge.
            if (now - lastReconciled >= RECONCILE_INTERVAL_IN_MILLIS) {
                log.debug().attr("srcSize", src.entrySet().size()).attr("dstSize", dst.entrySet().size())
                        .attr("elapsedSecs", TimeUnit.MILLISECONDS.toSeconds(now - started))
                        .log("Tableviews not synced yet; reconciling");
                reconcile(src, dst, started);
                lastReconciled = now;
            }
            Thread.sleep(100);
        }
        return true;
    }

    /**
     * Copies items the destination table view is missing and removes stale items that no longer
     * exist in the source. Channel updates that land between the existing-items copy and the
     * registration of the tail listeners are only visible as existing items of the freshly
     * started views, so the tail listeners never see them. Writes flow to the migration source
     * while the syncer starts, making the source view authoritative; destination-only items are
     * removed only when they predate this sync phase and are still absent from the source, so a
     * concurrent fresh write to the destination is never discarded. Failures are logged and left
     * for the next reconcile pass. Runs on the caller's (load manager) thread with each batch
     * bounded by the metadata store operation timeout.
     */
    private void reconcile(ServiceUnitStateTableView src, ServiceUnitStateTableView dst, long started)
            throws InterruptedException {
        // Snapshot the destination entries before iterating the source so that a key arriving
        // in the destination through a concurrent tail sync cannot be misclassified as stale.
        var staleDstItems = new HashMap<String, ServiceUnitStateData>();
        for (var e : dst.entrySet()) {
            staleDstItems.put(e.getKey(), e.getValue());
        }
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (var e : src.entrySet()) {
                if (staleDstItems.remove(e.getKey()) == null) {
                    log.info().attr("serviceUnit", e.getKey())
                            .log("Reconciling item missing from the destination tableview");
                    if (dst.isMetadataStoreBased()) {
                        // Write directly to the store like syncExistingItemsToMetadataStore
                        // does; the view's put() would conflict the item out.
                        futures.add(writeToMetadataStore(e.getKey(), e.getValue()));
                    } else {
                        futures.add(dst.put(e.getKey(), e.getValue()));
                    }
                    maybeWaitCompletion(futures, false);
                }
            }
            for (var e : staleDstItems.entrySet()) {
                // Only remove items written before this sync phase began and re-confirmed absent
                // from the source: a fresh destination write (e.g. from a broker already switched
                // to the destination implementation) is propagated to the source by the tail
                // listener instead of being deleted here.
                if (e.getValue().timestamp() < started && src.get(e.getKey()) == null) {
                    log.info().attr("serviceUnit", e.getKey())
                            .log("Reconciling stale item in the destination tableview");
                    futures.add(dst.delete(e.getKey()));
                    maybeWaitCompletion(futures, false);
                }
            }
            maybeWaitCompletion(futures, true);
        } catch (IOException | ExecutionException | TimeoutException e) {
            // Transient write failures leave a size divergence behind; the next reconcile pass
            // (or the sync-wait timeout) handles it.
            log.warn().exception(e).log("Failed to reconcile tableview items");
        }
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
            log.error().exception(e).log("Failed to close SystemTopicTableView");
            throw e;
        }

        try {
            if (metadataStoreTableView != null) {
                metadataStoreTableView.close();
                metadataStoreTableView = null;
                log.info("Closed MetadataStoreTableView");
            }
        } catch (Exception e) {
            log.error().exception(e).log("Failed to close MetadataStoreTableView");
            throw e;
        }

        log.info("Successfully closed ServiceUnitStateTableViewSyncer.");
        isActive = false;
    }

    public boolean isActive() {
        return isActive;
    }

    @VisibleForTesting
    public void setSyncWaitTimeInSecs(int syncWaitTimeInSecs) {
        this.syncWaitTimeInSecs = syncWaitTimeInSecs;
    }
}
