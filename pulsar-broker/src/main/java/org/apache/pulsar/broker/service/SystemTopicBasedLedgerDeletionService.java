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
package org.apache.pulsar.broker.service;

import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.deletion.LedgerComponent;
import org.apache.bookkeeper.mledger.deletion.LedgerDeletionService;
import org.apache.bookkeeper.mledger.deletion.LedgerType;
import org.apache.bookkeeper.mledger.deletion.PendingDeleteLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.broker.systopic.LedgerDeletionSystemTopicClient;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemTopicBasedLedgerDeletionService implements LedgerDeletionService {

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedLedgerDeletionService.class);

    private final PulsarAdmin pulsarAdmin;

    private final int ledgerDeletionParallelism;

    private final ServiceConfiguration serviceConfiguration;

    private final Map<OffloadPoliciesImpl, LedgerOffloader> offloaderMap = new ConcurrentHashMap<>();

    private StatsProvider statsProvider = new NullStatsProvider();

    private OpStatsLogger deleteLedgerOpLogger;

    private OpStatsLogger deleteOffloadLedgerOpLogger;

    private final LoadingCache<String, TreeSet<Long>> ledgersCache;

    private final BookKeeper bookKeeper;

    private final PulsarService pulsarService;

    private SystemTopicClient<PendingDeleteLedgerInfo> ledgerDeletionTopicClient;

    private transient CompletableFuture<SystemTopicClient.Reader<PendingDeleteLedgerInfo>> readerFuture;

    private transient CompletableFuture<SystemTopicClient.Writer<PendingDeleteLedgerInfo>> writerFuture;

    public SystemTopicBasedLedgerDeletionService(PulsarService pulsarService, ServiceConfiguration serviceConfiguration)
            throws PulsarServerException {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
        this.pulsarService = pulsarService;
        this.pulsarAdmin = pulsarService.getAdminClient();
        this.bookKeeper = pulsarService.getBookKeeperClient();
        this.serviceConfiguration = serviceConfiguration;
        this.ledgerDeletionParallelism = serviceConfiguration.getLedgerDeletionParallelismOfTwoPhaseDeletion();
        this.ledgersCache = Caffeine.newBuilder()
                .expireAfterWrite(serviceConfiguration.getReconsumeLaterSecondsOfTwoPhaseDeletion(), TimeUnit.SECONDS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable TreeSet<Long> load(@NonNull String key) throws Exception {
                        return fetchInUseLedgerIds(key);
                    }
                });
    }

    private SystemTopicClient<PendingDeleteLedgerInfo> getLedgerDeletionTopicClient() throws PulsarClientException {
        TopicName ledgerDeletionSystemTopic =
                NamespaceEventsSystemTopicFactory.getSystemTopicName(NamespaceName.SYSTEM_NAMESPACE,
                        EventType.LEDGER_DELETION);
        if (ledgerDeletionSystemTopic == null) {
            throw new PulsarClientException.InvalidTopicNameException(
                    "Can't create SystemTopicBasedLedgerDeletionService, " + "because the topicName is null!");
        }
        return namespaceEventsSystemTopicFactory.createLedgerDeletionSystemTopicClient(ledgerDeletionSystemTopic,
                serviceConfiguration.getSendDelaySecondsOfTwoPhaseDeletion(),
                serviceConfiguration.getReconsumeLaterSecondsOfTwoPhaseDeletion());
    }

    @Override
    public void start() throws PulsarClientException, PulsarAdminException {
        this.ledgerDeletionTopicClient = getLedgerDeletionTopicClient();
        initStatsLogger();
        initLedgerDeletionSystemTopic();
    }

    private void initStatsLogger() {
        Configuration configuration = new ClientConfiguration();
        if (serviceConfiguration.isBookkeeperClientExposeStatsToPrometheus()) {
            configuration.addProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                    serviceConfiguration.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
            configuration.addProperty(PrometheusMetricsProvider.CLUSTER_NAME, serviceConfiguration.getClusterName());
            this.statsProvider = new PrometheusMetricsProvider();
        }
        this.statsProvider.start(configuration);
        StatsLogger statsLogger = statsProvider.getStatsLogger("pulsar_ledger_deletion");
        this.deleteLedgerOpLogger = statsLogger.getOpStatsLogger("delete_ledger");
        this.deleteOffloadLedgerOpLogger = statsLogger.getOpStatsLogger("delete_offload_ledger");
    }

    private CompletableFuture<Void> initSystemTopic(String topicName, int partition) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        doCreateSystemTopic(future, topicName, partition);
        return future;
    }

    private void doCreateSystemTopic(CompletableFuture<Void> future, String topicName, int partition) {
        this.pulsarAdmin.topics()
                .createPartitionedTopicAsync(topicName, partition).whenComplete((res, e) -> {
                    if (e != null && !(e instanceof PulsarAdminException.ConflictException)) {
                        log.error("Initial system topic " + topicName + "failed.", e);
                        doCreateSystemTopic(future, topicName, partition);
                        return;
                    }
                    future.complete(null);
                });
    }

    private void initLedgerDeletionSystemTopic() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(initSystemTopic(SystemTopicNames.LEDGER_DELETION_TOPIC.getPartitionedTopicName(),
                this.ledgerDeletionParallelism));
        futures.add(initSystemTopic(SystemTopicNames.LEDGER_DELETION_RETRY_TOPIC.getPartitionedTopicName(), 1));
        futures.add(initSystemTopic(SystemTopicNames.LEDGER_DELETION_DLQ_TOPIC.getPartitionedTopicName(), 1));

        FutureUtil.waitForAll(futures).whenComplete((res, e) -> {
            initReaderFuture();
            initWriterFuture();
        });
    }

    private void readMorePendingDeleteLedger(LedgerDeletionSystemTopicClient.PendingDeleteLedgerReader reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                deleteLedger(reader, msg).exceptionally(e -> {
                    log.warn("Delete ledger {} failed.", msg, e);
                    return null;
                });
                readMorePendingDeleteLedger(reader);
            } else {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more pending delete ledger exception, close the read now!", ex);
                    reader.closeAsync();
                    initReaderFuture();
                } else {
                    log.warn("Read more pending delete ledger exception, read again.", ex);
                    readMorePendingDeleteLedger(reader);
                }
            }
        });
    }

    private void initReaderFuture() {
        this.readerFuture = ledgerDeletionTopicClient.newReaderAsync().whenComplete((reader, ex) -> {
            if (ex != null) {
                log.error("Failed to create reader on ledger deletion system topic", ex);
                initReaderFuture();
            } else {
                readMorePendingDeleteLedger((LedgerDeletionSystemTopicClient.PendingDeleteLedgerReader) reader);
            }
        });
    }

    private void initWriterFuture() {
        this.writerFuture = ledgerDeletionTopicClient.newWriterAsync().whenComplete((writer, ex) -> {
            if (ex != null) {
                log.error("Failed to create writer on ledger deletion system topic", ex);
                initWriterFuture();
            }
        });
    }

    private String tuneTopicName(String topicName) {
        if (topicName.contains("/" + TopicDomain.persistent.value())) {
            return topicName.replaceFirst("/" + TopicDomain.persistent.value(), "");
        }
        return topicName;
    }

    @Override
    public CompletableFuture<?> appendPendingDeleteLedger(String topicName, long ledgerId, LedgerInfo context,
                                                          LedgerComponent component, LedgerType type,
                                                          boolean checkLedgerStillInUse) {
        topicName = tuneTopicName(topicName);
        PendingDeleteLedgerInfo pendingDeleteLedger = null;
        if (LedgerType.LEDGER == type) {
            pendingDeleteLedger =
                    new PendingDeleteLedgerInfo(topicName, component, type, ledgerId, context, checkLedgerStillInUse);
        } else if (LedgerType.OFFLOAD_LEDGER == type) {
            if (!context.getOffloadContext().hasUidMsb()) {
                CompletableFuture<?> future = new CompletableFuture<>();
                future.completeExceptionally(
                        new IllegalArgumentException("The ledger " + ledgerId + " didn't offload."));
                return future;
            }
            pendingDeleteLedger =
                    new PendingDeleteLedgerInfo(topicName, component, type, ledgerId, context, checkLedgerStillInUse);
        }
        return sendMessage(pendingDeleteLedger);
    }

    @Override
    public void close() throws Exception {
        asyncClose().get();
    }

    @Override
    public CompletableFuture<?> asyncClose() {
        if (readerFuture != null && !readerFuture.isCompletedExceptionally()) {
            return readerFuture.thenCompose(SystemTopicClient.Reader::closeAsync);
        }
        ledgersCache.invalidateAll();
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<?> deleteLedger(LedgerDeletionSystemTopicClient.PendingDeleteLedgerReader reader,
                                              Message<PendingDeleteLedgerInfo> message) {
        PendingDeleteLedgerInfo pendingDeleteLedger = message.getValue();
        if (isToDeleteLedger(pendingDeleteLedger)) {
            if (LedgerType.LEDGER == pendingDeleteLedger.getLedgerType()) {
                return asyncDeleteLedger(pendingDeleteLedger.getTopicName(),
                        pendingDeleteLedger.getLedgerId()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message).exceptionally(ex -> {
                            log.warn("Ack pending delete ledger {} failed.", message, ex);
                            return null;
                        });
                        return;
                    }
                    reader.reconsumeLaterAsync(message).exceptionally(ex -> {
                        log.warn("Reconsume pending delete ledger {} later failed.", message, ex);
                        return null;
                    });
                });
            } else if (LedgerType.OFFLOAD_LEDGER == pendingDeleteLedger.getLedgerType()) {
                return asyncDeleteOffloadedLedger(pendingDeleteLedger.getTopicName(),
                        pendingDeleteLedger.getContext()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message).exceptionally(ex -> {
                            log.warn("Ack pending delete ledger {} failed.", message, ex);
                            return null;
                        });
                        return;
                    }
                    reader.reconsumeLaterAsync(message).exceptionally(ex -> {
                        log.warn("Reconsume pending delete ledger {} later failed.", message, ex);
                        return null;
                    });
                });
            }
            reader.ackMessageAsync(message).exceptionally(ex -> {
                log.warn("Ack pending delete ledger {} failed.", message, ex);
                return null;
            });
            return FutureUtil.failedFuture(
                    new InvalidParameterException("Received pending delete ledger message with invalid ledger type."));
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] ledger {} still in use, delete it later.", pendingDeleteLedger.getTopicName(),
                    pendingDeleteLedger.getLedgerId());
        }
        return reader.reconsumeLaterAsync(message);
    }

    private boolean isToDeleteLedger(PendingDeleteLedgerInfo pendingDeleteLedger) {
        if (!pendingDeleteLedger.isCheckLedgerStillInUse()) {
            return true;
        }
        TreeSet<Long> ledgerIds;
        try {
            ledgerIds = getLedgerIds(pendingDeleteLedger);
        } catch (PulsarAdminException e) {
            log.error("Fetch metadata for {} failed.", pendingDeleteLedger.getTopicName(), e);
            return false;
        }
        //means the current cache is out of date.
        if (pendingDeleteLedger.getLedgerId() > ledgerIds.last()) {
            return false;
        }
        return !ledgerIds.contains(pendingDeleteLedger.getLedgerId());
    }

    private TreeSet<Long> getLedgerIds(PendingDeleteLedgerInfo pendingDeleteLedger) throws PulsarAdminException {
        return ledgersCache.get(pendingDeleteLedger.getTopicName());
    }

    private TreeSet<Long> fetchInUseLedgerIds(String topicName) throws PulsarAdminException {
        TreeSet<Long> ledgerIds = new TreeSet<>();

        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(topicName);

        ledgerIds.addAll(internalStats.ledgers.stream().map(ele1 -> ele1.ledgerId).collect(Collectors.toSet()));
        ledgerIds.addAll(
                internalStats.cursors.values().stream().map(ele -> ele.cursorLedger).collect(Collectors.toSet()));
        ledgerIds.addAll(internalStats.schemaLedgers.stream().map(ele -> ele.ledgerId).collect(Collectors.toSet()));

        return ledgerIds;
    }

    private CompletableFuture<?> sendMessage(PendingDeleteLedgerInfo pendingDeleteLedger) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (writerFuture == null) {
            result.completeExceptionally(new IllegalStateException("Writer not initialize"));
        }
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                writer.writeAsync(pendingDeleteLedger).whenComplete(((messageId, e) -> {
                            if (e != null) {
                                result.completeExceptionally(e);
                            } else {
                                if (messageId != null) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(new RuntimeException("Got message id is null."));
                                }
                            }
                        })
                );
            }
        });
        return result;
    }

    private CompletableFuture<?> asyncDeleteLedger(String topicName, long ledgerId) {
        final long startTime = MathUtils.nowInNano();
        CompletableFuture<?> future = new CompletableFuture<>();
        log.info("[{}] Start async delete ledger {}", topicName, ledgerId);
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", topicName, ledgerId);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error delete ledger {} : {}", topicName, ledgerId, BKException.getMessage(rc));
                deleteLedgerOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                future.completeExceptionally(ManagedLedgerImpl.createManagedLedgerException(rc));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleted ledger {}", topicName, ledgerId);
            }
            deleteLedgerOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            future.complete(null);
        }, null);
        return future;
    }

    private CompletableFuture<?> asyncDeleteOffloadedLedger(String topicName, LedgerInfo ledgerInfo) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        final long startTime = MathUtils.nowInNano();

        OffloadPoliciesImpl offloadPolicies = buildOffloadPolicies(ledgerInfo);

        LedgerOffloader ledgerOffloader = offloaderMap.get(offloadPolicies);
        if (ledgerOffloader == null) {
            synchronized (this) {
                ledgerOffloader = offloaderMap.get(offloadPolicies);
                if (ledgerOffloader == null) {
                    try {
                        ledgerOffloader = pulsarService.createManagedLedgerOffloader(offloadPolicies);
                        offloaderMap.put(offloadPolicies, ledgerOffloader);
                    } catch (PulsarServerException e) {
                        deleteOffloadLedgerOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime),
                                TimeUnit.NANOSECONDS);
                        future.completeExceptionally(new IllegalStateException("Failed to create ledger offload", e));
                        return future;
                    }
                }
            }
        }

        UUID uuid = new UUID(ledgerInfo.getOffloadContext().getUidMsb(), ledgerInfo.getOffloadContext().getUidLsb());
        Long ledgerId = ledgerInfo.getLedgerId();
        log.info("[{}] Start async delete offloaded ledger, ledgerId {} uuid {}.", topicName, ledgerId, uuid);

        Map<String, String> metadataMap = Maps.newHashMap();
        Map<String, String> metadata = OffloadUtils.getOffloadDriverMetadata(ledgerInfo, Collections.emptyMap());
        metadataMap.putAll(metadata);
        metadataMap.put("ManagedLedgerName", topicName);
        try {
            ledgerOffloader.deleteOffloaded(ledgerId, uuid, metadataMap).whenComplete((ignored, exception) -> {
                if (exception != null) {
                    deleteOffloadLedgerOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime),
                            TimeUnit.NANOSECONDS);
                    log.warn("[{}] Failed delete offload for ledgerId {} uuid {}.)",
                            topicName, ledgerId, uuid, exception);
                    future.completeExceptionally(new ManagedLedgerException("Failed to delete offloaded ledger"));
                    return;
                }
                deleteOffloadLedgerOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime),
                        TimeUnit.NANOSECONDS);
                future.complete(null);
            });
        } catch (Exception e) {
            log.warn("[{}] Failed to delete offloaded ledgers.", topicName, e);
        }
        return future;
    }

    private OffloadPoliciesImpl buildOffloadPolicies(LedgerInfo ledgerInfo) {
        String driverName = OffloadUtils.getOffloadDriverName(ledgerInfo, "");
        if (FILE_SYSTEM_DRIVER.equals(driverName)) {
            // TODO: 2022/7/13 Filesystem offloader params needs more info
            return null;
        } else {
            Map<String, String> metadata = OffloadUtils.getOffloadDriverMetadata(ledgerInfo, Collections.emptyMap());
            String bucket = metadata.get(METADATA_FIELD_BUCKET);
            String region = metadata.get(METADATA_FIELD_REGION);
            String endpoint = metadata.get(METADATA_FIELD_ENDPOINT);
            return OffloadPoliciesImpl.create(driverName, bucket, region, endpoint);
        }
    }

    //TieredStorageConfiguration.METADATA_FIELD_BUCKET
    private static final String METADATA_FIELD_BUCKET = "bucket";
    //TieredStorageConfiguration.METADATA_FIELD_REGION
    private static final String METADATA_FIELD_REGION = "region";
    //TieredStorageConfiguration.METADATA_FIELD_ENDPOINT
    private static final String METADATA_FIELD_ENDPOINT = "serviceEndpoint";
    //OffloadPoliciesImpl.DRIVER_NAMES[3]
    private static final String FILE_SYSTEM_DRIVER = "filesystem";


}
