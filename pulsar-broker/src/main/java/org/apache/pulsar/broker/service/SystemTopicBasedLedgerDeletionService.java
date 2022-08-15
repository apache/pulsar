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
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.deletion.LedgerComponent;
import org.apache.bookkeeper.mledger.deletion.LedgerDeletionService;
import org.apache.bookkeeper.mledger.deletion.LedgerType;
import org.apache.bookkeeper.mledger.deletion.PendingDeleteLedgerInfo;
import org.apache.bookkeeper.mledger.deletion.PendingDeleteLedgerInvalidException;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
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
import org.apache.pulsar.common.protocol.topic.DeleteLedgerPayload;
import org.apache.pulsar.common.util.FutureUtil;
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
        this.ledgerDeletionParallelism = serviceConfiguration.getLedgerDeletionParallelismOfTopicTwoPhaseDeletion();
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
                serviceConfiguration.getSendDelayOfTopicTwoPhaseDeletionInSeconds(),
                serviceConfiguration.getReconsumeLaterOfTopicTwoPhaseDeletionInSeconds());
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
                handleMessage(reader, msg).exceptionally(e -> {
                    log.warn("Delete ledger {} failed.", msg.getValue(), e);
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
                                                          LedgerComponent component, LedgerType type) {
        topicName = tuneTopicName(topicName);
        PendingDeleteLedgerInfo pendingDeleteLedger = null;
        if (LedgerType.LEDGER == type) {
            pendingDeleteLedger =
                    new PendingDeleteLedgerInfo(topicName, component, type, ledgerId, context);
        } else if (LedgerType.OFFLOAD_LEDGER == type) {
            if (!context.getOffloadContext().hasUidMsb()) {
                CompletableFuture<?> future = new CompletableFuture<>();
                future.completeExceptionally(
                        new IllegalArgumentException("The ledger " + ledgerId + " didn't offload."));
                return future;
            }
            pendingDeleteLedger =
                    new PendingDeleteLedgerInfo(topicName, component, type, ledgerId, context);
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
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<?> handleMessage(LedgerDeletionSystemTopicClient.PendingDeleteLedgerReader reader,
                                               Message<PendingDeleteLedgerInfo> message) {
        CompletableFuture<?> future = new CompletableFuture<>();
        deleteInBroker(message).whenComplete((res, ex) -> {
            if (ex != null) {
                if (ex instanceof PulsarAdminException.NotFoundException) {
                    deleteLocally(message).whenComplete((res1, ex1) -> {
                        if (ex1 != null) {
                            if (ex1 instanceof PendingDeleteLedgerInvalidException) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Received invalid pending delete ledger {}, invalid reason: {}",
                                            message.getValue().getLedgerId(), ex1.getMessage());
                                }
                                reader.ackMessageAsync(message);
                                future.complete(null);
                                return;
                            }
                            reader.reconsumeLaterAsync(message);
                            future.completeExceptionally(ex1);
                        }
                        reader.ackMessageAsync(message);
                        future.complete(null);
                    });
                } else if (ex instanceof PendingDeleteLedgerInvalidException) {
                    if (log.isDebugEnabled()) {
                        log.debug("Received invalid pending delete ledger {}, invalid reason: {}",
                                message.getValue().getLedgerId(), ex.getMessage());
                    }
                    reader.ackMessageAsync(message);
                    future.complete(null);
                } else if (ex instanceof PulsarAdminException.ServerSideErrorException) {

                } else {
                    reader.reconsumeLaterAsync(message);
                    future.completeExceptionally(ex);
                }
                return;
            }
            reader.ackMessageAsync(message);
            future.complete(null);
        });
        return future;
    }

    private CompletableFuture<?> deleteInBroker(Message<PendingDeleteLedgerInfo> message) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            PendingDeleteLedgerInfo pendingDeleteLedger = message.getValue();
            //Now support managed_ledger two phase deletion.
            if (LedgerComponent.MANAGED_LEDGER == pendingDeleteLedger.getLedgerComponent()) {
                Long ledgerId = pendingDeleteLedger.getLedgerId();
                String topicName = pendingDeleteLedger.getTopicName();
                String ledgerType = pendingDeleteLedger.getLedgerType().name();
                String ledgerComponent = pendingDeleteLedger.getLedgerComponent().name();
                DeleteLedgerPayload deleteLedgerPayload =
                        new DeleteLedgerPayload(ledgerId, topicName, ledgerType, ledgerComponent);
                if (LedgerType.OFFLOAD_LEDGER == pendingDeleteLedger.getLedgerType()) {
                    deleteLedgerPayload.setOffloadContext(buildOffloadContext(pendingDeleteLedger));
                }
                pulsarAdmin.topics().deleteLedgerAsync(deleteLedgerPayload).whenComplete((res, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                        return;
                    }
                    future.complete(null);
                });
            } else if (LedgerComponent.MANAGED_CURSOR == pendingDeleteLedger.getLedgerComponent()) {
                future.complete(null);
            } else if (LedgerComponent.SCHEMA_STORAGE == pendingDeleteLedger.getLedgerComponent()) {
                future.complete(null);
            } else {
                future.completeExceptionally(new PendingDeleteLedgerInvalidException("Unknown ledger component"));
            }
        } catch (PendingDeleteLedgerInvalidException ex) {
            future.completeExceptionally(ex);
            return future;
        }
        return future;
    }

    private DeleteLedgerPayload.OffloadContext buildOffloadContext(PendingDeleteLedgerInfo pendingDeleteLedgerInfo)
            throws PendingDeleteLedgerInvalidException {
        if (LedgerType.LEDGER == pendingDeleteLedgerInfo.getLedgerType()) {
            return null;
        }
        LedgerInfo context = pendingDeleteLedgerInfo.getContext();
        if (context == null || !context.hasOffloadContext() || !context.getOffloadContext().hasUidMsb()) {
            throw new PendingDeleteLedgerInvalidException("Offload ledger didn't find uuid.");
        }
        DeleteLedgerPayload.OffloadContext targetContext = new DeleteLedgerPayload.OffloadContext();
        MLDataFormats.OffloadContext offloadContext = context.getOffloadContext();

        targetContext.setLsb(offloadContext.getUidLsb());
        targetContext.setMsb(offloadContext.getUidMsb());
        targetContext.setDriverName(OffloadUtils.getOffloadDriverName(offloadContext, ""));
        targetContext.setMetadata(OffloadUtils.getOffloadDriverMetadata(offloadContext, Collections.emptyMap()));
        return targetContext;
    }


    private CompletableFuture<?> deleteLocally(Message<PendingDeleteLedgerInfo> message) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PendingDeleteLedgerInfo pendingDeleteLedger = message.getValue();
        if (LedgerType.LEDGER == pendingDeleteLedger.getLedgerType()) {
            asyncDeleteLedger(pendingDeleteLedger.getTopicName(), pendingDeleteLedger.getLedgerId(),
                    pendingDeleteLedger.getLedgerComponent(), false).whenComplete((res, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                    return;
                }
                future.complete(null);
            });
        } else if (LedgerType.OFFLOAD_LEDGER == pendingDeleteLedger.getLedgerType()) {
            asyncDeleteOffloadedLedger(pendingDeleteLedger.getTopicName(), pendingDeleteLedger.getLedgerId(),
                    pendingDeleteLedger.getContext().getOffloadContext()).whenComplete((res, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                    return;
                }
                future.complete(null);
            });
        }
        future.completeExceptionally(new PendingDeleteLedgerInvalidException("Unknown ledger type"));
        return future;
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

    @Override
    public CompletableFuture<?> asyncDeleteLedger(String topicName, long ledgerId, LedgerComponent component,
                                                  boolean isBelievedDelete) {
        final long startTime = MathUtils.nowInNano();
        CompletableFuture<?> future = new CompletableFuture<>();
        log.info("[{}] Start async delete ledger {}", topicName, ledgerId);
        CompletableFuture<Void> believedFuture;
        if (isBelievedDelete) {
            believedFuture = CompletableFuture.completedFuture(null);
        } else {
            believedFuture = bookKeeper.getLedgerMetadata(ledgerId).thenCompose(metadata -> {
                CompletableFuture<Void> result = new CompletableFuture<>();
                Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
                if (!LedgerMetadataUtils.isComponentMatch(component, customMetadata)) {
                    result.completeExceptionally(
                            new PendingDeleteLedgerInvalidException("Ledger metadata component mismatch"));
                    return result;
                }
                if (!LedgerMetadataUtils.isNameMatch(component, topicName, customMetadata)) {
                    result.completeExceptionally(
                            new PendingDeleteLedgerInvalidException("Ledger metadata name mismatch"));
                    return result;
                }
                result.complete(null);
                return result;
            });
        }
        believedFuture.thenAccept(ignore -> {
            bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
                if (isNoSuchLedgerExistsException(rc)) {
                    log.warn("[{}] Ledger was already deleted {}", topicName, ledgerId);
                    future.complete(null);
                    deleteLedgerOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime),
                            TimeUnit.NANOSECONDS);
                    return;
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
        }).exceptionally(ex -> {
            if (isNoSuchLedgerExistsException(ex.getCause())) {
                log.warn("[{}] Ledger was already deleted {}", topicName, ledgerId);
                future.complete(null);
                return null;
            }
            log.error("[{}] Error delete ledger {}.", topicName, ledgerId, ex.getCause());
            future.completeExceptionally(ex.getCause());
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<?> asyncDeleteOffloadedLedger(String topicName, long ledgerId,
                                                           MLDataFormats.OffloadContext offloadContext) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        final long startTime = MathUtils.nowInNano();

        OffloadPoliciesImpl offloadPolicies = buildOffloadPolicies(offloadContext);

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

        UUID uuid = new UUID(offloadContext.getUidMsb(), offloadContext.getUidLsb());
        log.info("[{}] Start async delete offloaded ledger, ledgerId {} uuid {}.", topicName, ledgerId, uuid);

        Map<String, String> metadataMap = Maps.newHashMap();
        Map<String, String> metadata = OffloadUtils.getOffloadDriverMetadata(offloadContext, Collections.emptyMap());
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

    private OffloadPoliciesImpl buildOffloadPolicies(MLDataFormats.OffloadContext offloadContext) {
        String driverName = OffloadUtils.getOffloadDriverName(offloadContext, "");
        if (FILE_SYSTEM_DRIVER.equals(driverName)) {
            // TODO: 2022/7/13 Filesystem offloader params needs more info
            return null;
        } else {
            Map<String, String> metadata =
                    OffloadUtils.getOffloadDriverMetadata(offloadContext, Collections.emptyMap());
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
