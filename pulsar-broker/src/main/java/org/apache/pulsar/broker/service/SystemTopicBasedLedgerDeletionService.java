/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.deletion.LedgerDeletionService;
import org.apache.bookkeeper.mledger.deletion.RubbishLedger;
import org.apache.bookkeeper.mledger.deletion.LedgerComponent;
import org.apache.bookkeeper.mledger.deletion.LedgerType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.LedgerDeletionSystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemTopicBasedLedgerDeletionService implements LedgerDeletionService {

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedLedgerDeletionService.class);

    private final PulsarAdmin pulsarAdmin;

    private final int workers;

    private final Map<String, Set<Long>> managedLedgerExistsCache = new ConcurrentHashMap<>();

    private final Map<String, Set<Long>> managedCursorExistsCache = new ConcurrentHashMap<>();

    private final Map<String, Set<Long>> schemaStorageExistsCache = new ConcurrentHashMap<>();

    private ManagedLedgerConfig config;

    private final BookKeeper bookKeeper;

    private SystemTopicClient<RubbishLedger> ledgerDeletionTopicClient;

    private transient CompletableFuture<SystemTopicClient.Reader<RubbishLedger>> readerFuture;

    private transient CompletableFuture<SystemTopicClient.Writer<RubbishLedger>> writerFuture;

    public SystemTopicBasedLedgerDeletionService(PulsarClient client, PulsarAdmin pulsarAdmin,
                                                 BookKeeper bookKeeper, int workers) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.pulsarAdmin = pulsarAdmin;
        this.bookKeeper = bookKeeper;
        this.workers = Math.max(1, workers);
    }

    private SystemTopicClient<RubbishLedger> getLedgerDeletionTopicClient() throws PulsarClientException {
        TopicName systemTopicName =
                NamespaceEventsSystemTopicFactory.getSystemTopicName(NamespaceName.SYSTEM_NAMESPACE,
                        EventType.LEDGER_DELETION);
        if (systemTopicName == null) {
            throw new PulsarClientException.InvalidTopicNameException(
                    "Can't create SystemTopicBaseTxnBufferSnapshotService, "
                            + "because the topicName is null!");
        }
        return namespaceEventsSystemTopicFactory.createLedgerDeletionSystemTopicClient(NamespaceName.SYSTEM_NAMESPACE);
    }

    @Override
    public void start() throws PulsarClientException, PulsarAdminException {
        this.ledgerDeletionTopicClient = getLedgerDeletionTopicClient();
        initSystemTopic();
    }

    private void initSystemTopic() {
        this.pulsarAdmin.topics()
                .createPartitionedTopicAsync(SystemTopicNames.LEDGER_DELETION_TOPIC.getPartitionedTopicName(), workers)
                .whenComplete((res, e) -> {
                    if (e != null && !(e instanceof PulsarAdminException.ConflictException)) {
                        initSystemTopic();
                        return;
                    }
                    initReaderFuture();
                    initWriterFuture();
                });
    }

    private void readMoreRubbishLedger(LedgerDeletionSystemTopicClient.RubbishLedgerReader reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                deleteRubbishLedger(reader, msg);
                readMoreRubbishLedger(reader);
            } else {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more rubbish ledger exception, close the read now!", ex);
                    reader.closeAsync();
                    initReaderFuture();
                } else {
                    log.warn("Read more rubbish ledger exception, read again.", ex);
                    readMoreRubbishLedger(reader);
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
                readMoreRubbishLedger((LedgerDeletionSystemTopicClient.RubbishLedgerReader) reader);
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
    public CompletableFuture<?> appendRubbishLedger(String topicName, long ledgerId, LedgerInfo context,
                                                    LedgerComponent component, LedgerType type,
                                                    boolean checkLedgerStillInUse) {
        topicName = tuneTopicName(topicName);
        RubbishLedger rubbishLedger = null;
        if (LedgerType.LEDGER == type) {
            ManagedLedgerInfo.LedgerInfo ledgerInfo = ManagedLedgerInfo.LedgerInfo.buildLedger(ledgerId);
            rubbishLedger = new RubbishLedger(topicName, component, type, ledgerInfo, checkLedgerStillInUse);
        } else if (LedgerType.OFFLOAD_LEDGER == type) {
            if (!context.getOffloadContext().hasUidMsb()) {
                CompletableFuture<?> future = new CompletableFuture<>();
                future.completeExceptionally(
                        new IllegalArgumentException("The ledger " + ledgerId + " didn't offload."));
                return future;
            }
            UUID uuid = new UUID(context.getOffloadContext().getUidMsb(), context.getOffloadContext().getUidLsb());
            ManagedLedgerInfo.LedgerInfo ledgerInfo =
                    ManagedLedgerInfo.LedgerInfo.buildOffloadLedger(ledgerId, uuid.toString());
            rubbishLedger = new RubbishLedger(topicName, component, type, ledgerInfo, checkLedgerStillInUse);
        }
        return sendRubbishMsg(rubbishLedger);
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

    @Override
    public void setUpOffloadConfig(ManagedLedgerConfig managedLedgerConfig) {
        this.config = managedLedgerConfig;
    }

    private CompletableFuture<?> deleteRubbishLedger(LedgerDeletionSystemTopicClient.RubbishLedgerReader reader,
                                                     Message<RubbishLedger> message) {
        RubbishLedger rubbishLedger = message.getValue();
        if (isToDeleteLedger(rubbishLedger)) {
            if (LedgerType.LEDGER == rubbishLedger.getLedgerType()) {
                return asyncDeleteLedger(rubbishLedger.getTopicName(),
                        rubbishLedger.getLedgerInfo().getLedgerId()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message);
                        return;
                    }
                    reader.reconsumeLaterAsync(message);
                });
            } else if (LedgerType.OFFLOAD_LEDGER == rubbishLedger.getLedgerType()) {
                return asyncDeleteOffloadedLedger(rubbishLedger.getTopicName(),
                        rubbishLedger.getLedgerInfo()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message);
                        return;
                    }
                    reader.reconsumeLaterAsync(message);
                });
            }
            reader.ackMessageAsync(message);
            return FutureUtil.failedFuture(
                    new InvalidParameterException("Received rubbish ledger message with invalid ledger type."));
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] ledger {} still in use, delete it later.", rubbishLedger.getTopicName(),
                    rubbishLedger.getLedgerInfo().getLedgerId());
        }
        return reader.reconsumeLaterAsync(message);
    }

    private boolean isToDeleteLedger(RubbishLedger rubbishLedger) {
        if (!rubbishLedger.isCheckLedgerStillInUse()) {
            return true;
        }
        Set<Long> ledgerIds;
        try {
            ledgerIds = getLedgerIds(rubbishLedger);
        } catch (Exception e) {
            return false;
        }

        if (ledgerIds.contains(rubbishLedger.getLedgerInfo().getLedgerId())) {
            try {
                updateRubbishExistsCache(rubbishLedger.getTopicName());
            } catch (Exception e) {
                return false;
            }
            try {
                ledgerIds = getLedgerIds(rubbishLedger);
            } catch (Exception e) {
                return false;
            }
            return !ledgerIds.contains(rubbishLedger.getLedgerInfo().getLedgerId());
        }
        return true;
    }

    private Set<Long> getLedgerIds(RubbishLedger rubbishLedger) throws PulsarAdminException {
        if (LedgerComponent.MANAGED_LEDGER == rubbishLedger.getLedgerComponent()) {
            Set<Long> ledgerIds = managedLedgerExistsCache.get(rubbishLedger.getTopicName());
            if (ledgerIds == null) {
                updateRubbishExistsCache(rubbishLedger.getTopicName());
            }
            return managedLedgerExistsCache.get(rubbishLedger.getTopicName());
        } else if (LedgerComponent.MANAGED_CURSOR == rubbishLedger.getLedgerComponent()) {
            Set<Long> ledgerIds = managedCursorExistsCache.get(rubbishLedger.getTopicName());
            if (ledgerIds == null) {
                updateRubbishExistsCache(rubbishLedger.getTopicName());
            }
            return managedCursorExistsCache.get(rubbishLedger.getTopicName());
        } else if (LedgerComponent.SCHEMA_STORAGE == rubbishLedger.getLedgerComponent()) {
            Set<Long> ledgerIds = schemaStorageExistsCache.get(rubbishLedger.getTopicName());
            if (ledgerIds == null) {
                updateRubbishExistsCache(rubbishLedger.getTopicName());
            }
            return schemaStorageExistsCache.get(rubbishLedger.getTopicName());
        }
        throw new IllegalArgumentException("Unknown rubbish source: " + rubbishLedger.getLedgerComponent());
    }

    private void updateRubbishExistsCache(String topicName) throws PulsarAdminException {

        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(topicName);
        Set<Long> ledgerIds = internalStats.ledgers.stream().map(ele1 -> ele1.ledgerId).collect(Collectors.toSet());

        Set<Long> cursorIds =
                internalStats.cursors.values().stream().map(ele -> ele.cursorLedger).collect(Collectors.toSet());
        Set<Long> schemaIds = internalStats.schemaLedgers.stream().map(ele -> ele.ledgerId).collect(Collectors.toSet());

        managedLedgerExistsCache.put(topicName, ledgerIds);
        managedCursorExistsCache.put(topicName, cursorIds);
        schemaStorageExistsCache.put(topicName, schemaIds);
    }

    private CompletableFuture<?> sendRubbishMsg(RubbishLedger rubbishLedger) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                writer.writeAsync(rubbishLedger).whenComplete(((messageId, e) -> {
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
        CompletableFuture<?> future = new CompletableFuture<>();
        log.info("[{}] Start async delete ledger {}", topicName, ledgerId);
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", topicName, ledgerId);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error delete ledger {} : {}", topicName, ledgerId, BKException.getMessage(rc));
                future.completeExceptionally(ManagedLedgerImpl.createManagedLedgerException(rc));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleted ledger {}", topicName, ledgerId);
            }
            future.complete(null);
        }, null);
        return future;
    }

    private CompletableFuture<?> asyncDeleteOffloadedLedger(String topicName, ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        CompletableFuture<?> future = new CompletableFuture<>();
        if (config == null) {
            future.completeExceptionally(
                    new IllegalArgumentException("Offload config didn't setup, can't delete offload ledger."));
            return future;
        }
        if (!ledgerInfo.isOffloaded()) {
            future.completeExceptionally(new IllegalArgumentException(
                    String.format("[%s] Failed delete offload for ledgerId %s, can't find offload context.", topicName,
                            ledgerInfo.getLedgerId())));
            return future;
        }
        String cleanupReason = "Trash-Trimming";

        Long ledgerId = ledgerInfo.getLedgerId();
        UUID uuid = UUID.fromString(ledgerInfo.getOffloadedContextUuid());
        log.info("[{}] Start async delete offloaded ledger, ledgerId {} uuid {} because of the reason {}.", topicName,
                ledgerId, uuid, cleanupReason);

        Map<String, String> metadataMap = Maps.newHashMap();
        metadataMap.putAll(config.getLedgerOffloader().getOffloadDriverMetadata());
        metadataMap.put("ManagedLedgerName", topicName);

        try {
            config.getLedgerOffloader()
                    .deleteOffloaded(ledgerId, uuid, metadataMap)
                    .whenComplete((ignored, exception) -> {
                        if (exception != null) {
                            log.warn("[{}] Failed delete offload for ledgerId {} uuid {}, (cleanup reason: {})",
                                    topicName, ledgerId, uuid, cleanupReason, exception);
                            future.completeExceptionally(
                                    new ManagedLedgerException("Failed to delete offloaded ledger"));
                            return;
                        }
                        future.complete(null);
                    });
        } catch (Exception e) {
            log.warn("[{}] Failed to delete offloaded ledgers.", topicName, e);
        }
        return future;
    }

    static class Tuple<K, V> {
        private K ledgerIds;
        private V cursorIds;
    }
}
