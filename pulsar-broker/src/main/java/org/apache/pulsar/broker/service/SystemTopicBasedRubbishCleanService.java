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
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.rubbish.RubbishCleanService;
import org.apache.bookkeeper.mledger.rubbish.RubbishInfo;
import org.apache.bookkeeper.mledger.rubbish.RubbishSource;
import org.apache.bookkeeper.mledger.rubbish.RubbishType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.RubbishCleanerSystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemTopicBasedRubbishCleanService implements RubbishCleanService {

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedRubbishCleanService.class);

    private final PulsarAdmin pulsarAdmin;

    private final Map<Long, Boolean> managedLedgerExistsCache = new ConcurrentSkipListMap<>();

    private final Map<Long, Boolean> managedCursorExistsCache = new ConcurrentSkipListMap<>();

    private final Map<Long, Boolean> schemaStorageExistsCache = new ConcurrentSkipListMap<>();

    private ManagedLedgerConfig config;

    private final BookKeeper bookKeeper;

    private SystemTopicClient<RubbishInfo> rubbishCleanerTopicClient;

    private CompletableFuture<SystemTopicClient.Reader<RubbishInfo>> readerFuture;

    public SystemTopicBasedRubbishCleanService(PulsarClient client, PulsarAdmin pulsarAdmin,
                                               BookKeeper bookKeeper) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.pulsarAdmin = pulsarAdmin;
        this.bookKeeper = bookKeeper;
    }

    private SystemTopicClient<RubbishInfo> getRubbishCleanerTopicClient() throws PulsarClientException {
        TopicName systemTopicName =
                NamespaceEventsSystemTopicFactory.getSystemTopicName(NamespaceName.SYSTEM_NAMESPACE,
                        EventType.RUBBISH_CLEANER);
        if (systemTopicName == null) {
            throw new PulsarClientException.InvalidTopicNameException(
                    "Can't create SystemTopicBaseTxnBufferSnapshotService, "
                            + "because the topicName is null!");
        }
        return namespaceEventsSystemTopicFactory.createRubbishCleanerSystemTopicClient(NamespaceName.SYSTEM_NAMESPACE);
    }

    @Override
    public void start() throws PulsarClientException {
        this.rubbishCleanerTopicClient = getRubbishCleanerTopicClient();
        initReaderFuture();
    }

    private void readMoreRubbishInfo(RubbishCleanerSystemTopicClient.RubbishInfoReader reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                deleteRubbishLedger(reader, msg);
                readMoreRubbishInfo(reader);
            } else {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more topic policies exception, close the read now!", ex);
                    reader.closeAsync();
                    initReaderFuture();
                } else {
                    log.warn("Read more topic polices exception, read again.", ex);
                    readMoreRubbishInfo(reader);
                }
            }
        });
    }

    private void initReaderFuture() {
        this.readerFuture = rubbishCleanerTopicClient.newReaderAsync().whenComplete((reader, ex) -> {
            if (ex != null) {
                log.error("Failed to create reader on rubbish system topic", ex);
                reader.closeAsync();
                initReaderFuture();
            } else {
                readMoreRubbishInfo((RubbishCleanerSystemTopicClient.RubbishInfoReader) reader);
            }
        });
    }

    @Override
    public CompletableFuture<?> appendRubbishLedger(String topicName, long ledgerId, LedgerInfo context,
                                                    RubbishSource source, RubbishType type,
                                                    boolean checkLedgerStillInUse) {
        RubbishInfo rubbishInfo = null;
        if (RubbishType.LEDGER == type) {
            ManagedLedgerInfo.LedgerInfo ledgerInfo = ManagedLedgerInfo.LedgerInfo.buildLedger(ledgerId);
            rubbishInfo = new RubbishInfo(topicName, source, type, ledgerInfo, checkLedgerStillInUse);
        } else if (RubbishType.OFFLOAD_LEDGER == type) {
            if (!context.getOffloadContext().hasUidMsb()) {
                CompletableFuture<?> future = new CompletableFuture<>();
                future.completeExceptionally(
                        new IllegalArgumentException("The ledger " + ledgerId + " didn't offload."));
                return future;
            }
            UUID uuid = new UUID(context.getOffloadContext().getUidMsb(), context.getOffloadContext().getUidLsb());
            ManagedLedgerInfo.LedgerInfo ledgerInfo =
                    ManagedLedgerInfo.LedgerInfo.buildOffloadLedger(ledgerId, uuid.toString());
            rubbishInfo = new RubbishInfo(topicName, source, type, ledgerInfo, checkLedgerStillInUse);
        }
        return sendRubbishMsg(rubbishInfo);
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

    private boolean isToDeleteLedger(RubbishInfo rubbishInfo) {
        if (!rubbishInfo.isCheckLedgerStillInUse()) {
            return true;
        }
        if (managedLedgerExistsCache.containsKey(rubbishInfo.getLedgerInfo().getLedgerId())) {
            try {
                updateRubbishExistsCache(rubbishInfo);
            } catch (PulsarAdminException e) {
                return false;
            }
            return !managedLedgerExistsCache.containsKey(rubbishInfo.getLedgerInfo().getLedgerId());
        }
        return true;
    }


    private void updateRubbishExistsCache(RubbishInfo rubbishInfo) throws PulsarAdminException {
        switch (rubbishInfo.getRubbishSource()) {
            case MANAGED_LEDGER:
                updateManagedLedgerExistsCache(rubbishInfo.getTopicName());
                break;
            case MANAGED_CURSOR:
                updateManagedCursorExistsCache(rubbishInfo.getTopicName());
                break;
            case SCHEMA_STORAGE:
                updateSchemaStorageExistsCache(rubbishInfo.getTopicName());
                break;
        }
    }

    private void updateManagedLedgerExistsCache(String topicName) throws PulsarAdminException {
        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(topicName);
        Map<Long, Boolean> ledgersExists = new HashMap<>();
        internalStats.ledgers.forEach(ledger -> ledgersExists.put(ledger.ledgerId, true));
        managedLedgerExistsCache.clear();
        managedLedgerExistsCache.putAll(ledgersExists);

        HashMap<Long, Boolean> cursorExists = new HashMap<>();
        internalStats.cursors.values().forEach(cursor -> cursorExists.put(cursor.cursorLedger, true));
        managedCursorExistsCache.clear();
        managedCursorExistsCache.putAll(cursorExists);
    }

    private void updateManagedCursorExistsCache(String topicName) throws PulsarAdminException {
        PersistentTopicInternalStats internalStats = pulsarAdmin.topics().getInternalStats(topicName);
        Map<Long, Boolean> ledgersExists = new HashMap<>();
        internalStats.ledgers.forEach(ledger -> ledgersExists.put(ledger.ledgerId, true));
        managedLedgerExistsCache.clear();
        managedLedgerExistsCache.putAll(ledgersExists);

        HashMap<Long, Boolean> cursorExists = new HashMap<>();
        internalStats.cursors.values().forEach(cursor -> cursorExists.put(cursor.cursorLedger, true));
        managedCursorExistsCache.clear();
        managedCursorExistsCache.putAll(cursorExists);
    }

    private void updateSchemaStorageExistsCache(String topicName) throws PulsarAdminException {
        List<SchemaInfo> allSchemas = pulsarAdmin.schemas().getAllSchemas(topicName);
        allSchemas.forEach(schema -> schema.getSchemaDefinition());

    }

    private CompletableFuture<?> deleteRubbishLedger(RubbishCleanerSystemTopicClient.RubbishInfoReader reader,
                                                     Message<RubbishInfo> message) {
        RubbishInfo rubbishInfo = message.getValue();
        if (isToDeleteLedger(rubbishInfo)) {
            if (RubbishType.LEDGER == rubbishInfo.getRubbishType()) {
                return asyncDeleteLedger(rubbishInfo.getTopicName(),
                        rubbishInfo.getLedgerInfo().getLedgerId()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message);
                        return;
                    }
                    reader.reconsumeLaterAsync(message);
                });
            } else if (RubbishType.OFFLOAD_LEDGER == rubbishInfo.getRubbishType()) {
                return asyncDeleteOffloadedLedger(rubbishInfo.getTopicName(),
                        rubbishInfo.getLedgerInfo()).whenComplete((res, e) -> {
                    if (e == null) {
                        reader.ackMessageAsync(message);
                        return;
                    }
                    reader.reconsumeLaterAsync(message);
                });
            }
            reader.ackMessageAsync(message);
            return FutureUtil.failedFuture(
                    new InvalidParameterException("Received rubbishInfo message with invalid rubbish type."));
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] ledger {} still in use, delete it later.", rubbishInfo.getTopicName(),
                    rubbishInfo.getLedgerInfo().getLedgerId());
        }
        return reader.reconsumeLaterAsync(message);
    }

    private CompletableFuture<?> sendRubbishMsg(RubbishInfo rubbishInfo) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        CompletableFuture<SystemTopicClient.Writer<RubbishInfo>> writerFuture =
                rubbishCleanerTopicClient.newWriterAsync();
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                writer.writeAsync(rubbishInfo).whenComplete(((messageId, e) -> {
                            if (e != null) {
                                result.completeExceptionally(e);
                            } else {
                                if (messageId != null) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(new RuntimeException("Got message id is null."));
                                }
                            }
                            writer.closeAsync().whenComplete((v, cause) -> {
                                if (cause != null) {
                                    log.error("Close writer error.", cause);
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Close writer success.");
                                    }
                                }
                            });
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
}
