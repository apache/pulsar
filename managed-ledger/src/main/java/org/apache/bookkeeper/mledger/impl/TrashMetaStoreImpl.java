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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.TrashDataComponent;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.batching.AbstractBatchedMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrashMetaStoreImpl implements TrashMetaStore {

    private static final Logger log = LoggerFactory.getLogger(TrashMetaStoreImpl.class);

    private static final String BASE_NODE = "/trash-data";

    private static final String PREFIX = BASE_NODE + "/";

    private static final String DELETE = "/delete";

    private static final String ARCHIVE = "/archive-";

    private static final String TRASH_KEY_SEPARATOR = "-";

    private static final String DELETABLE_LEDGER_SUFFIX = "DL";

    private static final String DELETABLE_OFFLOADED_LEDGER_SUFFIX = "DOL";

    private static final int RETRY_COUNT = 9;

    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    //key:ledgerId value:storageContext
    private NavigableMap<String, LedgerInfo> trashData = new ConcurrentSkipListMap<>();

    private final AtomicInteger archiveCount = new AtomicInteger();

    private final CallbackMutex trashMutex = new CallbackMutex();

    private final CallbackMutex deleteMutex = new CallbackMutex();

    private final AbstractBatchedMetadataStore metadataStore;

    private volatile Stat deleteStat;

    private final String type;

    private final String name;

    private long archiveIndex;

    private final ManagedLedgerConfig config;

    private final OrderedScheduler scheduledExecutor;

    private final OrderedExecutor executor;

    private final BookKeeper bookKeeper;

    private final int trashDataLimitSize;

    public TrashMetaStoreImpl(String type, String name, MetadataStore metadataStore, ManagedLedgerConfig config,
                              OrderedScheduler scheduledExecutor, OrderedExecutor executor, BookKeeper bookKeeper) {
        this.type = type;
        this.name = name;
        this.config = config;
        if (!(metadataStore instanceof AbstractBatchedMetadataStore)) {
            throw new IllegalStateException("TrashMetaStoreImpl metadata store must support batch operation.");
        }
        this.metadataStore = (AbstractBatchedMetadataStore) metadataStore;
        this.scheduledExecutor = scheduledExecutor;
        this.executor = executor;
        this.bookKeeper = bookKeeper;
        this.trashDataLimitSize = config.getTrashDataLimitSize();
    }

    @Override
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        metadataStore.getChildren(buildPath()).whenCompleteAsync((res, e) -> {
            if (e != null) {
                log.error("Get archive index failed, name:{} type: {}", name, type, e);
                future.completeExceptionally(e);
            } else {
                long maxIndex = -1;
                for (String node : res) {
                    if (node.startsWith(ARCHIVE)) {
                        long index = Long.parseLong(node.split(ARCHIVE)[1]);
                        if (index > maxIndex) {
                            maxIndex = index;
                        }
                    }
                }
                archiveIndex = maxIndex + 1;
            }
            metadataStore.get(buildDeletePath()).whenComplete((r, ex) -> {
                if (ex != null) {
                    log.error("Get delete data failed, name:{} type: {}", name, type, e);
                    future.completeExceptionally(e);
                } else {
                    if (r.isEmpty()) {
                        future.complete(null);
                        return;
                    }
                    byte[] value = r.get().getValue();
                    try {
                        trashData.putAll(deSerialize(value));
                        deleteStat = r.get().getStat();
                        future.complete(null);
                    } catch (InvalidProtocolBufferException exc) {
                        future.completeExceptionally(getException(exc));
                    }
                }
            });
        }, scheduledExecutor.chooseThread(name));
        return future;
    }

    private static ManagedLedgerException.MetaStoreException getException(Throwable t) {
        if (t.getCause() instanceof MetadataStoreException.BadVersionException) {
            return new ManagedLedgerException.BadVersionException(t.getMessage());
        } else {
            return new ManagedLedgerException.MetaStoreException(t);
        }
    }

    public void appendInBackground(String key, LedgerInfo context, CompletableFuture<?> future) {
        executor.executeOrdered(name, safeRun(() -> appendTrashData(key, context, future)));
    }

    @Override
    public void appendLedgerTrashData(long ledgerId, LedgerInfo context, CompletableFuture<?> future) {
        String key = buildLedgerKey(ledgerId);
        appendTrashData(key, context, future);
    }

    @Override
    public void appendOffloadLedgerTrashData(long ledgerId, LedgerInfo context, CompletableFuture<?> future) {
        String key = buildOffloadLedgerKey(ledgerId);
        appendTrashData(key, context, future);
    }

    private void appendTrashData(String key, LedgerInfo context, CompletableFuture<?> future) {
        if (!trashMutex.tryLock()) {
            scheduledExecutor.schedule(safeRun(() -> appendInBackground(key, context, future)), 100,
                    TimeUnit.MILLISECONDS);
        }
        try {
            trashData.put(key, context);
            archiveStaleDataIfFull(future);
        } finally {
            trashMutex.unlock();
        }
    }

    @Override
    public void asyncUpdateTrashData(TrashMetaStore.TrashMetaStoreCallback<Void> callback) {
        metadataStore.put(buildDeletePath(), serialize(trashData), Optional.of(deleteStat.getVersion()))
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        callback.operationFailed(getException(e));
                        return;
                    }
                    deleteStat = res;
                    callback.operationComplete(null);
                }, executor.chooseThread(name));
    }

    private byte[] serialize(Map<String, LedgerInfo> toPersist) {
        return TrashDataComponent.newBuilder().putAllComponent(toPersist).build().toByteArray();
    }

    private Map<String, LedgerInfo> deSerialize(byte[] content) throws InvalidProtocolBufferException {
        TrashDataComponent component = TrashDataComponent.parseFrom(content);
        return component.getComponentMap();
    }

    @Override
    public void triggerDelete() {
        if (!deleteMutex.tryLock() || !trashMutex.tryLock()) {
            scheduledExecutor.schedule(this::triggerDelete, 100, TimeUnit.MILLISECONDS);
            return;
        }
        try {
            List<TrashDeleteHelper> toDelete = getToDeleteData();
            if (toDelete.size() == 0) {
                return;
            }
            for (TrashDeleteHelper delHelper : toDelete) {
                asyncDeleteTrash(delHelper);
            }
        } finally {
            deleteMutex.unlock();
            trashMutex.unlock();
        }
    }

    @Override
    public List<Long> getAllArchiveIndex() {
        return null;
    }

    @Override
    public Map<String, LedgerInfo> getArchiveData(Long index) {
        return null;
    }

    private void archiveStaleDataIfFull(CompletableFuture<?> future) {
        if (trashData.size() <= trashDataLimitSize) {
            future.complete(null);
            return;
        }
        //persist and remove 1/2 trash data, avoid always trigger archive to operate metadata.
        long archiveSize = trashDataLimitSize / 2 == 0 ? 1 : trashDataLimitSize / 2;

        Map<String, LedgerInfo> toArchive = new ConcurrentSkipListMap<>();
        for (Map.Entry<String, LedgerInfo> entry : trashData.entrySet()) {
            toArchive.put(entry.getKey(), entry.getValue());
            if (toArchive.size() == archiveSize) {
                break;
            }
        }
        //persist
        increaseArchiveCountAndWhenTrashFull(future);
    }

    private void increaseArchiveCountAndWhenTrashFull(CompletableFuture<?> future) {
        archiveCount.set(trashDataLimitSize / 2);
        updateArchiveDataIfNecessary(future, executor);
    }

    private void increaseArchiveCountWhenDeleteFailed(CompletableFuture<?> future) {
        archiveCount.incrementAndGet();
        updateArchiveDataIfNecessary(future, scheduledExecutor);
    }

    private void updateArchiveDataIfNecessary(CompletableFuture<?> future, OrderedExecutor callbackExecutor) {
        if (archiveCount.get() < trashDataLimitSize / 2) {
            future.complete(null);
            return;
        }
        asyncUpdateArchiveData(new TrashMetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result) {
                future.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                future.completeExceptionally(e);
            }
        }, callbackExecutor);
    }


    private String buildPath() {
        return PREFIX + name + "/" + type;
    }

    private String buildDeletePath() {
        return buildPath() + DELETE;
    }

    private String buildArchivePath() {
        return buildPath() + ARCHIVE + archiveIndex;
    }

    //take 1/10 trash to delete, if the size over 10, use 10 to delete.
    private List<TrashDeleteHelper> getToDeleteData() {
        if (trashData.size() == 0) {
            return Collections.emptyList();
        }
        if (trashData.size() == 1 && halfRandomSuccess()) {
            Map.Entry<String, LedgerInfo> entry = trashData.firstEntry();
            return Collections.singletonList(TrashDeleteHelper.build(entry.getKey(), entry.getValue()));
        }
        int batchSize = trashData.size() / 10;
        if (batchSize > 10) {
            batchSize = 10;
        }
        if (batchSize == 0) {
            batchSize = 1;
        }
        List<TrashDeleteHelper> toDelete = new ArrayList<>(batchSize);
        for (Map.Entry<String, LedgerInfo> entry : trashData.descendingMap().entrySet()) {
            TrashDeleteHelper delHelper = TrashDeleteHelper.build(entry.getKey(), entry.getValue());
            //if last retryCount is zero, the before data retryCount is zero too.
            if (delHelper.retryCount == 0) {
                break;
            }
            toDelete.add(delHelper);
            if (toDelete.size() == batchSize) {
                break;
            }
        }
        return toDelete;
    }

    private boolean halfRandomSuccess() {
        return ThreadLocalRandom.current().nextInt(100) >= 50;
    }

    private void asyncUpdateArchiveData(TrashMetaStore.TrashMetaStoreCallback<Void> callback,
                                        OrderedExecutor callbackExecutor) {
        //transaction operation
        NavigableMap<String, LedgerInfo> persistDelete = new ConcurrentSkipListMap<>();
        NavigableMap<String, LedgerInfo> persistArchive = new ConcurrentSkipListMap<>();


        for (Map.Entry<String, LedgerInfo> entry : trashData.entrySet()) {
            persistArchive.put(entry.getKey(), entry.getValue());
            if (persistArchive.size() >= trashDataLimitSize / 2) {
                break;
            }
        }

        persistDelete.putAll(trashData);
        for (Map.Entry<String, LedgerInfo> entry : persistArchive.entrySet()) {
            persistDelete.remove(entry.getKey());
        }
        //build delete persist operation
        List<MetadataOp> txOps = new ArrayList<>(2);
        OpPut opDeletePersist = new OpPut(buildDeletePath(), serialize(persistDelete),
                deleteStat == null ? Optional.of(-1L) : Optional.of(deleteStat.getVersion()),
                EnumSet.noneOf(CreateOption.class));
        //build archive persist operation
        OpPut opArchivePersist = new OpPut(buildArchivePath(), serialize(persistArchive), Optional.of(-1L),
                EnumSet.noneOf(CreateOption.class));
        txOps.add(opDeletePersist);
        txOps.add(opArchivePersist);
        metadataStore.batchOperation(txOps);

        opDeletePersist.getFuture().whenCompleteAsync((res, e) -> {
            if (e != null) {
                log.error("Persist trash data failed.", e);
                callback.operationFailed(getException(e));
                return;
            }
            opArchivePersist.getFuture().whenComplete((res1, e1) -> {
                if (e1 != null) {
                    log.error("Persist archive data failed.", e1);
                    callback.operationFailed(getException(e1));
                    return;
                }
                deleteStat = res;
                trashData = persistDelete;
                archiveCount.set(0);
                archiveIndex++;
            });
        }, callbackExecutor.chooseThread(name));

    }

    private String buildLedgerKey(long ledgerId) {
        return buildKey(RETRY_COUNT, ledgerId, DELETABLE_LEDGER_SUFFIX);
    }

    private String buildOffloadLedgerKey(long ledgerId) {
        return buildKey(RETRY_COUNT, ledgerId, DELETABLE_OFFLOADED_LEDGER_SUFFIX);
    }

    private static String buildKey(int retryCount, long ledgerId, String suffix) {
        return retryCount + TRASH_KEY_SEPARATOR + String.format("%019d", ledgerId) + TRASH_KEY_SEPARATOR
                + suffix;
    }

    private void asyncDeleteTrash(TrashDeleteHelper delHelper) {
        if (delHelper.isLedger()) {
            asyncDeleteLedger(delHelper.ledgerId, new AsyncCallbacks.DeleteLedgerCallback() {
                @Override
                public void deleteLedgerComplete(Object ctx) {
                    onDeleteSuccess(delHelper);
                }

                @Override
                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    onDeleteFailed(delHelper);
                }
            });
        } else if (delHelper.isOffloadLedger()) {
            asyncDeleteOffloadedLedger(delHelper.ledgerId, delHelper.context,
                    new AsyncCallbacks.DeleteLedgerCallback() {
                        @Override
                        public void deleteLedgerComplete(Object ctx) {
                            onDeleteSuccess(delHelper);
                        }

                        @Override
                        public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            onDeleteFailed(delHelper);
                        }
                    });
        }
    }

    private void onDeleteSuccess(TrashDeleteHelper delHelper) {
        String key = delHelper.transferToTrashKey();
        if (log.isDebugEnabled()) {
            String info = null;
            if (delHelper.isLedger()) {
                info = String.format("Delete ledger %s success.", delHelper.ledgerId);
            } else if (delHelper.isOffloadLedger()) {
                info = String.format("Delete offload ledger %s success.", delHelper.ledgerId);
            }
            log.debug(info);
        }
        trashData.remove(key);
    }

    private void onDeleteFailed(TrashDeleteHelper delHelper) {
        if (delHelper.retryCount == 0) {
            //copy to archive
            if (log.isDebugEnabled()) {
                String info = null;
                if (delHelper.isLedger()) {
                    info = String.format("Delete ledger %d reach retry limit %d, copy it to archive",
                            delHelper.ledgerId, RETRY_COUNT);
                } else if (delHelper.isOffloadLedger()) {
                    info = String.format("Delete offload ledger %d reach retry limit %d, copy it to archive",
                            delHelper.ledgerId, RETRY_COUNT);
                }
                log.debug(info);
            }
            increaseArchiveCountWhenDeleteFailed(COMPLETED_FUTURE);
        } else {
            //override old key
            String key = delHelper.transferToTrashKey();
            trashData.remove(key);
            trashData.put(buildKey(delHelper.retryCount - 1, delHelper.ledgerId, delHelper.suffix),
                    delHelper.context);
        }
    }

    private void asyncDeleteLedger(long ledgerId, AsyncCallbacks.DeleteLedgerCallback callback) {
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", name, ledgerId);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error delete ledger {} : {}", name, ledgerId, BKException.getMessage(rc));
                callback.deleteLedgerFailed(ManagedLedgerImpl.createManagedLedgerException(rc), null);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleted ledger {}", name, ledgerId);
            }
            callback.deleteLedgerComplete(ctx);
        }, null);
    }

    private void asyncDeleteOffloadedLedger(long ledgerId, LedgerInfo info,
                                            AsyncCallbacks.DeleteLedgerCallback callback) {
        if (!info.getOffloadContext().hasUidMsb()) {
            return;
        }
        String cleanupReason = "Trash-Trimming";

        UUID uuid = new UUID(info.getOffloadContext().getUidMsb(), info.getOffloadContext().getUidLsb());

        log.info("[{}] Cleanup offload for ledgerId {} uuid {} because of the reason {}.", name, ledgerId, uuid,
                cleanupReason);
        Map<String, String> metadataMap = Maps.newHashMap();
        metadataMap.putAll(config.getLedgerOffloader().getOffloadDriverMetadata());
        metadataMap.put("ManagedLedgerName", name);

        try {
            config.getLedgerOffloader()
                    .deleteOffloaded(ledgerId, uuid, metadataMap)
                    .whenComplete((ignored, exception) -> {
                        if (exception != null) {
                            log.warn("[{}] Error cleaning up offload for {}, (cleanup reason: {})",
                                    name, ledgerId, cleanupReason, exception);
                            log.warn("[{}] Failed to delete offloaded ledger after retries {} / {}", name, ledgerId,
                                    uuid);
                            callback.deleteLedgerFailed(
                                    new ManagedLedgerException("Failed to delete offloaded ledger after retries"),
                                    null);
                            return;
                        }
                        callback.deleteLedgerComplete(null);
                    });
        } catch (Exception e) {
            log.warn("[{}] Failed to cleanup offloaded ledgers.", name, e);
        }
    }


    private static class TrashDeleteHelper {

        private final int retryCount;

        private final long ledgerId;

        private final String suffix;

        private final LedgerInfo context;

        public TrashDeleteHelper(int retryCount, long ledgerId, String suffix, LedgerInfo context) {
            this.retryCount = retryCount;
            this.ledgerId = ledgerId;
            this.suffix = suffix;
            this.context = context;
        }

        public static TrashDeleteHelper build(String key, LedgerInfo context) {
            String[] split = key.split(TRASH_KEY_SEPARATOR);
            int retryCont = Integer.parseInt(split[0]);
            long ledgerId = Long.parseLong(split[1]);
            return new TrashDeleteHelper(retryCont, ledgerId, split[2], context);
        }

        private String transferToTrashKey() {
            return TrashMetaStoreImpl.buildKey(retryCount, ledgerId, suffix);
        }

        private boolean isLedger() {
            return DELETABLE_LEDGER_SUFFIX.equals(suffix);
        }

        private boolean isOffloadLedger() {
            return DELETABLE_OFFLOADED_LEDGER_SUFFIX.equals(suffix);
        }
    }


}
