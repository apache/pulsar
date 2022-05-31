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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedTrash;
import org.apache.bookkeeper.mledger.ManagedTrashMXBean;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.TrashDataComponent;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.batching.AbstractBatchedMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedTrashImpl implements ManagedTrash {

    private static final Logger log = LoggerFactory.getLogger(ManagedTrashImpl.class);

    private static final String BASE_NODE = "/trash-data";

    private static final String PREFIX = BASE_NODE + "/";

    private static final String DELETE = "/delete";

    private static final String ARCHIVE = "/archive-";

    private static final String TRASH_KEY_SEPARATOR = "-";

    private static final int RETRY_COUNT = 9;

    private static final long EMPTY_LEDGER_ID = -1L;

    private static final LedgerInfo EMPTY_LEDGER_INFO = LedgerInfo.newBuilder().setLedgerId(EMPTY_LEDGER_ID).build();

    private static final AtomicReferenceFieldUpdater<ManagedTrashImpl, ManagedTrashImpl.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedTrashImpl.class, ManagedTrashImpl.State.class, "state");

    protected volatile ManagedTrashImpl.State state = null;

    //key:ledgerId value:storageContext
    private NavigableMap<String, LedgerInfo> trashData = new ConcurrentSkipListMap<>();

    private final AtomicInteger toArchiveCount = new AtomicInteger();

    private final CallbackMutex trashMutex = new CallbackMutex();

    private final CallbackMutex deleteMutex = new CallbackMutex();

    private final CallbackMutex trashPersistMutex = new CallbackMutex();

    private final AbstractBatchedMetadataStore metadataStore;

    private volatile Stat deleteStat;

    private final String type;

    private final String name;

    private final ManagedLedgerConfig config;

    private final OrderedScheduler scheduledExecutor;

    private final OrderedExecutor executor;

    private final BookKeeper bookKeeper;

    private final int archiveDataLimitSize;

    private volatile boolean trashIsDirty;

    private ScheduledFuture<?> checkTrashPersistTask;

    private final ManagedTrashMXBean managedTrashMXBean;

    public ManagedTrashImpl(String type, String name, MetadataStore metadataStore, ManagedLedgerConfig config,
                            OrderedScheduler scheduledExecutor, OrderedExecutor executor, BookKeeper bookKeeper) {
        this.type = type;
        this.name = name;
        this.config = config;
        if (!(metadataStore instanceof AbstractBatchedMetadataStore)) {
            throw new IllegalStateException("ManagedTrashImpl metadata store must support batch operation.");
        }
        STATE_UPDATER.set(this, State.None);
        this.metadataStore = (AbstractBatchedMetadataStore) metadataStore;
        this.scheduledExecutor = scheduledExecutor;
        this.executor = executor;
        this.bookKeeper = bookKeeper;
        this.archiveDataLimitSize = config.getArchiveDataLimitSize();
        this.managedTrashMXBean = new ManagedTrashMXBeanImpl(this);
    }

    @Override
    public String name() {
        return name + "-" + type;
    }

    @Override
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        metadataStore.get(buildDeletePath()).whenCompleteAsync((res, e) -> {
            if (e != null) {
                log.error("[{}] Get delete data failed.", name(), e);
                future.completeExceptionally(e);
                return;
            }
            if (res.isEmpty()) {
                future.complete(null);
                return;
            }
            byte[] value = res.get().getValue();
            try {
                trashData.putAll(deSerialize(value));
                deleteStat = res.get().getStat();
                calculateArchiveCount().whenComplete((res1, e1) -> {
                    if (e1 != null) {
                        future.completeExceptionally(getException(e1));
                        return;
                    }
                    toArchiveCount.set(res1);
                    future.complete(null);
                    checkTrashPersistTask =
                            scheduledExecutor.scheduleAtFixedRate(safeRun(this::persistTrashIfNecessary), 30L, 30L,
                                    TimeUnit.MINUTES);
                    STATE_UPDATER.set(this, State.INITIALIZED);
                    triggerDeleteInBackground();
                });
            } catch (InvalidProtocolBufferException exc) {
                future.completeExceptionally(getException(exc));
            }
        }, scheduledExecutor.chooseThread(name));
        return future;
    }

    private void persistTrashIfNecessary() {
        if (trashIsDirty) {
            asyncUpdateTrashData();
        }
    }

    private CompletableFuture<Integer> calculateArchiveCount() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        calculateArchiveCountInBackground(future);
        return future;
    }

    private void calculateArchiveCountInBackground(CompletableFuture<Integer> future) {
        executor.executeOrdered(name, safeRun(() -> internalCalculateArchiveCount(future)));
    }

    private void internalCalculateArchiveCount(CompletableFuture<Integer> future) {
        if (!trashMutex.tryLock()) {
            scheduledExecutor.schedule(safeRun(() -> calculateArchiveCountInBackground(future)), 100,
                    TimeUnit.MILLISECONDS);
            return;
        }
        try {
            int toArchiveCount = 0;
            for (Map.Entry<String, LedgerInfo> entry : trashData.entrySet()) {
                if (!entry.getKey().startsWith("0")) {
                    break;
                }
                toArchiveCount++;
            }
            future.complete(toArchiveCount);
        } finally {
            trashMutex.unlock();
        }
    }

    @Override
    public CompletableFuture<?> appendLedgerTrashData(long ledgerId, LedgerInfo context, String type) {
        CompletableFuture<?> future = new CompletableFuture<>();
        if (context == null) {
            context = EMPTY_LEDGER_INFO;
        }
        appendTrashData(buildKey(RETRY_COUNT, ledgerId, type), context, future);
        return future;
    }

    private static ManagedLedgerException.MetaStoreException getException(Throwable t) {
        if (t.getCause() instanceof MetadataStoreException.BadVersionException) {
            return new ManagedLedgerException.BadVersionException(t.getMessage());
        } else {
            return new ManagedLedgerException.MetaStoreException(t);
        }
    }

    public void appendInBackground(final String key, final LedgerInfo context, final CompletableFuture<?> future) {
        executor.executeOrdered(name, safeRun(() -> appendTrashData(key, context, future)));
    }

    private void appendTrashData(final String key, final LedgerInfo context, final CompletableFuture<?> future) {
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            future.completeExceptionally(getException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return;
        }
        if (!trashMutex.tryLock()) {
            scheduledExecutor.schedule(safeRun(() -> appendInBackground(key, context, future)), 100,
                    TimeUnit.MILLISECONDS);
            return;
        }
        try {
            trashData.put(key, context);
            managedTrashMXBean.increaseTotalNumberOfDeleteLedgers();
            trashIsDirty = true;
        } finally {
            trashMutex.unlock();
        }
    }

    @Override
    public CompletableFuture<?> asyncUpdateTrashData() {
        log.info("{} Start async update trash data", name());
        CompletableFuture<Void> future = new CompletableFuture<>();
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            future.completeExceptionally(getException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return future;
        }
        metadataStore.put(buildDeletePath(), serialize(trashData),
                        deleteStat == null ? Optional.of(-1L) : Optional.of(deleteStat.getVersion()))
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        future.completeExceptionally(getException(e));
                        return;
                    }
                    deleteStat = res;
                    trashIsDirty = false;
                    trashPersistMutex.unlock();
                    future.complete(null);
                }, executor.chooseThread(name));
        return future;
    }

    private byte[] serialize(Map<String, LedgerInfo> toPersist) {
        TrashDataComponent.Builder builder = TrashDataComponent.newBuilder();
        for (Map.Entry<String, LedgerInfo> entry : toPersist.entrySet()) {
            MLDataFormats.TrashData.Builder innerBuilder = MLDataFormats.TrashData.newBuilder().setKey(entry.getKey());
            if (entry.getValue().getLedgerId() != EMPTY_LEDGER_ID) {
                innerBuilder.setValue(entry.getValue());
            }
            builder.addComponent(innerBuilder.build());
        }
        return builder.build().toByteArray();
    }

    private Map<String, LedgerInfo> deSerialize(byte[] content) throws InvalidProtocolBufferException {
        TrashDataComponent component = TrashDataComponent.parseFrom(content);
        List<MLDataFormats.TrashData> componentList = component.getComponentList();
        Map<String, LedgerInfo> result = new ConcurrentSkipListMap<>();
        for (MLDataFormats.TrashData ele : componentList) {
            if (ele.hasValue()) {
                result.put(ele.getKey(), ele.getValue());
            } else {
                result.put(ele.getKey(), EMPTY_LEDGER_INFO);
            }
        }
        return result;
    }

    private void triggerDeleteInBackground() {
        executor.executeOrdered(name, safeRun(this::triggerDelete));
    }

    @Override
    public void triggerDelete() {
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            log.warn("[{}] is not initialized, current state: {}", name(), state);
            return;
        }
        if (!deleteMutex.tryLock()) {
            return;
        }
        if (!trashMutex.tryLock()) {
            scheduledExecutor.schedule(this::triggerDeleteInBackground, 100, TimeUnit.MILLISECONDS);
            deleteMutex.unlock();
            return;
        }
        List<TrashDeleteHelper> toDelete = getToDeleteData();
        if (toDelete.size() == 0) {
            deleteMutex.unlock();
            trashMutex.unlock();
            return;
        }
        for (TrashDeleteHelper delHelper : toDelete) {
            //unlock in asyncDeleteTrash
            asyncDeleteTrash(delHelper);
        }
    }

    @Override
    public CompletableFuture<List<Long>> getAllArchiveIndex() {
        return metadataStore.getChildren(buildParentPath()).thenComposeAsync(children -> {
            CompletableFuture<List<Long>> future = new CompletableFuture<>();
            if (CollectionUtils.isEmpty(children)) {
                future.complete(Collections.emptyList());
                return future;
            }
            List<Long> archiveIndexes = new ArrayList<>();
            for (String ele : children) {
                if (!ele.startsWith(ARCHIVE)) {
                    continue;
                }
                String indexStr = ele.split(ARCHIVE)[1];
                archiveIndexes.add(Long.parseLong(indexStr));
            }
            future.complete(archiveIndexes);
            return future;
        }, executor.chooseThread(name));
    }

    @Override
    public CompletableFuture<Map<String, LedgerInfo>> getArchiveData(final long index) {
        return metadataStore.get(buildArchivePath(index)).thenComposeAsync(optResult -> {
            CompletableFuture<Map<String, LedgerInfo>> future = new CompletableFuture<>();
            if (optResult.isPresent()) {
                byte[] content = optResult.get().getValue();
                try {
                    Map<String, LedgerInfo> result = deSerialize(content);
                    future.complete(result);
                } catch (InvalidProtocolBufferException e) {
                    future.completeExceptionally(e);
                }
            }
            return future;
        }, executor.chooseThread(name));
    }

    @Override
    public long getTrashDataSize() {
        return trashData.size();
    }

    @Override
    public long getToArchiveDataSize() {
        return toArchiveCount.get();
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        if (checkTrashPersistTask != null) {
            checkTrashPersistTask.cancel(true);
            checkTrashPersistTask = null;
        }
        asyncUpdateTrashData().whenComplete((res, e) -> {
            if (e != null) {
                callback.closeFailed((ManagedLedgerException) e, ctx);
                return;
            }
            callback.closeComplete(ctx);
        });
        STATE_UPDATER.set(this, State.Closed);
    }

    private CompletableFuture<?> increaseArchiveCountWhenDeleteFailed() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        toArchiveCount.incrementAndGet();
        managedTrashMXBean.increaseTotalNumberOfArchiveLedgers();
        updateArchiveDataIfNecessary(future);
        return future;
    }

    private void updateArchiveDataIfNecessary(final CompletableFuture<?> future) {
        if (toArchiveCount.get() < archiveDataLimitSize) {
            future.complete(null);
            return;
        }
        asyncUpdateArchiveData(future);
    }


    private String buildParentPath() {
        return PREFIX + name + "/" + type;
    }

    private String buildDeletePath() {
        return buildParentPath() + DELETE;
    }

    private String buildArchivePath(long index) {
        return buildParentPath() + ARCHIVE + index;
    }

    //take 1/10 trash to delete, if the size over 10, use 10 to delete.
    private List<TrashDeleteHelper> getToDeleteData() {
        if (trashData.size() == 0) {
            return Collections.emptyList();
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

    private void asyncUpdateArchiveData(CompletableFuture<?> future) {
        log.info("[{}] Start async update archive data", name());
        //transaction operation
        NavigableMap<String, LedgerInfo> persistDelete = new ConcurrentSkipListMap<>();
        NavigableMap<String, LedgerInfo> persistArchive = new ConcurrentSkipListMap<>();


        for (Map.Entry<String, LedgerInfo> entry : trashData.entrySet()) {
            persistArchive.put(entry.getKey(), entry.getValue());
            if (persistArchive.size() >= archiveDataLimitSize) {
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
        Map.Entry<String, LedgerInfo> lastEntry = persistArchive.lastEntry();
        OpPut opArchivePersist =
                new OpPut(buildArchivePath(TrashDeleteHelper.build(lastEntry.getKey()).ledgerId),
                        serialize(persistArchive), Optional.of(-1L), EnumSet.noneOf(CreateOption.class));
        txOps.add(opDeletePersist);
        txOps.add(opArchivePersist);
        metadataStore.batchOperation(txOps);

        opDeletePersist.getFuture().whenCompleteAsync((res, e) -> {
            if (e != null) {
                log.error("[{}] Persist trash data failed.", name(), e);
                future.completeExceptionally(getException(e));
                return;
            }
            opArchivePersist.getFuture().whenCompleteAsync((res1, e1) -> {
                if (e1 != null) {
                    log.error("[{}] Persist archive data failed.", name(), e1);
                    future.completeExceptionally(getException(e1));
                    return;
                }
                deleteStat = res;
                trashData = persistDelete;
                trashIsDirty = false;
                toArchiveCount.set(0);
                future.complete(null);
            }, executor.chooseThread(name));
        }, executor.chooseThread(name));

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
        try {
            String key = delHelper.transferToTrashKey();
            if (log.isDebugEnabled()) {
                String info = null;
                if (delHelper.isLedger()) {
                    info = String.format("[%s] Delete ledger %s success.", name(), delHelper.ledgerId);
                } else if (delHelper.isOffloadLedger()) {
                    info = String.format("[%s] Delete offload ledger %s success.", name(), delHelper.ledgerId);
                }
                log.debug(info);
            }
            trashData.remove(key);
            trashIsDirty = true;
        } finally {
            boolean continueToDelete = continueToDelete();
            deleteMutex.unlock();
            trashMutex.unlock();
            if (continueToDelete) {
                triggerDeleteInBackground();
            }
        }
    }

    private void onDeleteFailed(TrashDeleteHelper delHelper) {
        try {
            //override old key
            String key = delHelper.transferToTrashKey();
            trashData.remove(key);
            trashData.put(buildKey(delHelper.retryCount - 1, delHelper.ledgerId, delHelper.suffix),
                    delHelper.context);
            trashIsDirty = true;
            if (delHelper.retryCount - 1 == 0) {
                if (log.isWarnEnabled()) {
                    String info = null;
                    if (delHelper.isLedger()) {
                        info = String.format("[%s] Delete ledger %d reach retry limit %d.", name(), delHelper.ledgerId,
                                RETRY_COUNT);
                    } else if (delHelper.isOffloadLedger()) {
                        info = String.format("[%s] Delete offload ledger %d reach retry limit %d.", name(),
                                delHelper.ledgerId, RETRY_COUNT);
                    }
                    log.warn(info);
                }
                increaseArchiveCountWhenDeleteFailed();
            }
        } finally {
            boolean continueToDelete = continueToDelete();
            deleteMutex.unlock();
            trashMutex.unlock();
            if (continueToDelete) {
                triggerDeleteInBackground();
            }
        }
    }

    private boolean continueToDelete() {
        TrashDeleteHelper delHelper = TrashDeleteHelper.build(trashData.lastEntry().getKey());
        return delHelper.retryCount > 0;
    }

    private void asyncDeleteLedger(long ledgerId, AsyncCallbacks.DeleteLedgerCallback callback) {
        log.info("[{}] Start  async delete ledger {}", name(), ledgerId);
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", name(), ledgerId);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error delete ledger {} : {}", name(), ledgerId, BKException.getMessage(rc));
                callback.deleteLedgerFailed(ManagedLedgerImpl.createManagedLedgerException(rc), null);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleted ledger {}", name(), ledgerId);
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

        log.info("[{}] Start async delete offloaded ledger, ledgerId {} uuid {} because of the reason {}.", name(),
                ledgerId, uuid, cleanupReason);

        Map<String, String> metadataMap = Maps.newHashMap();
        metadataMap.putAll(config.getLedgerOffloader().getOffloadDriverMetadata());
        metadataMap.put("ManagedLedgerName", name);

        try {
            config.getLedgerOffloader()
                    .deleteOffloaded(ledgerId, uuid, metadataMap)
                    .whenComplete((ignored, exception) -> {
                        if (exception != null) {
                            log.warn("[{}] Failed delete offload for ledgerId {} uuid {}, (cleanup reason: {})",
                                    name(), ledgerId, uuid, cleanupReason, exception);
                            callback.deleteLedgerFailed(new ManagedLedgerException("Failed to delete offloaded ledger"),
                                    null);
                            return;
                        }
                        callback.deleteLedgerComplete(null);
                    });
        } catch (Exception e) {
            log.warn("[{}] Failed to delete offloaded ledgers.", name(), e);
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

        public static TrashDeleteHelper build(String key) {
            String[] split = key.split(TRASH_KEY_SEPARATOR);
            int retryCont = Integer.parseInt(split[0]);
            long ledgerId = Long.parseLong(split[1]);
            return new TrashDeleteHelper(retryCont, ledgerId, split[2], null);
        }

        public static TrashDeleteHelper build(String key, LedgerInfo context) {
            String[] split = key.split(TRASH_KEY_SEPARATOR);
            int retryCont = Integer.parseInt(split[0]);
            long ledgerId = Long.parseLong(split[1]);
            return new TrashDeleteHelper(retryCont, ledgerId, split[2], context);
        }

        private String transferToTrashKey() {
            return ManagedTrashImpl.buildKey(retryCount, ledgerId, suffix);
        }

        private boolean isLedger() {
            return DELETABLE_LEDGER.equals(suffix);
        }

        private boolean isOffloadLedger() {
            return DELETABLE_OFFLOADED_LEDGER.equals(suffix);
        }
    }

    public enum State {
        None,
        INITIALIZED,
        Closed,
    }


}
