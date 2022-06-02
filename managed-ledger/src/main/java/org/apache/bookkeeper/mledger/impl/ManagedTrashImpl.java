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
import java.util.HashMap;
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

    private static final String DELETE_SUFFIX = "/delete";

    private static final String ARCHIVE = "archive-";

    private static final String ARCHIVE_SUFFIX = "/" + ARCHIVE;

    private static final String TRASH_KEY_SEPARATOR = "-";

    private static final int RETRY_COUNT = 9;

    private static final long EMPTY_LEDGER_ID = -1L;

    private static final LedgerInfo EMPTY_LEDGER_INFO = LedgerInfo.newBuilder().setLedgerId(EMPTY_LEDGER_ID).build();

    private static final AtomicReferenceFieldUpdater<ManagedTrashImpl, ManagedTrashImpl.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedTrashImpl.class, ManagedTrashImpl.State.class, "state");

    protected volatile ManagedTrashImpl.State state = null;

    private NavigableMap<TrashKey, LedgerInfo> trashData = new ConcurrentSkipListMap<>();

    //todo 未达到 archiveLimit 的 trashData 中 leftRetryCount == 0 的数据是否需要单独一个节点维护数据

    private final AtomicInteger toArchiveCount = new AtomicInteger();

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
                toArchiveCount.set(calculateArchiveCount());
                future.complete(null);
                checkTrashPersistTask =
                        scheduledExecutor.scheduleAtFixedRate(safeRun(this::persistTrashIfNecessary), 30L, 30L,
                                TimeUnit.MINUTES);
                STATE_UPDATER.set(this, State.INITIALIZED);
                triggerDeleteInBackground();
            } catch (InvalidProtocolBufferException exc) {
                future.completeExceptionally(ManagedLedgerException.getManagedLedgerException(exc));
            }
        }, scheduledExecutor.chooseThread(name));
        return future;
    }

    private void persistTrashIfNecessary() {
        if (trashIsDirty) {
            asyncUpdateTrashData();
        }
    }

    private int calculateArchiveCount() {
        int toArchiveCount = 0;
        for (TrashKey key : trashData.keySet()) {
            if (key.retryCount != 0) {
                break;
            }
            toArchiveCount++;
        }
        return toArchiveCount;
    }

    @Override
    public void appendLedgerTrashData(long ledgerId, LedgerInfo context, String type)
            throws ManagedLedgerException {
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            throw ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state)));
        }
        if (context == null) {
            context = EMPTY_LEDGER_INFO;
        }
        TrashKey key = null;
        if (ManagedTrash.LEDGER.equals(type)) {
            key = TrashKey.buildKey(RETRY_COUNT, ledgerId, 0L, type);
        } else if (ManagedTrash.OFFLOADED_LEDGER.equals(type)) {
            key = TrashKey.buildKey(RETRY_COUNT, ledgerId, context.getOffloadContext().getUidMsb(), type);
        }
        trashData.put(key, context);
        managedTrashMXBean.increaseTotalNumberOfDeleteLedgers();
        trashIsDirty = true;

    }

    @Override
    public CompletableFuture<?> asyncUpdateTrashData() {
        log.info("{} Start async update trash data", name());
        CompletableFuture<Void> future = new CompletableFuture<>();
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            future.completeExceptionally(ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return future;
        }
        metadataStore.put(buildDeletePath(), serialize(trashData),
                        deleteStat == null ? Optional.of(-1L) : Optional.of(deleteStat.getVersion()))
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        future.completeExceptionally(getMetaStoreException(e));
                        return;
                    }
                    deleteStat = res;
                    trashIsDirty = false;
                    trashPersistMutex.unlock();
                    future.complete(null);
                }, executor.chooseThread(name));
        return future;
    }


    private byte[] serialize(Map<TrashKey, LedgerInfo> toPersist) {
        Map<String, LedgerInfo> transfer = transferTo(toPersist);
        TrashDataComponent.Builder builder = TrashDataComponent.newBuilder();
        for (Map.Entry<String, LedgerInfo> entry : transfer.entrySet()) {
            MLDataFormats.TrashData.Builder innerBuilder = MLDataFormats.TrashData.newBuilder().setKey(entry.getKey());
            if (entry.getValue().getLedgerId() != EMPTY_LEDGER_ID) {
                innerBuilder.setValue(entry.getValue());
            }
            builder.addComponent(innerBuilder.build());
        }
        return builder.build().toByteArray();
    }

    private Map<String, LedgerInfo> transferTo(Map<TrashKey, LedgerInfo> to) {
        Map<String, LedgerInfo> result = new HashMap<>();
        for (Map.Entry<TrashKey, LedgerInfo> entry : to.entrySet()) {
            result.put(entry.getKey().toStringKey(), entry.getValue());
        }
        return result;
    }

    private Map<TrashKey, LedgerInfo> deSerialize(byte[] content) throws InvalidProtocolBufferException {
        TrashDataComponent component = TrashDataComponent.parseFrom(content);
        List<MLDataFormats.TrashData> componentList = component.getComponentList();
        Map<String, LedgerInfo> result = new HashMap<>();
        for (MLDataFormats.TrashData ele : componentList) {
            if (ele.hasValue()) {
                result.put(ele.getKey(), ele.getValue());
            } else {
                result.put(ele.getKey(), EMPTY_LEDGER_INFO);
            }
        }
        return transferFrom(result);
    }


    private Map<TrashKey, LedgerInfo> transferFrom(Map<String, LedgerInfo> from) {
        Map<TrashKey, LedgerInfo> result = new HashMap<>();
        for (Map.Entry<String, LedgerInfo> entry : from.entrySet()) {
            result.put(TrashKey.buildKey(entry.getKey()), entry.getValue());
        }
        return result;
    }

    @Override
    public void triggerDeleteInBackground() {
        executor.executeOrdered(name, safeRun(this::triggerDelete));
    }

    private void triggerDelete() {
        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            log.warn("[{}] is not initialized, current state: {}", name(), state);
            return;
        }
        if (!deleteMutex.tryLock()) {
            return;
        }
        List<DelHelper> toDelete = getToDeleteData();

        if (toDelete.size() == 0) {
            deleteMutex.unlock();
            return;
        }
        for (DelHelper delHelper : toDelete) {
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
    public CompletableFuture<List<LedgerInfo>> getArchiveData(final long index) {
        return metadataStore.get(buildArchivePath(index)).thenComposeAsync(optResult -> {
            CompletableFuture<List<LedgerInfo>> future = new CompletableFuture<>();
            if (optResult.isPresent()) {
                byte[] content = optResult.get().getValue();
                try {
                    Map<TrashKey, LedgerInfo> result = deSerialize(content);
                    future.complete(result.values().stream().toList());
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
        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.closeComplete(ctx);
            return;
        }
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
        return PREFIX + type + "/" + name;
    }

    private String buildDeletePath() {
        return buildParentPath() + DELETE_SUFFIX;
    }

    private String buildArchivePath(long index) {
        return buildParentPath() + ARCHIVE_SUFFIX + index;
    }

    //take 1/10 trash to delete, if the size over 10, use 10 to delete.
    private List<DelHelper> getToDeleteData() {
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
        List<DelHelper> toDelete = new ArrayList<>(batchSize);
        for (Map.Entry<TrashKey, LedgerInfo> entry : trashData.descendingMap().entrySet()) {
            //if last retryCount is zero, the before data retryCount is zero too.
            if (entry.getKey().retryCount == 0) {
                break;
            }
            toDelete.add(DelHelper.buildHelper(entry.getKey(), entry.getValue()));
            if (toDelete.size() == batchSize) {
                break;
            }
        }
        return toDelete;
    }

    private void asyncUpdateArchiveData(CompletableFuture<?> future) {
        log.info("[{}] Start async update archive data", name());

        State state = STATE_UPDATER.get(this);
        if (state != State.INITIALIZED) {
            future.completeExceptionally(ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return;
        }
        //transaction operation
        NavigableMap<TrashKey, LedgerInfo> persistDelete = new ConcurrentSkipListMap<>();
        NavigableMap<TrashKey, LedgerInfo> persistArchive = new ConcurrentSkipListMap<>();


        for (Map.Entry<TrashKey, LedgerInfo> entry : trashData.entrySet()) {
            persistArchive.put(entry.getKey(), entry.getValue());
            if (persistArchive.size() >= archiveDataLimitSize) {
                break;
            }
        }

        persistDelete.putAll(trashData);
        for (Map.Entry<TrashKey, LedgerInfo> entry : persistArchive.entrySet()) {
            persistDelete.remove(entry.getKey());
        }
        //build delete persist operation
        List<MetadataOp> txOps = new ArrayList<>(2);
        OpPut opDeletePersist = new OpPut(buildDeletePath(), serialize(persistDelete),
                deleteStat == null ? Optional.of(-1L) : Optional.of(deleteStat.getVersion()),
                EnumSet.noneOf(CreateOption.class));
        //build archive persist operation
        Map.Entry<TrashKey, LedgerInfo> lastEntry = persistArchive.lastEntry();
        OpPut opArchivePersist =
                new OpPut(buildArchivePath(lastEntry.getKey().ledgerId), serialize(persistArchive), Optional.of(-1L),
                        EnumSet.noneOf(CreateOption.class));
        txOps.add(opDeletePersist);
        txOps.add(opArchivePersist);
        metadataStore.batchOperation(txOps);

        opDeletePersist.getFuture().whenCompleteAsync((res, e) -> {
            if (e != null) {
                log.error("[{}] Persist trash data failed.", name(), e);
                future.completeExceptionally(getMetaStoreException(e));
                return;
            }
            opArchivePersist.getFuture().whenCompleteAsync((res1, e1) -> {
                if (e1 != null) {
                    log.error("[{}] Persist archive data failed.", name(), e1);
                    future.completeExceptionally(getMetaStoreException(e1));
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

    private void asyncDeleteTrash(DelHelper delHelper) {
        if (delHelper.key.isLedger()) {
            asyncDeleteLedger(delHelper.key.ledgerId, new AsyncCallbacks.DeleteLedgerCallback() {
                @Override
                public void deleteLedgerComplete(Object ctx) {
                    onDeleteSuccess(delHelper);
                }

                @Override
                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    onDeleteFailed(delHelper);
                }
            });
        } else if (delHelper.key.isOffloadLedger()) {
            asyncDeleteOffloadedLedger(delHelper.key.ledgerId, delHelper.context,
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

    private void onDeleteSuccess(DelHelper helper) {
        try {
            if (log.isDebugEnabled()) {
                String info = null;
                if (helper.key.isLedger()) {
                    info = String.format("[%s] Delete ledger %s success.", name(), helper.key.ledgerId);
                } else if (helper.key.isOffloadLedger()) {
                    info = String.format("[%s] Delete offload ledger %s success.", name(), helper.key.ledgerId);
                }
                log.debug(info);
            }
            trashData.remove(helper.key);
            trashIsDirty = true;
        } finally {
            boolean continueToDelete = continueToDelete();
            deleteMutex.unlock();
            if (continueToDelete) {
                triggerDeleteInBackground();
            }
        }
    }

    private void onDeleteFailed(DelHelper helper) {
        try {
            //override old key
            trashData.remove(helper.key);
            trashData.put(TrashKey.buildKey(helper.key.retryCount - 1, helper.key.ledgerId, helper.key.msb,
                    helper.key.suffix), helper.context);
            trashIsDirty = true;
            if (helper.key.retryCount - 1 == 0) {
                if (log.isWarnEnabled()) {
                    String info = null;
                    if (helper.key.isLedger()) {
                        info = String.format("[%s] Delete ledger %d reach retry limit %d.", name(), helper.key.ledgerId,
                                RETRY_COUNT);
                    } else if (helper.key.isOffloadLedger()) {
                        info = String.format("[%s] Delete offload ledger %d reach retry limit %d.", name(),
                                helper.key.ledgerId, RETRY_COUNT);
                    }
                    log.warn(info);
                }
                increaseArchiveCountWhenDeleteFailed();
            }
        } finally {
            boolean continueToDelete = continueToDelete();
            deleteMutex.unlock();
            if (continueToDelete) {
                triggerDeleteInBackground();
            }
        }
    }

    private boolean continueToDelete() {
        return trashData.lastEntry().getKey().retryCount > 0;
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

    private static ManagedLedgerException.MetaStoreException getMetaStoreException(Throwable t) {
        if (t.getCause() instanceof MetadataStoreException.BadVersionException) {
            return new ManagedLedgerException.BadVersionException(t.getMessage());
        } else {
            return new ManagedLedgerException.MetaStoreException(t);
        }
    }

    private static class DelHelper {
        private final TrashKey key;
        private final LedgerInfo context;

        public DelHelper(TrashKey key, LedgerInfo context) {
            this.key = key;
            this.context = context;
        }

        public static DelHelper buildHelper(TrashKey key, LedgerInfo context) {
            return new DelHelper(key, context);
        }
    }


    private static class TrashKey implements Comparable<TrashKey> {

        private final int retryCount;

        private final long ledgerId;

        private final long msb;

        private final String suffix;

        public TrashKey(int retryCount, long ledgerId, long msb, String suffix) {
            this.retryCount = retryCount;
            this.ledgerId = ledgerId;
            this.msb = msb;
            this.suffix = suffix;
        }

        private String toStringKey() {
            return retryCount + TRASH_KEY_SEPARATOR + ledgerId + TRASH_KEY_SEPARATOR + msb + TRASH_KEY_SEPARATOR
                    + suffix;
        }

        public static TrashKey buildKey(int retryCount, long ledgerId, long msb, String suffix) {
            return new TrashKey(retryCount, ledgerId, msb, suffix);
        }

        public static TrashKey buildKey(String strKey) {
            String[] split = strKey.split(TRASH_KEY_SEPARATOR);
            int retryCount = Integer.parseInt(split[0]);
            long ledgerId = Long.parseLong(split[1]);
            long msb = Long.parseLong(split[2]);
            String suffix = split[3];
            return new TrashKey(retryCount, ledgerId, msb, suffix);
        }

        private boolean isLedger() {
            return LEDGER.equals(suffix);
        }

        private boolean isOffloadLedger() {
            return OFFLOADED_LEDGER.equals(suffix);
        }

        @Override
        public int compareTo(TrashKey other) {
            int c1 = this.retryCount - other.retryCount;
            if (c1 != 0) {
                return c1;
            }
            long c2 = this.ledgerId - other.ledgerId;
            if (c2 != 0) {
                return c2 > 0 ? 1 : -1;
            }
            long c3 = this.msb - other.msb;
            if (c3 != 0) {
                return c3 > 0 ? 1 : -1;
            }
            return this.suffix.compareTo(other.suffix);
        }
    }

    public enum State {
        None,
        INITIALIZED,
        Closed,
    }


}
