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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedTrashImpl implements ManagedTrash {

    private static final Logger log = LoggerFactory.getLogger(ManagedTrashImpl.class);

    private static final String BASE_NODE = "/managed-trash";

    private static final String PREFIX = BASE_NODE + "/";

    private static final String DELETE_SUFFIX = "/delete";

    private static final String ARCHIVE = "archive-";

    private static final String ARCHIVE_SUFFIX = "/" + ARCHIVE;

    private static final String TRASH_KEY_SEPARATOR = "-";

    private static final int RETRY_COUNT = 3;

    private static final long EMPTY_LEDGER_ID = -1L;

    private static final LedgerInfo EMPTY_LEDGER_INFO = LedgerInfo.newBuilder().setLedgerId(EMPTY_LEDGER_ID).build();

    private static final AtomicReferenceFieldUpdater<ManagedTrashImpl, ManagedTrashImpl.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedTrashImpl.class, ManagedTrashImpl.State.class, "state");

    protected volatile ManagedTrashImpl.State state = null;

    private NavigableMap<TrashKey, LedgerInfo> trashData = new ConcurrentSkipListMap<>();

    //todo 未达到 archiveLimit 的 trashData 中 leftRetryCount == 0 的数据是否需要单独一个节点维护数据

    private final AtomicInteger toArchiveCount = new AtomicInteger();

    private final CallbackMutex deleteMutex = new CallbackMutex();

    private final CallbackMutex trashMutex = new CallbackMutex();

    private final CallbackMutex archiveMutex = new CallbackMutex();

    private final MetadataStore metadataStore;

    private volatile Stat deleteStat;

    private final AtomicInteger continueDeleteImmediately = new AtomicInteger();

    private final String type;

    private final String name;

    private final ManagedLedgerConfig config;

    private final OrderedScheduler scheduledExecutor;

    private final OrderedExecutor executor;

    private final BookKeeper bookKeeper;

    private final int archiveDataLimitSize;

    private final long deleteIntervalMillis;

    private volatile boolean trashIsDirty;

    private ScheduledFuture<?> checkTrashPersistTask;

    private final ManagedTrashMXBean managedTrashMXBean;

    public ManagedTrashImpl(ManagedType type, String name, MetadataStore metadataStore, ManagedLedgerConfig config,
                            OrderedScheduler scheduledExecutor, OrderedExecutor executor, BookKeeper bookKeeper) {
        this.type = type.getName();
        this.name = name;
        this.config = config;
        this.metadataStore = metadataStore;
        this.scheduledExecutor = scheduledExecutor;
        this.executor = executor;
        this.bookKeeper = bookKeeper;
        this.archiveDataLimitSize = config.getArchiveDataLimitSize();
        this.deleteIntervalMillis = TimeUnit.SECONDS.toMillis(config.getDeleteIntervalSeconds());
        this.managedTrashMXBean = new ManagedTrashMXBeanImpl(this);
        STATE_UPDATER.set(this, State.None);
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
                STATE_UPDATER.set(this, State.Initialized);
                checkTrashPersistTask =
                        scheduledExecutor.scheduleAtFixedRate(safeRun(this::persistTrashIfNecessary), 30L, 30L,
                                TimeUnit.MINUTES);
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
                STATE_UPDATER.set(this, State.Initialized);
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
    public void appendLedgerTrashData(long ledgerId, LedgerInfo context, LedgerType type)
            throws ManagedLedgerException {
        State state = STATE_UPDATER.get(this);
        if (state != State.Initialized) {
            throw ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state)));
        }
        if (context == null) {
            context = EMPTY_LEDGER_INFO;
        }
        TrashKey key = null;
        if (ManagedTrash.LedgerType.LEDGER.equals(type)) {
            key = TrashKey.buildKey(RETRY_COUNT, ledgerId, 0L, type);
        } else if (ManagedTrash.LedgerType.OFFLOAD_LEDGER.equals(type)) {
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
        doAsyncUpdateTrashData(future);
        return future;
    }

    public void asyncUpdateTrashDataInBackground(CompletableFuture<?> future) {
        executor.executeOrdered(name, safeRun(() -> doAsyncUpdateTrashData(future)));
    }

    private void doAsyncUpdateTrashData(CompletableFuture<?> future) {
        State state = STATE_UPDATER.get(this);
        if (state != State.Initialized) {
            future.completeExceptionally(ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return;
        }
        if (!trashMutex.tryLock()) {
            scheduledExecutor.schedule(() -> asyncUpdateTrashDataInBackground(future), 100, TimeUnit.MILLISECONDS);
            return;
        }
        metadataStore.put(buildDeletePath(), serialize(trashData),
                        deleteStat == null ? Optional.of(-1L) : Optional.of(deleteStat.getVersion()))
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        future.completeExceptionally(getMetaStoreException(e));
                        trashMutex.unlock();
                        return;
                    }
                    deleteStat = res;
                    trashIsDirty = false;
                    future.complete(null);
                    trashMutex.unlock();
                }, executor.chooseThread(name));
    }


    public byte[] serialize(Map<TrashKey, LedgerInfo> toPersist) {
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
        Map<String, LedgerInfo> result = new ConcurrentSkipListMap<>();
        for (Map.Entry<TrashKey, LedgerInfo> entry : to.entrySet()) {
            result.put(entry.getKey().toStringKey(), entry.getValue());
        }
        return result;
    }

    public NavigableMap<TrashKey, LedgerInfo> deSerialize(byte[] content) throws InvalidProtocolBufferException {
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
        return transferFrom(result);
    }


    private NavigableMap<TrashKey, LedgerInfo> transferFrom(Map<String, LedgerInfo> from) {
        NavigableMap<TrashKey, LedgerInfo> result = new ConcurrentSkipListMap<>();
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
        if (state != State.Initialized) {
            log.warn("[{}] is not initialized, current state: {}", name(), state);
            return;
        }
        if (!deleteMutex.tryLock()) {
            continueDeleteImmediately.incrementAndGet();
            return;
        }
        List<DelHelper> toDelete = getToDeleteData();
        if (toDelete.size() == 0) {
            continueDeleteImmediately.set(0);
            deleteMutex.unlock();
            return;
        }
        toDelete.removeIf(ele -> System.currentTimeMillis() - ele.key.lastDeleteTs < deleteIntervalMillis);

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (DelHelper delHelper : toDelete) {
            futures.add(asyncDeleteTrash(delHelper));
        }
        FutureUtil.waitForAll(futures).whenCompleteAsync((res, e) -> {
            deleteMutex.unlock();
            continueDeleteIfNecessary();
        });
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
    public CompletableFuture<Map<TrashKey, LedgerInfo>> getArchiveData(final long index) {
        return metadataStore.get(buildArchivePath(index)).thenComposeAsync(optResult -> {
            CompletableFuture<Map<TrashKey, LedgerInfo>> future = new CompletableFuture<>();
            if (optResult.isPresent()) {
                byte[] content = optResult.get().getValue();
                try {
                    Map<TrashKey, LedgerInfo> result = deSerialize(content);
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
        if (!archiveMutex.tryLock()) {
            future.complete(null);
            return;
        }
        log.info("[{}] Start async update archive data", name());

        State state = STATE_UPDATER.get(this);
        if (state != State.Initialized) {
            future.completeExceptionally(ManagedLedgerException.getManagedLedgerException(new IllegalStateException(
                    String.format("[%s] is not initialized, current state: %s", name(), state))));
            return;
        }
        asyncUpdateTrashData().thenAccept(ignore -> {
            NavigableMap<TrashKey, LedgerInfo> persistArchive = new ConcurrentSkipListMap<>();
            //here we didn't lock trashData, so maybe the persistArchive is discontinuous. such as: 1,2,3,10,12...
            for (Map.Entry<TrashKey, LedgerInfo> entry : trashData.entrySet()) {
                //in theory, the retryCount can't greater than 0.
                if (entry.getKey().retryCount > 0 || persistArchive.size() >= archiveDataLimitSize) {
                    break;
                }
                persistArchive.put(entry.getKey(), entry.getValue());
            }

            metadataStore.put(buildArchivePath(System.currentTimeMillis()), serialize(persistArchive),
                    Optional.of(-1L)).whenCompleteAsync((res, e) -> {
                if (e != null) {
                    log.error("[{}] Persist archive data failed.", name(), e);
                    future.completeExceptionally(getMetaStoreException(e));
                    deleteMutex.unlock();
                    return;
                }
                persistArchive.keySet().forEach(ele -> trashData.remove(ele));
                trashIsDirty = false;
                for (int i = 0; i < persistArchive.size(); i++) {
                    toArchiveCount.decrementAndGet();
                }
                asyncUpdateTrashData().whenComplete((res1, e1) -> {
                    if (e1 != null) {
                        future.completeExceptionally(getMetaStoreException(e1));
                        archiveMutex.unlock();
                        return;
                    }
                    future.complete(null);
                    archiveMutex.unlock();
                });
            }, executor.chooseThread(name));
        }).exceptionally(e -> {
            log.error("[{}] Persist archive data failed.", name(), e);
            future.completeExceptionally(getMetaStoreException(e));
            archiveMutex.unlock();
            return null;
        });


    }

    private CompletableFuture<?> asyncDeleteTrash(DelHelper delHelper) {
        if (delHelper.key.isLedger()) {
            CompletableFuture<?> future = asyncDeleteLedger(delHelper.key.ledgerId);
            future.whenCompleteAsync((res, e) -> {
                if (e != null) {
                    onDeleteFailed(delHelper);
                    return;
                }
                onDeleteSuccess(delHelper);
            }, executor.chooseThread(name));
            return future;
        } else if (delHelper.key.isOffloadLedger()) {
            CompletableFuture<?> future = asyncDeleteOffloadedLedger(delHelper.key.ledgerId, delHelper.context);
            future.whenCompleteAsync((res, e) -> {
                if (e != null) {
                    onDeleteFailed(delHelper);
                    return;
                }
                onDeleteSuccess(delHelper);
            }, executor.chooseThread(name));
            return future;
        }
        return CompletableFuture.completedFuture(null);
    }

    private void onDeleteSuccess(DelHelper helper) {
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
    }

    private void onDeleteFailed(DelHelper helper) {
        //override old key
        trashData.remove(helper.key);

        TrashKey newKey = TrashKey.buildKey(helper.key.retryCount - 1, helper.key.ledgerId, helper.key.msb,
                helper.key.type);
        newKey.markLastDeleteTs();
        trashData.put(newKey, helper.context);
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
    }

    private void continueDeleteIfNecessary() {
        Map.Entry<TrashKey, LedgerInfo> lastEntry = trashData.lastEntry();
        if (lastEntry == null) {
            return;
        }
        if (lastEntry.getKey().retryCount > 0) {
            if (continueDeleteImmediately.get() > 0) {
                triggerDeleteInBackground();
                continueDeleteImmediately.decrementAndGet();
            } else {
                scheduledExecutor.schedule(this::triggerDeleteInBackground, deleteIntervalMillis / 5,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    private CompletableFuture<?> asyncDeleteLedger(long ledgerId) {
        CompletableFuture<?> future = new CompletableFuture<>();
        log.info("[{}] Start  async delete ledger {}", name(), ledgerId);
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", name(), ledgerId);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error delete ledger {} : {}", name(), ledgerId, BKException.getMessage(rc));
                future.completeExceptionally(ManagedLedgerImpl.createManagedLedgerException(rc));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleted ledger {}", name(), ledgerId);
            }
            future.complete(null);
        }, null);
        return future;
    }

    private CompletableFuture<?> asyncDeleteOffloadedLedger(long ledgerId, LedgerInfo info) {
        CompletableFuture<?> future = new CompletableFuture<>();
        if (!info.getOffloadContext().hasUidMsb()) {
            future.completeExceptionally(new IllegalArgumentException(
                    String.format("[%s] Failed delete offload for ledgerId %s, can't find offload context.", name(),
                            ledgerId)));
            return future;
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
                            future.completeExceptionally(
                                    new ManagedLedgerException("Failed to delete offloaded ledger"));
                            return;
                        }
                        future.complete(null);
                    });
        } catch (Exception e) {
            log.warn("[{}] Failed to delete offloaded ledgers.", name(), e);
        }
        return future;
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

    public static class TrashKey implements Comparable<TrashKey> {

        private final int retryCount;

        private final long ledgerId;

        //the same ledgerId maybe correspond two offload storage.
        private final long msb;

        private final LedgerType type;

        private long lastDeleteTs;

        public TrashKey(int retryCount, long ledgerId, long msb, LedgerType type) {
            this.retryCount = retryCount;
            this.ledgerId = ledgerId;
            this.msb = msb;
            this.type = type;
        }

        private void markLastDeleteTs() {
            this.lastDeleteTs = System.currentTimeMillis();
        }

        private String toStringKey() {
            return retryCount + TRASH_KEY_SEPARATOR + ledgerId + TRASH_KEY_SEPARATOR + msb + TRASH_KEY_SEPARATOR
                    + type;
        }

        public static TrashKey buildKey(int retryCount, long ledgerId, long msb, LedgerType type) {
            return new TrashKey(retryCount, ledgerId, msb, type);
        }

        public static TrashKey buildKey(String strKey) {
            String[] split = strKey.split(TRASH_KEY_SEPARATOR);
            int retryCount = Integer.parseInt(split[0]);
            long ledgerId = Long.parseLong(split[1]);
            long msb = Long.parseLong(split[2]);
            LedgerType type = LedgerType.valueOf(split[3]);
            return new TrashKey(retryCount, ledgerId, msb, type);
        }

        public int getRetryCount() {
            return retryCount;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long getMsb() {
            return msb;
        }

        public LedgerType getType() {
            return type;
        }

        private boolean isLedger() {
            return LedgerType.LEDGER.equals(type);
        }

        private boolean isOffloadLedger() {
            return LedgerType.OFFLOAD_LEDGER.equals(type);
        }

        @Override
        public int compareTo(TrashKey other) {
            if (other == this) {
                return 0;
            }
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
            return this.type.compareTo(other.type);
        }
    }

    public enum State {
        None,
        Initialized,
        Closed,
    }


}
