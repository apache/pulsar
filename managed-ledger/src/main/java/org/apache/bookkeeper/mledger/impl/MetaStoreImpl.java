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
package org.apache.bookkeeper.mledger.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.proto.CompressionType;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfoMetadata;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfoMetadata;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class MetaStoreImpl implements MetaStore, Consumer<Notification> {

    private static final String BASE_NODE = "/managed-ledgers";
    private static final String PREFIX = BASE_NODE + "/";

    private final MetadataStore store;
    private final OrderedExecutor executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<Optional<GetResult>>> publicationReads = ConcurrentHashMap.newKeySet();
    private final Set<Runnable> pendingCursorCallbacks = ConcurrentHashMap.newKeySet();

    private static final int MAGIC_MANAGED_INFO_METADATA = 0x4778; // 0100 0111 0111 1000
    private final MetadataCompressionConfig ledgerInfoCompressionConfig;
    private final MetadataCompressionConfig cursorInfoCompressionConfig;

    private final Map<String, UpdateCallback<ManagedLedgerInfo>> managedLedgerInfoUpdateCallbackMap;

    // Reusable LightProto objects for compression metadata serialization/deserialization.
    // These are used from MetaStore callbacks which are dispatched on the ordered executor,
    // so each thread gets its own instance via ThreadLocal.
    private static final ThreadLocal<ManagedLedgerInfoMetadata> tlMlInfoMetadata =
            ThreadLocal.withInitial(ManagedLedgerInfoMetadata::new);
    private static final ThreadLocal<ManagedCursorInfoMetadata> tlCursorInfoMetadata =
            ThreadLocal.withInitial(ManagedCursorInfoMetadata::new);

    public MetaStoreImpl(MetadataStore store, OrderedExecutor executor) {
        this.store = store;
        this.executor = executor;
        this.ledgerInfoCompressionConfig = MetadataCompressionConfig.noCompression;
        this.cursorInfoCompressionConfig = MetadataCompressionConfig.noCompression;
        managedLedgerInfoUpdateCallbackMap = new ConcurrentHashMap<>();
        if (store != null) {
            store.registerListener(this);
        }
    }

    public MetaStoreImpl(MetadataStore store, OrderedExecutor executor,
                         MetadataCompressionConfig ledgerInfoCompressionConfig,
                         MetadataCompressionConfig cursorInfoCompressionConfig) {
        this.store = store;
        this.executor = executor;
        this.ledgerInfoCompressionConfig = ledgerInfoCompressionConfig;
        this.cursorInfoCompressionConfig = cursorInfoCompressionConfig;
        managedLedgerInfoUpdateCallbackMap = new ConcurrentHashMap<>();
        if (store != null) {
            store.registerListener(this);
        }
    }

    @Override
    public void getManagedLedgerInfo(String ledgerName, boolean createIfMissing, Map<String, String> properties,
            MetaStoreCallback<ManagedLedgerInfo> callback) {
        // Try to get the content or create an empty node
        String path = PREFIX + ledgerName;
        store.get(path)
                .thenAcceptAsync(optResult -> {
                    if (optResult.isPresent()) {
                        ManagedLedgerInfo info;
                        try {
                            info = parseManagedLedgerInfo(optResult.get().getValue());
                            info = updateMLInfoTimestamp(info);
                            callback.operationComplete(info, optResult.get().getStat());
                        } catch (Exception e) {
                            callback.operationFailed(getException(e));
                        }
                    } else {
                        // Z-node doesn't exist
                        if (createIfMissing) {
                            log.info("Creating '{}'", path);

                            store.put(path, new byte[0], Optional.of(-1L))
                                    .thenAccept(stat -> {
                                        ManagedLedgerInfo ledgerBuilder = new ManagedLedgerInfo();
                                        if (properties != null) {
                                            properties.forEach((k, v) -> {
                                                ledgerBuilder.addProperty()
                                                        .setKey(k)
                                                        .setValue(v);
                                            });
                                        }
                                        callback.operationComplete(ledgerBuilder, stat);
                                    }).exceptionally(ex -> {
                                        if (FutureUtil.unwrapCompletionException(ex)
                                                instanceof MetadataStoreException.BadVersionException) {
                                            // The z-node was created concurrently after the read above returned
                                            // "not found". This happens for example when the broker creates the
                                            // partitions of a topic while the same topic is being loaded. Read the
                                            // z-node back instead of failing the managed ledger initialization.
                                            log.info("Managed ledger path '{}' was concurrently created, reading it "
                                                    + "back", path);
                                            getManagedLedgerInfo(ledgerName, false, null, callback);
                                            return null;
                                        }
                                        callback.operationFailed(getException(ex));
                                        return null;
                                    });
                        } else {
                            // Tried to open a managed ledger but it doesn't exist and we shouldn't creating it at this
                            // point
                            callback.operationFailed(new MetadataNotFoundException("Managed ledger not found"));
                        }
                    }
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    try {
                        executor.executeOrdered(ledgerName,
                                () -> callback.operationFailed(getException(ex)));
                    } catch (RejectedExecutionException e) {
                        //executor maybe shutdown, use common pool to run callback.
                        CompletableFuture.runAsync(() -> callback.operationFailed(getException(ex)));
                    }
                    return null;
                });
    }

    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        CompletableFuture<Map<String, String>> result = new CompletableFuture<>();
        getManagedLedgerInfo(name, false, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                HashMap<String, String> propertiesMap = new HashMap<>(mlInfo.getPropertiesCount());
                if (mlInfo.getPropertiesCount() > 0) {
                    for (int i = 0; i < mlInfo.getPropertiesCount(); i++) {
                        propertiesMap.put(mlInfo.getPropertyAt(i).getKey(),
                                mlInfo.getPropertyAt(i).getValue());
                    }
                }
                result.complete(propertiesMap);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                if (e instanceof MetadataNotFoundException) {
                    result.complete(new HashMap<>());
                } else {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    @Override
    public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
            MetaStoreCallback<Void> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating metadata version={} with content={}", ledgerName, stat, mlInfo);
        }

        String path = PREFIX + ledgerName;
        store.put(path, compressLedgerInfo(mlInfo), Optional.of(stat.getVersion()))
                .thenAcceptAsync(newVersion -> callback.operationComplete(null, newVersion),
                        executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName,
                            () -> callback.operationFailed(getException(ex)));
                    return null;
                });
    }

    @Override
    public void getCursors(String ledgerName, MetaStoreCallback<List<String>> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Get cursors list", ledgerName);
        }

        String path = PREFIX + ledgerName;
        store.getChildren(path)
                .thenAcceptAsync(cursors -> callback.operationComplete(cursors, null), executor
                        .chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName,
                            () -> callback.operationFailed(getException(ex)));
                    return null;
                });
    }

    @Override
    public void asyncGetCursorInfo(String ledgerName, String cursorName,
            MetaStoreCallback<ManagedCursorInfo> callback) {
        String path = PREFIX + ledgerName + "/" + cursorName;
        if (log.isDebugEnabled()) {
            log.debug("Reading from {}", path);
        }

        store.get(path)
                .thenAcceptAsync(optRes -> {
                    if (optRes.isPresent()) {
                        try {
                            ManagedCursorInfo info = parseManagedCursorInfo(optRes.get().getValue());
                            callback.operationComplete(info, optRes.get().getStat());
                        } catch (Exception e) {
                            callback.operationFailed(getException(e));
                        }
                    } else {
                        callback.operationFailed(new MetadataNotFoundException("Cursor metadata not found"));
                    }
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName,
                            () -> callback.operationFailed(getException(ex)));
                    return null;
                });
    }

    @Override
    public void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat stat,
            MetaStoreCallback<Void> callback) {
        asyncUpdateCursorInfo(ledgerName, cursorName, info, stat, null, CursorWrite.UPDATE, callback);
    }

    @Override
    public void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat stat,
            Duration verificationTimeout, CursorWrite write, MetaStoreCallback<Void> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Updating cursor info ledgerId={} mark-delete={}:{} lastActive={}",
                    ledgerName, cursorName, info.getCursorsLedgerId(), info.getMarkDeleteLedgerId(),
                    info.getMarkDeleteEntryId(), info.getLastActive());
        }

        String path = PREFIX + ledgerName + "/" + cursorName;

        if (log.isDebugEnabled()) {
            log.debug("[{}] {} consumer {} on meta-data store with {}", ledgerName,
                    stat == null ? "Creating" : "Updating", cursorName, info);
        }
        FutureUtil.supplySafely(() -> {
            // Generate an ID only for a new logical write. Caller-initiated retries must preserve this ID,
            // payload and expected version so an already-committed write can be recognized safely.
            if (info.getPublication().isEmpty()) {
                info.setPublication(UUID.randomUUID().toString());
            }
            return prepareCursorWrite(ledgerName, path, compressCursorInfo(info), stat, verificationTimeout,
                    write == CursorWrite.SWITCH_LEDGER ? info.getCursorsLedgerId() : -1);
        })
                .whenComplete((written, error) -> dispatchCursorCallback(ledgerName, () -> {
                    if (error == null) {
                        callback.operationComplete(null, written);
                    } else {
                        callback.operationFailed(getException(error));
                    }
                }));
    }

    private void dispatchCursorCallback(String ledgerName, Runnable callback) {
        // Register before checking closed: shutdown may discard even an accepted executor task.
        pendingCursorCallbacks.add(callback);
        if (!closed.get() && !executor.isShutdown()) {
            try {
                executor.executeOrdered(ledgerName, () -> deliverCursorCallback(callback));
                return;
            } catch (RejectedExecutionException ignored) {
                // The write already has a result. Deliver it on this thread if the executor stopped.
            }
        }
        deliverCursorCallback(callback);
    }

    private void deliverCursorCallback(Runnable callback) {
        // Only the ordered task or the closing thread can claim this result.
        if (pendingCursorCallbacks.remove(callback)) {
            try {
                callback.run();
            } catch (RuntimeException e) {
                log.error("Cursor metadata callback failed", e);
            }
        }
    }

    private CompletableFuture<Stat> prepareCursorWrite(String ledgerName, String path, byte[] content, Stat expected,
                                                       Duration timeout, long candidate) {
        // ZooKeeper create responses omit ctime. Pin the node incarnation before submitting a retryable switch.
        if (candidate >= 0 && expected != null && expected.getCreationTimestamp() == 0
                && store instanceof ZKMetadataStore zkStore) {
            return readCursorPublication(ledgerName, path, zkStore.getZkClient(), timeout).thenCompose(value -> {
                Stat pinned = value.map(GetResult::getStat)
                        .filter(stat -> !stat.isEphemeral() && stat.getVersion() == expected.getVersion())
                        .orElse(expected);
                return putCursorInfo(ledgerName, path, content, pinned, timeout, candidate);
            });
        }
        return putCursorInfo(ledgerName, path, content, expected, timeout, candidate);
    }

    private CursorPublicationOutcomeUnknownException unknownPublication(Throwable cause, ZKMetadataStore zkStore,
            String ledgerName, String path, byte[] content, Stat expected) {
        return new CursorPublicationOutcomeUnknownException(getException(cause), expected,
                (timeout, stillOwner, callback) ->
                        retryCursorPublication(zkStore, ledgerName, path, content, expected, timeout,
                                stillOwner, callback));
    }

    private void retryCursorPublication(ZKMetadataStore zkStore, String ledgerName, String path, byte[] content,
            Stat expected, Duration timeout, Supplier<CompletableFuture<Boolean>> stillOwner,
            MetaStoreCallback<ManagedCursorInfo> callback) {
        readCursorPublication(ledgerName, path, zkStore.getZkClient(), timeout).thenCompose(value -> {
            GetResult stored = value.orElse(null);
            if (expected == null || expected.getCreationTimestamp() <= 0 || stored == null
                    || stored.getStat().isEphemeral()
                    || stored.getStat().getCreationTimestamp() != expected.getCreationTimestamp()
                    || stored.getStat().getVersion() < expected.getVersion()) {
                return CompletableFuture.failedFuture(new MetadataStoreException.BadVersionException(path));
            }
            return stillOwner.get().thenCompose(owner -> {
                if (!owner) {
                    return CompletableFuture.failedFuture(new MetaStoreException("Lost ownership of " + path));
                }
                if (stored.getStat().getVersion() > expected.getVersion()) {
                    // This version fences the original CAS. The durable pointer decides which ledger to keep.
                    zkStore.accept(new Notification(NotificationType.Modified, path));
                    return CompletableFuture.completedFuture(stored);
                }
                // A late original write may still win. Both attempts use exactly the same CAS and publication bytes.
                return putCursorInfo(ledgerName, path, content, expected, timeout, -1)
                        .thenApply(stat -> new GetResult(content, stat));
            });
        }).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((stored, error) -> dispatchCursorCallback(ledgerName, () -> {
                    if (error != null) {
                        callback.operationFailed(getException(error));
                        return;
                    }
                    ManagedCursorInfo info;
                    try {
                        info = parseManagedCursorInfo(stored.getValue());
                    } catch (Exception e) {
                        callback.operationFailed(getException(e));
                        return;
                    }
                    callback.operationComplete(info, stored.getStat());
                }));
    }

    private CompletableFuture<Stat> putCursorInfo(String ledgerName, String path, byte[] content, Stat expected,
                                                 Duration verificationTimeout, long candidateLedgerId) {
        if (closed.get() || executor.isShutdown()) {
            return CompletableFuture.failedFuture(new MetadataStoreException.AlreadyClosedException());
        }
        long expectedVersion = expected == null ? -1 : expected.getVersion();
        return FutureUtil.supplySafely(() -> store.put(path, content, Optional.of(expectedVersion)))
                .exceptionallyCompose(error -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(error);
                    // Other metadata implementations retain their existing error handling.
                    if (!(store instanceof ZKMetadataStore zkStore)) {
                        return CompletableFuture.failedFuture(cause);
                    }
                    if (!(cause instanceof MetadataStoreException.BadVersionException
                            || cause.getCause() instanceof KeeperException ke && isUncertainResult(ke.code()))) {
                        return CompletableFuture.failedFuture(candidateLedgerId < 0 ? cause
                                : unknownPublication(cause, zkStore, ledgerName, path, content, expected));
                    }
                    var verification = FutureUtil.supplySafely(() ->
                            readCursorPublication(ledgerName, path, zkStore.getZkClient(), verificationTimeout))
                            .thenApply(value -> {
                                GetResult stored = value.orElse(null);
                                Stat confirmed = confirmCursorPublication(zkStore, path, expectedVersion,
                                        content, stored);
                                if (confirmed == null && candidateLedgerId >= 0
                                        && !canDeleteCursorLedger(expected, stored, candidateLedgerId)) {
                                    throw new CompletionException(cause);
                                }
                                return confirmed;
                            });
                    if (candidateLedgerId >= 0) {
                        // After put, only a completed verification can establish that deletion is safe.
                        verification = verification.exceptionallyCompose(ex -> CompletableFuture.failedFuture(
                                unknownPublication(ex, zkStore, ledgerName, path, content, expected)));
                    }
                    return verification.thenCompose(confirmed -> confirmed == null
                            ? CompletableFuture.failedFuture(cause) : CompletableFuture.completedFuture(confirmed));
                });
    }

    private static Stat confirmCursorPublication(ZKMetadataStore store, String path, long expectedVersion,
                                                 byte[] content, GetResult stored) {
        if (stored == null || stored.getStat().isEphemeral() || stored.getStat().getVersion() != expectedVersion + 1) {
            return null;
        }
        // The unique publication ID is part of these bytes.
        if (!Arrays.equals(content, stored.getValue())) {
            log.warn("Stored cursor bytes differ from publication at expected version {} for {}", expectedVersion + 1,
                    path);
            return null;
        }
        // A lost write response can also mean we missed the watch. Invalidate caches before reporting success.
        store.accept(new Notification(stored.getStat().isFirstVersion()
                ? NotificationType.Created : NotificationType.Modified, path));
        return stored.getStat();
    }

    private boolean canDeleteCursorLedger(Stat expected, GetResult stored, long candidateLedgerId) {
        // A read at the original version can precede a late commit. Require the same persistent node
        // at an advanced version (which fences the attempted CAS) and a pointer to a different ledger.
        if (expected == null || expected.isEphemeral() || expected.getCreationTimestamp() <= 0 || stored == null
                || stored.getStat().isEphemeral()
                || stored.getStat().getCreationTimestamp() != expected.getCreationTimestamp()
                || stored.getStat().getVersion() <= expected.getVersion()) {
            return false;
        }
        try {
            var info = parseManagedCursorInfo(stored.getValue());
            return info.hasCursorsLedgerId() && info.getCursorsLedgerId() != candidateLedgerId;
        } catch (Exception e) {
            log.warn("Could not verify the cursor ledger reference for {}", expected.getPath(), e);
            return false;
        }
    }

    private static boolean isUncertainResult(Code code) {
        return code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONMOVED
                || code == Code.SESSIONEXPIRED || code == Code.REQUESTTIMEOUT;
    }

    private CompletableFuture<Optional<GetResult>> readCursorPublication(
            String ledgerName, String path, ZooKeeper zk, Duration verificationTimeout) {
        var result = new CompletableFuture<Optional<GetResult>>();
        publicationReads.add(result);
        result.whenComplete((__, error) -> publicationReads.remove(result));
        Backoff backoff = Backoff.builder().initialDelay(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(1)).build();
        // Expire the same future that guards every retry, including when getData never replies.
        var verified = result;
        if (verificationTimeout != null) {
            verified = result.orTimeout(TimeUnit.MILLISECONDS.convert(verificationTimeout), TimeUnit.MILLISECONDS)
                    .exceptionally(error -> {
                        if (error instanceof TimeoutException) {
                            throw new CompletionException(new TimeoutException(
                                    "Verification timed out for " + path + ": cursor publication outcome is unknown"));
                        }
                        throw new CompletionException(error);
                    });
        }
        readCursorPublication(ledgerName, path, zk, result, backoff);
        return verified;
    }

    private void readCursorPublication(String ledgerName, String path, ZooKeeper zk,
                                       CompletableFuture<Optional<GetResult>> result, Backoff backoff) {
        if (result.isDone()) {
            return;
        }
        if (closed.get() || executor.isShutdown()) {
            result.completeExceptionally(new MetadataStoreException.AlreadyClosedException());
            return;
        }
        try {
            // Use a single-node read: a valid cursor can exceed ZooKeeper's multiRead response-size limit.
            zk.getData(path, false, (rc, __, ctx, bytes, stat) -> {
                try {
                    executor.executeOrdered(ledgerName, () -> {
                        if (result.isDone()) {
                            return;
                        }
                        Code code = Code.get(rc);
                        if (code == Code.OK) {
                            result.complete(Optional.of(new GetResult(bytes,
                                    new Stat(path, stat.getVersion(), stat.getCtime(), stat.getMtime(),
                                            stat.getEphemeralOwner() != 0,
                                            stat.getEphemeralOwner() == zk.getSessionId()))));
                        } else if (code == Code.NONODE) {
                            result.complete(Optional.empty());
                        } else if (isUncertainResult(code) || code == Code.THROTTLEDOP) {
                            // Keep the original write pending so repeated openCursor calls join it. Retry only
                            // verification, never another write. The one-second cap bounds retention after close.
                            CompletableFuture.delayedExecutor(backoff.next().toMillis(), TimeUnit.MILLISECONDS).execute(
                                    () -> readCursorPublication(ledgerName, path, zk, result, backoff));
                        } else {
                            result.completeExceptionally(
                                    new MetadataStoreException(KeeperException.create(code, path)));
                        }
                    });
                } catch (RejectedExecutionException e) {
                    result.completeExceptionally(new MetadataStoreException.AlreadyClosedException(e));
                }
            }, null);
        } catch (RuntimeException e) {
            result.completeExceptionally(new MetadataStoreException(e));
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            var error = new MetadataStoreException.AlreadyClosedException();
            publicationReads.forEach(read -> read.completeExceptionally(error));
            pendingCursorCallbacks.forEach(this::deliverCursorCallback);
        }
    }

    @Override
    public void asyncRemoveCursor(String ledgerName, String cursorName, MetaStoreCallback<Void> callback) {
        String path = PREFIX + ledgerName + "/" + cursorName;
        log.info("[{}] Remove cursor={}", ledgerName, cursorName);

        store.delete(path, Optional.empty())
                .thenAcceptAsync(v -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] cursor delete done", ledgerName, cursorName);
                    }
                    callback.operationComplete(null, null);
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, () -> {
                        Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                        if (actEx instanceof MetadataStoreException.NotFoundException){
                            log.info("[{}] [{}] cursor delete done because it did not exist.", ledgerName, cursorName);
                            callback.operationComplete(null, null);
                            return;
                        }
                        callback.operationFailed(getException(ex));
                    });
                    return null;
                });
    }

    @Override
    public void removeManagedLedger(String ledgerName, MetaStoreCallback<Void> callback) {
        log.info("[{}] Remove ManagedLedger", ledgerName);

        String path = PREFIX + ledgerName;
        store.delete(path, Optional.empty())
                .thenAcceptAsync(v -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] managed ledger delete done", ledgerName);
                    }
                    callback.operationComplete(null, null);
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName,
                            () -> callback.operationFailed(getException(ex)));
                    return null;
                });
    }

    @Override
    public Iterable<String> getManagedLedgers() throws MetaStoreException {
        try {
            return store.getChildren(BASE_NODE).join();
        } catch (CompletionException e) {
            throw getException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String path) {
        return store.exists(PREFIX + path);
    }

    @Override
    public void watchManagedLedgerInfo(String ledgerName, UpdateCallback<ManagedLedgerInfo> callback) {
        managedLedgerInfoUpdateCallbackMap.put(PREFIX + ledgerName, callback);
    }

    @Override
    public void unwatchManagedLedgerInfo(String ledgerName, UpdateCallback<ManagedLedgerInfo> callback) {
        managedLedgerInfoUpdateCallbackMap.remove(PREFIX + ledgerName, callback);
    }

    @Override
    public void accept(Notification notification) {
        if (!notification.getPath().startsWith(PREFIX) || notification.getType() != NotificationType.Modified) {
            return;
        }
        UpdateCallback<ManagedLedgerInfo> callback = managedLedgerInfoUpdateCallbackMap.get(notification.getPath());
        if (callback == null) {
            return;
        }
        String ledgerName = notification.getPath().substring(PREFIX.length());
        store.get(notification.getPath()).thenAcceptAsync(optResult -> {
            if (optResult.isPresent()) {
                ManagedLedgerInfo info;
                try {
                    info = parseManagedLedgerInfo(optResult.get().getValue());
                    info = updateMLInfoTimestamp(info);
                    callback.onUpdate(info, optResult.get().getStat());
                } catch (Exception e) {
                    log.error("[{}] Error when parseManagedLedgerInfo", ledgerName, e);
                }
            }
        }, executor.chooseThread(ledgerName)).exceptionally(ex -> {
            log.error("[{}] Error when read ManagedLedgerInfo", ledgerName, ex);
            return null;
        });
    }

    //
    // update timestamp if missing or 0
    // 3 cases - timestamp does not exist for ledgers serialized before
    // - timestamp is 0 for a ledger in recovery
    // - ledger has timestamp which is the normal case now

    private static ManagedLedgerInfo updateMLInfoTimestamp(ManagedLedgerInfo info) {
        List<ManagedLedgerInfo.LedgerInfo> infoList = new ArrayList<>(info.getLedgerInfosCount());
        long currentTime = System.currentTimeMillis();

        for (int i = 0; i < info.getLedgerInfosCount(); i++) {
            ManagedLedgerInfo.LedgerInfo ledgerInfo = info.getLedgerInfoAt(i);
            if (!ledgerInfo.hasTimestamp() || ledgerInfo.getTimestamp() == 0) {
                ManagedLedgerInfo.LedgerInfo updatedInfo = new ManagedLedgerInfo.LedgerInfo();
                updatedInfo.copyFrom(ledgerInfo);
                updatedInfo.setTimestamp(currentTime);
                infoList.add(updatedInfo);
            } else {
                infoList.add(ledgerInfo);
            }
        }
        ManagedLedgerInfo mlInfo = new ManagedLedgerInfo();
        mlInfo.addAllLedgerInfos(infoList);
        if (info.hasTerminatedPosition()) {
            mlInfo.setTerminatedPosition().copyFrom(info.getTerminatedPosition());
        }
        for (int i = 0; i < info.getPropertiesCount(); i++) {
            mlInfo.addProperty().copyFrom(info.getPropertyAt(i));
        }
        return mlInfo;
    }

    private static MetaStoreException getException(Throwable t) {
        Throwable actEx = FutureUtil.unwrapCompletionException(t);
        if (actEx instanceof MetadataStoreException.BadVersionException badVersionException) {
            return new ManagedLedgerException.BadVersionException(badVersionException);
        } else if (actEx instanceof MetaStoreException metaStoreException){
            return metaStoreException;
        } else {
            return new MetaStoreException(actEx);
        }
    }

    public byte[] compressLedgerInfo(ManagedLedgerInfo managedLedgerInfo) {
        CompressionType compressionType = ledgerInfoCompressionConfig.getCompressionType();
        if (compressionType.equals(CompressionType.NONE)) {
            return managedLedgerInfo.toByteArray();
        }

        int uncompressedSize = managedLedgerInfo.getSerializedSize();
        if (uncompressedSize > ledgerInfoCompressionConfig.getCompressSizeThresholdInBytes()) {
            ManagedLedgerInfoMetadata mlInfoMetadata = tlMlInfoMetadata.get();
            mlInfoMetadata.clear();
            mlInfoMetadata.setCompressionType(compressionType)
                    .setUncompressedSize(uncompressedSize);
            return compressManagedInfo(managedLedgerInfo.toByteArray(), mlInfoMetadata.toByteArray(),
                    mlInfoMetadata.getSerializedSize(), compressionType);
        }

        return managedLedgerInfo.toByteArray();
    }

    public byte[] compressCursorInfo(ManagedCursorInfo managedCursorInfo) {
        CompressionType compressionType = cursorInfoCompressionConfig.getCompressionType();
        if (compressionType.equals(CompressionType.NONE)) {
            return managedCursorInfo.toByteArray();
        }

        int uncompressedSize = managedCursorInfo.getSerializedSize();
        if (uncompressedSize > cursorInfoCompressionConfig.getCompressSizeThresholdInBytes()) {
            ManagedCursorInfoMetadata metadata = tlCursorInfoMetadata.get();
            metadata.clear();
            metadata.setCompressionType(compressionType)
                    .setUncompressedSize(uncompressedSize);
            return compressManagedInfo(managedCursorInfo.toByteArray(), metadata.toByteArray(),
                    metadata.getSerializedSize(), compressionType);
        }

        return managedCursorInfo.toByteArray();
    }

    public ManagedLedgerInfo parseManagedLedgerInfo(byte[] data) throws Exception {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        byte[] metadataBytes = extractCompressMetadataBytes(byteBuf);
        if (metadataBytes != null) {
            try {
                ManagedLedgerInfoMetadata metadata = tlMlInfoMetadata.get();
                metadata.clear();
                metadata.parseFrom(metadataBytes);
                ByteBuf uncompressed = getCompressionCodec(metadata.getCompressionType())
                        .decode(byteBuf, metadata.getUncompressedSize());
                try {
                    ManagedLedgerInfo info = new ManagedLedgerInfo();
                    info.parseFrom(uncompressed, uncompressed.readableBytes());
                    info.materialize();
                    return info;
                } finally {
                    uncompressed.release();
                }
            } catch (Exception e) {
                log.error("Failed to parse managedLedgerInfo metadata, "
                        + "fall back to parse managedLedgerInfo directly.", e);
                ManagedLedgerInfo info = new ManagedLedgerInfo();
                info.parseFrom(data);
                return info;
            } finally {
                byteBuf.release();
            }
        } else {
            ManagedLedgerInfo info = new ManagedLedgerInfo();
            info.parseFrom(data);
            return info;
        }
    }

    public ManagedCursorInfo parseManagedCursorInfo(byte[] data) throws Exception {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        byte[] metadataBytes = extractCompressMetadataBytes(byteBuf);
        if (metadataBytes != null) {
            try {
                ManagedCursorInfoMetadata metadata = tlCursorInfoMetadata.get();
                metadata.clear();
                metadata.parseFrom(metadataBytes);
                ByteBuf uncompressed = getCompressionCodec(metadata.getCompressionType())
                        .decode(byteBuf, metadata.getUncompressedSize());
                try {
                    ManagedCursorInfo info = new ManagedCursorInfo();
                    info.parseFrom(uncompressed, uncompressed.readableBytes());
                    // Detach lazy string fields, including the publication ID, before releasing the buffer.
                    info.materialize();
                    return info;
                } finally {
                    uncompressed.release();
                }
            } catch (Exception e) {
                log.error("Failed to parse ManagedCursorInfo metadata, "
                        + "fall back to parse ManagedCursorInfo directly", e);
                ManagedCursorInfo info = new ManagedCursorInfo();
                info.parseFrom(data);
                return info;
            } finally {
                byteBuf.release();
            }
        } else {
            ManagedCursorInfo info = new ManagedCursorInfo();
            info.parseFrom(data);
            return info;
        }
    }

    /**
     * Compress Managed Info data such as LedgerInfo, CursorInfo.
     *
     * compression data structure
     * [MAGIC_NUMBER](2) + [METADATA_SIZE](4) + [METADATA_PAYLOAD] + [MANAGED_LEDGER_INFO_PAYLOAD]
     */
    private byte[] compressManagedInfo(byte[] info, byte[] metadata, int metadataSerializedSize,
                                       CompressionType compressionType) {
        if (compressionType == null || compressionType.equals(CompressionType.NONE)) {
            return info;
        }

        CompositeByteBuf compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        try {
            ByteBuf metadataByteBuf = PulsarByteBufAllocator.DEFAULT.buffer(metadataSerializedSize + 6,
                    metadataSerializedSize + 6);
            metadataByteBuf.writeShort(MAGIC_MANAGED_INFO_METADATA);
            metadataByteBuf.writeInt(metadataSerializedSize);
            metadataByteBuf.writeBytes(metadata);
            ByteBuf encodeByteBuf = getCompressionCodec(compressionType)
                    .encode(Unpooled.wrappedBuffer(info));
            compositeByteBuf.addComponent(true, metadataByteBuf);
            compositeByteBuf.addComponent(true, encodeByteBuf);
            byte[] dataBytes = new byte[compositeByteBuf.readableBytes()];
            compositeByteBuf.readBytes(dataBytes);
            return dataBytes;
        } finally {
            compositeByteBuf.release();
        }
    }

    private byte[] extractCompressMetadataBytes(ByteBuf data) {
        if (data.readableBytes() > 0 && data.readShort() == MAGIC_MANAGED_INFO_METADATA) {
            int metadataSize = data.readInt();
            byte[] metadataBytes = new byte[metadataSize];
            data.readBytes(metadataBytes);
            return metadataBytes;
        }
        return null;
    }

    private CompressionCodec getCompressionCodec(CompressionType compressionType) {
        return CompressionCodecProvider.getCompressionCodec(
                org.apache.pulsar.common.api.proto.CompressionType.valueOf(compressionType.name()));
    }

}
