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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class MetaStoreImpl implements MetaStore, Consumer<Notification> {

    private static final String BASE_NODE = "/managed-ledgers";
    private static final String PREFIX = BASE_NODE + "/";

    private final MetadataStore store;
    private final OrderedExecutor executor;

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
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Updating cursor info ledgerId={} mark-delete={}:{} lastActive={}",
                    ledgerName, cursorName, info.getCursorsLedgerId(), info.getMarkDeleteLedgerId(),
                    info.getMarkDeleteEntryId(), info.getLastActive());
        }

        String path = PREFIX + ledgerName + "/" + cursorName;
        byte[] content = compressCursorInfo(info);

        long expectedVersion;

        if (stat != null) {
            expectedVersion = stat.getVersion();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Creating consumer {} on meta-data store with {}", ledgerName, cursorName, info);
            }
        } else {
            expectedVersion = -1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating consumer {} on meta-data store with {}", ledgerName, cursorName, info);
            }
        }
        store.put(path, content, Optional.of(expectedVersion))
                .thenAcceptAsync(optStat -> callback.operationComplete(null, optStat), executor
                        .chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName,
                            () -> callback.operationFailed(getException(ex)));
                    return null;
                });
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
    public void unwatchManagedLedgerInfo(String ledgerName) {
        managedLedgerInfoUpdateCallbackMap.remove(PREFIX + ledgerName);
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
