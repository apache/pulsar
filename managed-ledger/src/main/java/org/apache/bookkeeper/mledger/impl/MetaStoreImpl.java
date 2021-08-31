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

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.RejectedExecutionException;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.CompressionType;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class MetaStoreImpl implements MetaStore {

    private static final String BASE_NODE = "/managed-ledgers";
    private static final String PREFIX = BASE_NODE + "/";

    private final MetadataStore store;
    private final OrderedExecutor executor;

    private static final int MAGIC_MANAGED_LEDGER_INFO_METADATA = 0x4778; // 0100 0111 0111 1000
    private final CompressionType compressionType;

    public MetaStoreImpl(MetadataStore store, OrderedExecutor executor) {
        this.store = store;
        this.executor = executor;
        this.compressionType = CompressionType.NONE;
    }

    public MetaStoreImpl(MetadataStore store, OrderedExecutor executor, String compressionType) {
        this.store = store;
        this.executor = executor;
        CompressionType finalCompressionType;
        if (compressionType != null) {
            try {
                finalCompressionType = CompressionType.valueOf(compressionType);
            } catch (Exception e) {
                log.error("Failed to get compression type {} error msg: {}.", compressionType, e.getMessage());
                throw e;
            }
        } else {
            finalCompressionType = CompressionType.NONE;
        }
        this.compressionType = finalCompressionType;
    }

    @Override
    public void getManagedLedgerInfo(String ledgerName, boolean createIfMissing,
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
                        } catch (InvalidProtocolBufferException e) {
                            callback.operationFailed(getException(e));
                        }
                    } else {
                        // Z-node doesn't exist
                        if (createIfMissing) {
                            log.info("Creating '{}'", path);

                            store.put(path, new byte[0], Optional.of(-1L))
                                    .thenAccept(stat -> {
                                        ManagedLedgerInfo info = ManagedLedgerInfo.getDefaultInstance();
                                        callback.operationComplete(info, stat);
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
                                SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
                    } catch (RejectedExecutionException e) {
                        //executor maybe shutdown, use common pool to run callback.
                        CompletableFuture.runAsync(() -> callback.operationFailed(getException(ex)));
                    }
                    return null;
                });
    }

    @Override
    public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
            MetaStoreCallback<Void> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating metadata version={} with content={}", ledgerName, stat, mlInfo);
        }

        String path = PREFIX + ledgerName;
        store.put(path, compressLedgerInfo(mlInfo), Optional.of(stat.getVersion()))
                .thenAcceptAsync(newVersion -> callback.operationComplete(null, newVersion), executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
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
                .thenAcceptAsync(cursors -> callback.operationComplete(cursors, null), executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
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
                            ManagedCursorInfo info = ManagedCursorInfo.parseFrom(optRes.get().getValue());
                            callback.operationComplete(info, optRes.get().getStat());
                        } catch (InvalidProtocolBufferException e) {
                            callback.operationFailed(getException(e));
                        }
                    } else {
                        callback.operationFailed(new MetadataNotFoundException("Cursor metadata not found"));
                    }
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
                    return null;
                });
    }

    @Override
    public void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat stat,
            MetaStoreCallback<Void> callback) {
        log.info("[{}] [{}] Updating cursor info ledgerId={} mark-delete={}:{}", ledgerName, cursorName,
                info.getCursorsLedgerId(), info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());

        String path = PREFIX + ledgerName + "/" + cursorName;
        byte[] content = info.toByteArray(); // Binary format

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
                .thenAcceptAsync(optStat -> callback.operationComplete(null, optStat), executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
                    return null;
                });
    }

    @Override
    public void asyncRemoveCursor(String ledgerName, String cursorName, MetaStoreCallback<Void> callback) {
        String path = PREFIX + ledgerName + "/" + cursorName;
        log.info("[{}] Remove consumer={}", ledgerName, cursorName);

        store.delete(path, Optional.empty())
                .thenAcceptAsync(v -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] cursor delete done", ledgerName, cursorName);
                    }
                    callback.operationComplete(null, null);
                }, executor.chooseThread(ledgerName))
                .exceptionally(ex -> {
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
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
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
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

    //
    // update timestamp if missing or 0
    // 3 cases - timestamp does not exist for ledgers serialized before
    // - timestamp is 0 for a ledger in recovery
    // - ledger has timestamp which is the normal case now

    private static ManagedLedgerInfo updateMLInfoTimestamp(ManagedLedgerInfo info) {
        List<ManagedLedgerInfo.LedgerInfo> infoList = new ArrayList<>(info.getLedgerInfoCount());
        long currentTime = System.currentTimeMillis();

        for (ManagedLedgerInfo.LedgerInfo ledgerInfo : info.getLedgerInfoList()) {
            if (!ledgerInfo.hasTimestamp() || ledgerInfo.getTimestamp() == 0) {
                ManagedLedgerInfo.LedgerInfo.Builder singleInfoBuilder = ledgerInfo.toBuilder();
                singleInfoBuilder.setTimestamp(currentTime);
                infoList.add(singleInfoBuilder.build());
            } else {
                infoList.add(ledgerInfo);
            }
        }
        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder();
        mlInfo.addAllLedgerInfo(infoList);
        if (info.hasTerminatedPosition()) {
            mlInfo.setTerminatedPosition(info.getTerminatedPosition());
        }
        mlInfo.addAllProperties(info.getPropertiesList());
        return mlInfo.build();
    }

    private static MetaStoreException getException(Throwable t) {
        if (t.getCause() instanceof MetadataStoreException.BadVersionException) {
            return new ManagedLedgerException.BadVersionException(t.getMessage());
        } else {
            return new MetaStoreException(t);
        }
    }

    /**
     * Compress ManagedLedgerInfo data.
     *
     * compression data structure
     * [MAGIC_NUMBER](2) + [METADATA_SIZE](4) + [METADATA_PAYLOAD] + [MANAGED_LEDGER_INFO_PAYLOAD]
      */
    public byte[] compressLedgerInfo(ManagedLedgerInfo managedLedgerInfo) {
        if (compressionType == null || compressionType.equals(CompressionType.NONE)) {
            return managedLedgerInfo.toByteArray();
        }
        ByteBuf metadataByteBuf = null;
        ByteBuf encodeByteBuf = null;
        try {
            MLDataFormats.ManagedLedgerInfoMetadata mlInfoMetadata = MLDataFormats.ManagedLedgerInfoMetadata
                    .newBuilder()
                    .setCompressionType(compressionType)
                    .setUncompressedSize(managedLedgerInfo.getSerializedSize())
                    .build();
            metadataByteBuf = PulsarByteBufAllocator.DEFAULT.buffer(
                    mlInfoMetadata.getSerializedSize() + 6, mlInfoMetadata.getSerializedSize() + 6);
            metadataByteBuf.writeShort(MAGIC_MANAGED_LEDGER_INFO_METADATA);
            metadataByteBuf.writeInt(mlInfoMetadata.getSerializedSize());
            metadataByteBuf.writeBytes(mlInfoMetadata.toByteArray());

            encodeByteBuf = getCompressionCodec(compressionType)
                    .encode(Unpooled.wrappedBuffer(managedLedgerInfo.toByteArray()));
            CompositeByteBuf compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
            compositeByteBuf.addComponent(true, metadataByteBuf);
            compositeByteBuf.addComponent(true, encodeByteBuf);
            byte[] dataBytes = new byte[compositeByteBuf.readableBytes()];
            compositeByteBuf.readBytes(dataBytes);
            return dataBytes;
        } finally {
            if (metadataByteBuf != null) {
                metadataByteBuf.release();
            }
            if (encodeByteBuf != null) {
                encodeByteBuf.release();
            }
        }
    }

    public ManagedLedgerInfo parseManagedLedgerInfo(byte[] data) throws InvalidProtocolBufferException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
        if (byteBuf.readableBytes() > 0 && byteBuf.readShort() == MAGIC_MANAGED_LEDGER_INFO_METADATA) {
            ByteBuf decodeByteBuf = null;
            try {
                int metadataSize = byteBuf.readInt();
                byte[] metadataBytes = new byte[metadataSize];
                byteBuf.readBytes(metadataBytes);
                MLDataFormats.ManagedLedgerInfoMetadata metadata =
                        MLDataFormats.ManagedLedgerInfoMetadata.parseFrom(metadataBytes);

                long unpressedSize = metadata.getUncompressedSize();
                decodeByteBuf = getCompressionCodec(metadata.getCompressionType())
                        .decode(byteBuf, (int) unpressedSize);
                byte[] decodeBytes;
                // couldn't decode data by ZLIB compression byteBuf array() directly
                if (decodeByteBuf.hasArray() && !CompressionType.ZLIB.equals(metadata.getCompressionType())) {
                    decodeBytes = decodeByteBuf.array();
                } else {
                    decodeBytes = new byte[decodeByteBuf.readableBytes() - decodeByteBuf.readerIndex()];
                    decodeByteBuf.readBytes(decodeBytes);
                }
                return ManagedLedgerInfo.parseFrom(decodeBytes);
            } catch (Exception e) {
                log.error("Failed to parse managedLedgerInfo metadata, "
                        + "fall back to parse managedLedgerInfo directly.", e);
                return ManagedLedgerInfo.parseFrom(data);
            } finally {
                if (decodeByteBuf != null) {
                    decodeByteBuf.release();
                }
                byteBuf.release();
            }
        } else {
            return ManagedLedgerInfo.parseFrom(data);
        }
    }

    private CompressionCodec getCompressionCodec(CompressionType compressionType) {
        return CompressionCodecProvider.getCompressionCodec(
                org.apache.pulsar.common.api.proto.CompressionType.valueOf(compressionType.name()));
    }

}
