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
import java.util.concurrent.CompletionException;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class MetaStoreImpl implements MetaStore {

    private static final String BASE_NODE = "/managed-ledgers";
    private static final String PREFIX = BASE_NODE + "/";

    private final MetadataStore store;
    private final OrderedExecutor executor;

    public MetaStoreImpl(MetadataStore store, OrderedExecutor executor) {
        this.store = store;
        this.executor = executor;
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
                            info = ManagedLedgerInfo.parseFrom(optResult.get().getValue());
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
                    executor.executeOrdered(ledgerName, SafeRunnable.safeRun(() -> callback.operationFailed(getException(ex))));
                    return null;
                });
    }

    @Override
    public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
            MetaStoreCallback<Void> callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating metadata version={} with content={}", ledgerName, stat, mlInfo);
        }

        byte[] serializedMlInfo = mlInfo.toByteArray(); // Binary format
        String path = PREFIX + ledgerName;
        store.put(path, serializedMlInfo, Optional.of(stat.getVersion()))
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
        return mlInfo.build();
    }

    private static MetaStoreException getException(Throwable t) {
        if (t.getCause() instanceof MetadataStoreException.BadVersionException) {
            return new ManagedLedgerException.BadVersionException(t.getMessage());
        } else {
            return new MetaStoreException(t);
        }
    }
}
