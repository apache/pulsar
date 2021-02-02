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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class ManagedLedgerMetaStoreImpl implements ManagedLedgerMetaStore {


    protected volatile Stat ledgersStat;
    private final CallbackMutex metadataMutex = new CallbackMutex();
    final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    final MetaStore store;

    public CallbackMutex getMetadataMutex() {
        return metadataMutex;
    }


    public ManagedLedgerMetaStoreImpl(MetaStore metaStore) {
        this.store = metaStore;
    }

    public LedgerInfo put(Long ledgerId, LedgerInfo info) {
        return ledgers.put(ledgerId, info);
    }

    public Long lastLedgerId() {
        return ledgers.lastKey();
    }

    public boolean isEmpty() {
        return ledgers.isEmpty();
    }

    public void remove(Long ledgerId) {
        ledgers.remove(ledgerId);
    }

    public int size() {
        return ledgers.size();
    }

    public void closeLedger(long ledgerId, long ledgerLength, long entriesCountInLedger, long closeTimeInMillis) {
        LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(ledgerId).setEntries(entriesCountInLedger)
                .setSize(ledgerLength).setTimestamp(closeTimeInMillis).build();
        put(ledgerId, info);
    }

    public Optional<LedgerInfo> get(Long ledgerId) {
        final LedgerInfo ledgerInfo = ledgers.get(ledgerId);
        if (ledgerInfo == null) {
            return Optional.empty();
        } else {
            return Optional.of(ledgerInfo);
        }
    }

    public synchronized CompletableFuture<InitializeResult> initialize(String name, boolean createIfMissing) {
        final CompletableFuture<InitializeResult> initResult = new CompletableFuture<>();
        store.getManagedLedgerInfo(name, createIfMissing,
                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo result, Stat stat) {
                        ledgersStat = stat;
                        final PositionImpl terminatedPosition;
                        if (result.hasTerminatedPosition()) {
                            terminatedPosition = new PositionImpl(result.getTerminatedPosition());
                        } else {
                            terminatedPosition = null;
                        }

                        final Map<String, String> propertiesMap = new HashMap<>();

                        if (result.getPropertiesCount() > 0) {
                            for (int i = 0; i < result.getPropertiesCount(); i++) {
                                MLDataFormats.KeyValue property = result.getProperties(i);
                                propertiesMap.put(property.getKey(), property.getValue());
                            }
                        }


                        for (LedgerInfo ls : result.getLedgerInfoList()) {
                            ledgers.put(ls.getLedgerId(), ls);
                        }

                        initResult.complete(new InitializeResult(terminatedPosition, propertiesMap));
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        initResult.completeExceptionally(e);
                    }
                });
        return initResult;
    }

    public void transformLedgerInfo(String name, long ledgerId,
                                    LedgerInfoTransformation transformation,
                                    CompletableFuture<Void> finalPromise, NewInfoBuilder builder,
                                    ScheduledExecutorService scheduler) {
        synchronized (this) {
            if (!getMetadataMutex().tryLock()) {
                // retry in 100 milliseconds
                scheduler.schedule(
                        safeRun(() -> transformLedgerInfo(name, ledgerId, transformation, finalPromise, builder,
                                scheduler)), 100,
                        TimeUnit.MILLISECONDS);
            } else { // lock acquired
                CompletableFuture<Void> unlockingPromise = new CompletableFuture<>();
                unlockingPromise.whenComplete((res, ex) -> {
                    getMetadataMutex().unlock();
                    if (ex != null) {
                        finalPromise.completeExceptionally(ex);
                    } else {
                        finalPromise.complete(res);
                    }
                });

                LedgerInfo oldInfo = get(ledgerId).orElse(null);
                if (oldInfo == null) {
                    unlockingPromise.completeExceptionally(new ManagedLedgerImpl.OffloadConflict(
                            "Ledger " + ledgerId + " no longer exists in ManagedLedger, likely trimmed"));
                } else {
                    try {
                        LedgerInfo newInfo = transformation.transform(oldInfo);
                        final HashMap<Long, LedgerInfo> newLedgers = new HashMap<>(ledgers);
                        newLedgers.put(ledgerId, newInfo);
                        store.asyncUpdateLedgerIds(name, builder.build(newLedgers), ledgersStat,
                                new MetaStore.MetaStoreCallback<Void>() {
                                    @Override
                                    public void operationComplete(Void result, Stat stat) {
                                        ledgersStat = stat;
                                        put(ledgerId, newInfo);
                                        unlockingPromise.complete(null);
                                    }

                                    @Override
                                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                        unlockingPromise.completeExceptionally(e);
                                    }
                                });
                    } catch (ManagedLedgerException mle) {
                        unlockingPromise.completeExceptionally(mle);
                    }
                }
            }
        }
    }
}
