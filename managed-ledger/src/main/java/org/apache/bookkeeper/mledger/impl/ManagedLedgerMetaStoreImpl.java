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

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.metadata.api.Stat;

public class ManagedLedgerMetaStoreImpl implements ManagedLedgerMetaStore {
    final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    final MetaStore store;

    public ManagedLedgerMetaStoreImpl(MetaStore metaStore) {
        this.store = metaStore;
    }

    static public class InitializeResult {
        private Stat stat;
        private Optional<PositionImpl> terminatedPosition;
        private Map<String, String> propertiesMap;

        public InitializeResult(Stat stat,
                                Optional<PositionImpl> terminatedPosition,
                                Map<String, String> propertiesMap) {
            this.stat = stat;
            this.terminatedPosition = terminatedPosition;
            this.propertiesMap = propertiesMap;
        }

        public Stat getStat() {
            return stat;
        }

        public Optional<PositionImpl> getTerminatedPosition() {
            return terminatedPosition;
        }

        public Map<String, String> getPropertiesMap() {
            return propertiesMap;
        }
    }

    LedgerInfo put(Long ledgerId, LedgerInfo info) {
        return ledgers.put(ledgerId, info);
    }

    synchronized CompletableFuture<InitializeResult> initialize(String name, boolean createIfMissing) {
        final CompletableFuture<InitializeResult> initResult = new CompletableFuture<>();
        store.getManagedLedgerInfo(name, createIfMissing,
                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo result, Stat stat) {
                        final Optional<PositionImpl> terminatedPosition;
                        if (result.hasTerminatedPosition()) {
                            terminatedPosition = Optional.of(new PositionImpl(result.getTerminatedPosition()));
                        } else {
                            terminatedPosition = Optional.empty();
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

                        initResult.complete(new InitializeResult(stat, terminatedPosition, propertiesMap));
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        initResult.completeExceptionally(e);
                    }
                });
        return initResult;
    }
}
