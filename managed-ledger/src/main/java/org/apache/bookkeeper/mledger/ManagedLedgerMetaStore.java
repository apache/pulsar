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

package org.apache.bookkeeper.mledger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.util.CallbackMutex;

public interface ManagedLedgerMetaStore {
    interface NewInfoBuilder {
        MLDataFormats.ManagedLedgerInfo build(Map<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> newLedgers);
    }

    interface LedgerInfoTransformation {
        MLDataFormats.ManagedLedgerInfo.LedgerInfo transform(MLDataFormats.ManagedLedgerInfo.LedgerInfo oldInfo) throws
                ManagedLedgerException;
    }


    class InitializeResult {
        final private PositionImpl terminatedPosition;
        final private Map<String, String> propertiesMap;

        public InitializeResult(PositionImpl terminatedPosition,
                                Map<String, String> propertiesMap) {
            this.terminatedPosition = terminatedPosition;
            this.propertiesMap = propertiesMap;
        }

        public Optional<PositionImpl> getTerminatedPosition() {
            return Optional.ofNullable(terminatedPosition);
        }

        public Map<String, String> getPropertiesMap() {
            return propertiesMap;
        }
    }

    CallbackMutex getMetadataMutex();

    MLDataFormats.ManagedLedgerInfo.LedgerInfo put(Long ledgerId, MLDataFormats.ManagedLedgerInfo.LedgerInfo info);

    Long lastLedgerId();

    boolean isEmpty();

    void remove(Long ledgerId);

    int size();

    void closeLedger(long ledgerId, long ledgerLength, long entriesCountInLedger, long closeTimeInMillis);

    Optional<MLDataFormats.ManagedLedgerInfo.LedgerInfo> get(Long ledgerId);

    CompletableFuture<InitializeResult> initialize(String name, boolean createIfMissing);

    void transformLedgerInfo(String name, long ledgerId,
                             LedgerInfoTransformation transformation,
                             CompletableFuture<Void> finalPromise, NewInfoBuilder builder,
                             ScheduledExecutorService scheduler);
}
