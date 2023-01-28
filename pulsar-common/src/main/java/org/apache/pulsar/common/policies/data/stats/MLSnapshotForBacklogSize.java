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
package org.apache.pulsar.common.policies.data.stats;

import io.netty.util.Recycler;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.Getter;

/**
 * A value object that represents the snapshot of a managed ledger and is used to calculate the "backlogSize" of a
 * topic's subscriptions.
 */
public class MLSnapshotForBacklogSize {

    private static final FastThreadLocal<MLSnapshotForBacklogSize> ML_SNAPSHOT_THREAD_LOCAL =
            new FastThreadLocal<MLSnapshotForBacklogSize>(){
                protected MLSnapshotForBacklogSize initialValue() throws Exception {
                    return new MLSnapshotForBacklogSize();
                }
            };

    /** Corresponds to the 'ManagedLedgerImpl.ledgers' field. **/
    @Getter
    private NavigableMap<Long, SimpleLedgerInfo> ledgers = new TreeMap<>();

    /** Corresponds to the 'ManagedLedgerImpl.currentLedger' field. **/
    @Getter
    private long currentLedger;

    /** Corresponds to the 'ManagedLedgerImpl.currentLedgerSize' field. **/
    @Getter
    private long currentLedgerSize;

    /** Corresponds to the 'ManagedLedgerImpl.currentLedgerEntries' field. **/
    @Getter
    private long currentLedgerEntries;

    /** Corresponds to the 'ManagedLedgerImpl.totalSize' field. **/
    @Getter
    private long totalSize;

    private MLSnapshotForBacklogSize(){}

    public static MLSnapshotForBacklogSize newInstance(long currentLedger, long currentLedgerSize,
                                                       long currentLedgerEntries, long totalSize){
        MLSnapshotForBacklogSize v = ML_SNAPSHOT_THREAD_LOCAL.get();
        v.currentLedger = currentLedger;
        v.currentLedgerSize = currentLedgerSize;
        v.currentLedgerEntries = currentLedgerEntries;
        v.totalSize = totalSize;
        return v;
    }

    public void recycle(){
        currentLedger = 0;
        currentLedgerSize = 0;
        currentLedgerEntries = 0;
        totalSize = 0;
        ledgers.values().forEach(simpleLedgerInfo -> simpleLedgerInfo.recycle());
        // Avoid can not GC the overstretched collection.
        if (ledgers.size() > 8192) {
            ledgers = new TreeMap<>();
        } else {
            ledgers.clear();
        }
    }

    public static final class SimpleLedgerInfo {

        private final Recycler.Handle<SimpleLedgerInfo> handle;
        @Getter
        private long size;
        @Getter
        private long entries;

        private static final Recycler<SimpleLedgerInfo> SIMPLE_LEDGER_INFO_RECYCLER =
                new Recycler<SimpleLedgerInfo>(512) {
                    @Override
                    protected SimpleLedgerInfo newObject(Handle<SimpleLedgerInfo> handle) {
                        return new SimpleLedgerInfo(handle);
                    }
                };

        private SimpleLedgerInfo(Recycler.Handle<SimpleLedgerInfo> handle){
            this.handle = handle;
        }

        public static SimpleLedgerInfo newInstance(long size, long entries){
            SimpleLedgerInfo info = SIMPLE_LEDGER_INFO_RECYCLER.get();
            info.size = size;
            info.entries = entries;
            return info;
        }

        public void recycle(){
            size = 0;
            entries = 0;
            handle.recycle(this);
        }
    }
}
