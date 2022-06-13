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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.ManagedTrashImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

public interface ManagedTrash {

    enum ManagedType {
        MANAGED_LEDGER("managed-ledger"),
        MANAGED_CURSOR("managed-cursor"),
        SCHEMA("schema");
        private final String name;

        ManagedType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    enum LedgerType {
        BOTH,
        OFFLOAD_LEDGER,
        LEDGER
    }

    /**
     * ManagedTrash name.
     *
     * @return full topic name + type
     */
    String name();

    /**
     * Initialize.
     */
    CompletableFuture<?> initialize();

    /**
     * Append waiting to delete ledger.
     *
     * @param ledgerId ledgerId
     * @param context ledgerInfo, if offload ledger, need offload context
     * @param type LEDGER or OFFLOAD_LEDGER
     * @throws ManagedLedgerException
     */
    void appendLedgerTrashData(long ledgerId, LedgerInfo context, LedgerType type) throws ManagedLedgerException;

    /**
     * Persist trash data to meta store.
     */
    CompletableFuture<?> asyncUpdateTrashData();

    /**
     * Trigger deletion procedure.
     */
    void triggerDeleteInBackground();

    /**
     * Get all archive index, it needs combine with getArchiveData.
     */
    CompletableFuture<List<Long>> getAllArchiveIndex();

    /**
     * Get archive data detail info.
     *
     * @param index archive index
     * @return
     */
    CompletableFuture<Map<ManagedTrashImpl.TrashKey, LedgerInfo>> getArchiveData(long index);

    /**
     * Async close managedTrash, it will persist trash data to meta store.
     * @return
     */
    CompletableFuture<?> asyncClose();

    /**
     * Async close managedTrash, it can ensure that all ledger least delete once (exclude offload_ledger).
     * @return
     */
    CompletableFuture<?> asyncCloseAfterAllLedgerDeleteOnce();
}
