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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store max sequenceID in ManagedLedger properties, in order to recover transaction log.
 */
public class MLTransactionSequenceIdGenerator implements ManagedLedgerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(MLTransactionSequenceIdGenerator.class);
    private static final long TC_ID_NOT_USED = -1L;
    public static final String MAX_LOCAL_TXN_ID = "max_local_txn_id";
    private final AtomicLong sequenceId = new AtomicLong(TC_ID_NOT_USED);

    @Override
    public void beforeAddEntry(AddEntryOperation op, int numberOfMessages) {
        // do nothing
    }

    // When all of ledger have been deleted, we will generate sequenceId from managedLedger properties
    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(MAX_LOCAL_TXN_ID)) {
            sequenceId.set(Long.parseLong(propertiesMap.get(MAX_LOCAL_TXN_ID)));
        }
    }

    // When we don't roll over ledger, we can init sequenceId from the getLastAddConfirmed transaction metadata entry
    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LastEntryHandle lh) {
        return lh.readLastEntryAsync().thenAccept(lastEntryOptional -> {
            if (lastEntryOptional.isPresent()) {
                Entry lastEntry = lastEntryOptional.get();
                try {
                    List<TransactionMetadataEntry> transactionLogs =
                            MLTransactionLogImpl.deserializeEntry(lastEntry.getDataBuffer());
                    if (!CollectionUtils.isEmpty(transactionLogs)) {
                        TransactionMetadataEntry lastConfirmEntry =
                                transactionLogs.get(transactionLogs.size() - 1);
                        this.sequenceId.set(lastConfirmEntry.getMaxLocalTxnId());
                    }
                } finally {
                    lastEntry.release();
                }
            }
        });
    }

    // roll over ledger will update sequenceId to managedLedger properties
    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        propertiesMap.put(MAX_LOCAL_TXN_ID, sequenceId.get() + "");
    }

    long generateSequenceId() {
        return sequenceId.incrementAndGet();
    }

    long getCurrentSequenceId() {
        return sequenceId.get();
    }
}
