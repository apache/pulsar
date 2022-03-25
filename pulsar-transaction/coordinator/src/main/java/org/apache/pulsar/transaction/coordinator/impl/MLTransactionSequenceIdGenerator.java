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
package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
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
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
        return op;
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
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        if (lh.getLastAddConfirmed() >= 0) {
            lh.readAsync(lh.getLastAddConfirmed(), lh.getLastAddConfirmed()).whenComplete((entries, ex) -> {
                if (ex != null) {
                    log.error("[{}] Read last entry error.", name, ex);
                    promise.completeExceptionally(ex);
                } else {
                    if (entries != null) {
                        try {
                            LedgerEntry ledgerEntry = entries.getEntry(lh.getLastAddConfirmed());
                            if (ledgerEntry != null) {
                                TransactionMetadataEntry lastConfirmEntry = new TransactionMetadataEntry();
                                ByteBuf buffer = ledgerEntry.getEntryBuffer();
                                lastConfirmEntry.parseFrom(buffer, buffer.readableBytes());
                                this.sequenceId.set(lastConfirmEntry.getMaxLocalTxnId());
                            }
                            entries.close();
                            promise.complete(null);
                        } catch (Exception e) {
                            entries.close();
                            log.error("[{}] Failed to recover the tc sequenceId from the last add confirmed entry.",
                                    name, e);
                            promise.completeExceptionally(e);
                        }
                    } else {
                        promise.complete(null);
                    }
                }
            });
        } else {
            promise.complete(null);
        }
        return promise;
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
