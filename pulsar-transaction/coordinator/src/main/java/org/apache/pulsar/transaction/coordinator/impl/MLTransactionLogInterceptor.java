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

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Store max sequenceID in ManagedLedger properties, in order to recover transaction log.
 */
public class MLTransactionLogInterceptor implements ManagedLedgerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(MLTransactionLogInterceptor.class);
    public static final String MAX_LOCAL_TXN_ID = "max_local_txn_id";

    private volatile long maxLocalTxnId = -1;

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
        return null;
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {

    }

    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle ledgerHandle) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        propertiesMap.put(MAX_LOCAL_TXN_ID, maxLocalTxnId + "");
    }

    protected void setMaxLocalTxnId(long maxLocalTxnId) {
        this.maxLocalTxnId = maxLocalTxnId;
    }
}
