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

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLog;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTrackerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class MLTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {

    private static final Logger log = LoggerFactory.getLogger(MLTransactionMetadataStoreProvider.class);

    @Override
    public CompletableFuture<TransactionMetadataStore> openStore(TransactionCoordinatorID transactionCoordinatorId,
                                                                 ManagedLedgerFactory managedLedgerFactory,
                                                                 ManagedLedgerConfig managedLedgerConfig,
                                                                 TransactionTimeoutTracker timeoutTracker,
                                                                 TransactionRecoverTracker recoverTracker) {
        MLTransactionLogImpl txnLog = new MLTransactionLogImpl(transactionCoordinatorId,
                managedLedgerFactory, managedLedgerConfig);

        return txnLog.initialize().thenApply(__ ->
                new MLTransactionMetadataStore(transactionCoordinatorId, txnLog, timeoutTracker, recoverTracker));
    }
}