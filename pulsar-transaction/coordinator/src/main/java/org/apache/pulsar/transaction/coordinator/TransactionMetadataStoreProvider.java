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
package org.apache.pulsar.transaction.coordinator;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.Beta;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;

/**
 * A provider that provides {@link TransactionMetadataStore}.
 */
@Beta
public interface TransactionMetadataStoreProvider {

    /**
     * Construct a provider from the provided class.
     *
     * @param providerClassName the provider class name.
     * @return an instance of transaction metadata store provider.
     */
    static TransactionMetadataStoreProvider newProvider(String providerClassName) throws IOException {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(providerClassName);
            Object obj = providerClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof TransactionMetadataStoreProvider,
                "The factory has to be an instance of "
                    + TransactionMetadataStoreProvider.class.getName());

            return (TransactionMetadataStoreProvider) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Open the transaction metadata store for transaction coordinator
     * identified by <tt>transactionCoordinatorId</tt>.
     *
     * @param transactionCoordinatorId {@link TransactionCoordinatorID} the coordinator id.
     * @param managedLedgerFactory {@link ManagedLedgerFactory} the managedLedgerFactory to create managedLedger.
     * @param managedLedgerConfig {@link ManagedLedgerConfig} the managedLedgerConfig to create managedLedger.
     * @param timeoutTracker {@link TransactionTimeoutTracker} the timeoutTracker to handle transaction time out.
     * @param recoverTracker {@link TransactionRecoverTracker} the recoverTracker to handle transaction recover.
     * @return a future represents the result of the operation.
     *         an instance of {@link TransactionMetadataStore} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<TransactionMetadataStore> openStore(
            TransactionCoordinatorID transactionCoordinatorId, ManagedLedgerFactory managedLedgerFactory,
            ManagedLedgerConfig managedLedgerConfig, TransactionTimeoutTracker timeoutTracker,
            TransactionRecoverTracker recoverTracker);
}
