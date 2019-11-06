package org.apache.pulsar.transaction.coordinator.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class ManagedLedgerTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionMetadataStoreProvider.class);
    @Override
    public CompletableFuture<TransactionMetadataStore>
    openStore(TransactionCoordinatorID transactionCoordinatorId, ManagedLedgerFactory managedLedgerFactory) {
        TransactionMetadataStore transactionMetadataStore;
        try {
            transactionMetadataStore =
                    new ManagedLedgerTransactionMetadataStore(transactionCoordinatorId, managedLedgerFactory);
        } catch (Exception e) {
            log.error("ManagedLedgerTransactionMetadataStore init fail", e);
            return FutureUtil.failedFuture(e);
        }
        return CompletableFuture.completedFuture(transactionMetadataStore);
    }
}
