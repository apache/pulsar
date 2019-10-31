package org.apache.pulsar.transaction.coordinator.impl;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.impl.TopicBaseTransactionMetadataStore.TopicBaseTransactionWriter;


import java.util.concurrent.CompletableFuture;

class TopicBaseTransactionWriterImpl implements TopicBaseTransactionWriter {

    private ManagedLedger managedLedger;

    public TopicBaseTransactionWriterImpl(String tcID, ManagedLedgerFactory factory) throws Exception {
        this.managedLedger = factory.open(tcID);
    }

    @Override
    public CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry) {
        try {
            managedLedger.addEntry(transactionMetadataEntry.toByteArray());
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }
}
