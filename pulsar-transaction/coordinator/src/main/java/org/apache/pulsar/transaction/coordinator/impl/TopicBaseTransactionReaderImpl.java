package org.apache.pulsar.transaction.coordinator.impl;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class TopicBaseTransactionReaderImpl implements
        TopicBaseTransactionMetadataStore.TopicBaseTransactionReader {

    private ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();

    private long sequenceId;

    private final ManagedLedgerFactory managedLedgerFactory;

    private final ReadOnlyCursor readOnlyCursor;

    public TopicBaseTransactionReaderImpl(String tcId, ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.managedLedgerFactory =  managedLedgerFactory;
        this.readOnlyCursor = managedLedgerFactory
                .openReadOnlyCursor(tcId,
                        PositionImpl.earliest,
                        ((ManagedLedgerFactoryImpl)managedLedgerFactory)
                                .getManagedLedgers()
                                .get(tcId)
                                .getConfig());
        while (readOnlyCursor.hasMoreEntries()) {
            List<Entry> entries = readOnlyCursor.readEntries(100);
            for (int i = 0; i < entries.size(); i++) {
                MessageImpl<byte[]> message = MessageImpl.deserialize(entries.get(i).getDataBuffer());
                TransactionMetadataEntry transactionMetadataEntry =
                        TransactionMetadataEntry.parseFrom(message.getData());
                TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                        transactionMetadataEntry.getTxnidLeastBits());
                switch (transactionMetadataEntry.getMetadataOp()) {
                    case NEW:
                        sequenceId = sequenceId > transactionMetadataEntry.getTxnidLeastBits() ?
                                sequenceId : transactionMetadataEntry.getTxnidMostBits();
                        txnMetaMap.put(txnID, new TxnMetaImpl(txnID));
                        break;
                    case ADD_PARTITION:
                        txnMetaMap.get(txnID).addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                        break;
                    case ADD_SUBSCRIPTION:
                        txnMetaMap.get(txnID)
                                .addTxnSubscription(
                                        subscriptionToTxnSubscription(transactionMetadataEntry.getSubscriptionsList()));
                        break;
                    case UPDATE:
                        txnMetaMap.get(txnID)
                                .updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                        transactionMetadataEntry.getExpectedStatus());
                        break;
                        default:
                            throw new InvalidTxnStatusException("Transaction `" +
                                    txnID + "` load bad metadata operation from transaction log ");

                }
                transactionMetadataEntry.recycle();
                entries.get(i).release();
            }
        }
    }


    @Override
    public TxnMeta getTxnMeta(TxnID txnid) {
        return txnMetaMap.get(txnid);
    }

    @Override
    public Long readSequenceId() {
        return sequenceId;
    }

    @Override
    public void addNewTxn(TxnMeta txnMeta) {
        txnMetaMap.put(txnMeta.id(), txnMeta);
    }

    @Override
    public TxnStatus getTxnStatus(TxnID txnID) {
        return txnMetaMap.get(txnID).status();
    }

    private static List<TxnSubscription> subscriptionToTxnSubscription(List<Subscription> subscriptions) {
        List<TxnSubscription> txnSubscriptions = new ArrayList<>(subscriptions.size());
        for (int i = 0; i < subscriptions.size(); i++) {
            txnSubscriptions.add(new TxnSubscription(subscriptions.get(i).getTopic(), subscriptions.get(i).getSubscription()));
        }
        return txnSubscriptions;
    }

}
